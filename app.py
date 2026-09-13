import os
import re
import io
import time
import queue
import asyncio
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from flask import Flask, request
from telegram import Bot, Update
from telegram.request import HTTPXRequest
from web3 import Web3


# ============================================================
# تنظیمات
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

SOURCE_CHANNEL = int(os.getenv("SOURCE_CHANNEL", "0"))
REPORT_CHANNEL = int(os.getenv("REPORT_CHANNEL", "0"))

HOURLY_INTERVAL_MIN = int(os.getenv("HOURLY_INTERVAL_MIN", "60"))
EXPORT_INTERVAL_MIN = int(os.getenv("EXPORT_INTERVAL_MIN", "120"))

BATCH_SIZE = 300

NETWORKS = {
    "ETH": "https://eth.llamarpc.com",
    "BSC": "https://bsc-dataseed.binance.org/",
}


# ============================================================
# Flask / Queue
# ============================================================

app = Flask(__name__)
task_queue = queue.Queue()


# ============================================================
# PostgreSQL
# ============================================================

def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )


def init_db():
    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                id BIGSERIAL PRIMARY KEY,
                address TEXT NOT NULL UNIQUE,
                source_uploaded_at TIMESTAMPTZ,
                added_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()

        logging.info("PostgreSQL initialized")

    finally:
        conn.close()


def save_wallet(address, source_uploaded_at):
    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO wallets
                (address, source_uploaded_at)
            VALUES
                (%s, %s)
            ON CONFLICT (address) DO NOTHING
        """, (
            address.lower(),
            source_uploaded_at
        ))

        conn.commit()

    finally:
        conn.close()


def get_all_wallets():
    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                id,
                address,
                source_uploaded_at
            FROM wallets
            ORDER BY id ASC
        """)

        return cur.fetchall()

    finally:
        conn.close()


def get_meta(key, default=None):
    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT value FROM meta WHERE key = %s",
            (key,)
        )

        row = cur.fetchone()

        return row[0] if row else default

    finally:
        conn.close()


def set_meta(key, value):
    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO meta (key, value)
            VALUES (%s, %s)
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value
        """, (
            key,
            value
        ))

        conn.commit()

    finally:
        conn.close()


# ============================================================
# Telegram
# ============================================================

def create_bot(pool_size=20):
    return Bot(
        token=BOT_TOKEN,
        request=HTTPXRequest(
            connection_pool_size=pool_size,
            pool_timeout=60
        )
    )


# ============================================================
# فایل ورودی
# ============================================================

ADDRESS_PATTERN = re.compile(
    r"Addr:\s*(0x[a-fA-F0-9]{40})",
    re.IGNORECASE
)


async def handle_file(file_id, file_name, uploaded_at):
    bot = create_bot()

    try:
        logging.info(
            "Downloading source file: %s",
            file_name
        )

        telegram_file = await bot.get_file(file_id)

        content = await telegram_file.download_as_bytearray()

        text = content.decode(
            "utf-8",
            errors="ignore"
        )

        addresses = []

        # ترتیب فایل حفظ می‌شود
        for line in text.splitlines():

            match = ADDRESS_PATTERN.search(line)

            if match:
                address = match.group(1)

                addresses.append(address)

        if not addresses:
            logging.warning(
                "No addresses found in %s",
                file_name
            )
            return

        saved = 0

        for address in addresses:

            before = len(get_all_wallets())

            save_wallet(
                address,
                uploaded_at
            )

            after = len(get_all_wallets())

            if after > before:
                saved += 1

        total = len(get_all_wallets())

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "📥 <b>فایل جدید دریافت شد</b>\n\n"
                f"📄 فایل: <code>{file_name}</code>\n"
                f"🕐 زمان انتشار: <code>{uploaded_at}</code>\n"
                f"🔢 آدرس‌های داخل فایل: <code>{len(addresses)}</code>\n"
                f"➕ آدرس جدید: <code>{saved}</code>\n"
                f"📦 کل دیتابیس: <code>{total}</code>"
            ),
            parse_mode="HTML"
        )

        logging.info(
            "File processed: %s | addresses=%s | new=%s",
            file_name,
            len(addresses),
            saved
        )

    except Exception:
        logging.exception(
            "File processing failed"
        )


# ============================================================
# موجودی
# ============================================================

def get_wallet_balance(address):
    result = {
        "ETH": 0.0,
        "BSC": 0.0
    }

    for network, rpc in NETWORKS.items():

        try:
            w3 = Web3(
                Web3.HTTPProvider(
                    rpc,
                    request_kwargs={
                        "timeout": 10
                    }
                )
            )

            if not w3.is_connected():
                logging.warning(
                    "%s RPC not connected",
                    network
                )
                continue

            checksum = Web3.to_checksum_address(
                address
            )

            balance = w3.eth.get_balance(
                checksum
            )

            result[network] = float(
                w3.from_wei(
                    balance,
                    "ether"
                )
            )

        except Exception as e:

            logging.warning(
                "Balance error | network=%s | address=%s | error=%s",
                network,
                address,
                e
            )

    return result


# ============================================================
# گزارش ساعتی
# ============================================================

async def handle_hourly():
    bot = create_bot(30)

    wallets = get_all_wallets()

    total_wallets = len(wallets)

    logging.info(
        "Hourly scan started | total=%s",
        total_wallets
    )

    if not wallets:

        set_meta(
            "last_hourly",
            datetime.now(timezone.utc).isoformat()
        )

        return

    total_parts = (
        total_wallets + BATCH_SIZE - 1
    ) // BATCH_SIZE

    total_eth = 0.0
    total_bsc = 0.0
    rich_count = 0

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=(
            "⏰ <b>شروع گزارش ساعتی</b>\n\n"
            f"📦 تعداد کل: <code>{total_wallets}</code>\n"
            f"🔢 تعداد بخش‌ها: <code>{total_parts}</code>"
        ),
        parse_mode="HTML"
    )

    for start in range(
        0,
        total_wallets,
        BATCH_SIZE
    ):

        batch = wallets[
            start:start + BATCH_SIZE
        ]

        part_number = (
            start // BATCH_SIZE
        ) + 1

        logging.info(
            "Scanning batch %s/%s",
            part_number,
            total_parts
        )

        loop = asyncio.get_running_loop()

        with ThreadPoolExecutor(
            max_workers=10
        ) as executor:

            futures = [
                loop.run_in_executor(
                    executor,
                    get_wallet_balance,
                    row[1]
                )
                for row in batch
            ]

            results = await asyncio.gather(
                *futures
            )

        batch_eth = 0.0
        batch_bsc = 0.0
        batch_rich = 0

        for row, balance in zip(
            batch,
            results
        ):

            wallet_id = row[0]
            address = row[1]
            uploaded_at = row[2]

            eth = balance["ETH"]
            bsc = balance["BSC"]

            batch_eth += eth
            batch_bsc += bsc

            total_eth += eth
            total_bsc += bsc

            if eth > 0 or bsc > 0:

                batch_rich += 1
                rich_count += 1

                uploaded_text = (
                    uploaded_at.isoformat()
                    if uploaded_at
                    else "نامشخص"
                )

                await bot.send_message(
                    chat_id=REPORT_CHANNEL,
                    text=(
                        "💰 <b>آدرس دارای موجودی</b>\n\n"
                        f"🔢 شناسه: <code>{wallet_id}</code>\n"
                        f"📍 آدرس:\n"
                        f"<code>{address}</code>\n\n"
                        f"🕐 زمان انتشار فایل:\n"
                        f"<code>{uploaded_text}</code>\n\n"
                        f"🔹 ETH: <code>{eth:.8f}</code>\n"
                        f"🔹 BSC: <code>{bsc:.8f}</code>"
                    ),
                    parse_mode="HTML"
                )

                await asyncio.sleep(0.5)

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                f"📊 <b>بخش {part_number}/{total_parts}</b>\n\n"
                f"🔢 تعداد آدرس: <code>{len(batch)}</code>\n"
                f"🔹 ETH: <code>{batch_eth:.8f}</code>\n"
                f"🔹 BSC: <code>{batch_bsc:.8f}</code>\n"
                f"💰 دارای موجودی: <code>{batch_rich}</code>"
            ),
            parse_mode="HTML"
        )

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=(
            "✅ <b>گزارش ساعتی تمام شد</b>\n\n"
            f"📦 کل آدرس‌ها: <code>{total_wallets}</code>\n"
            f"🔹 مجموع ETH: <code>{total_eth:.8f}</code>\n"
            f"🔹 مجموع BSC: <code>{total_bsc:.8f}</code>\n"
            f"💰 تعداد دارای موجودی: <code>{rich_count}</code>"
        ),
        parse_mode="HTML"
    )

    set_meta(
        "last_hourly",
        datetime.now(timezone.utc).isoformat()
    )

    logging.info(
        "Hourly scan completed"
    )


# ============================================================
# Export
# ============================================================

async def handle_export():
    bot = create_bot()

    wallets = get_all_wallets()

    if not wallets:

        set_meta(
            "last_export",
            datetime.now(timezone.utc).isoformat()
        )

        return

    output = io.StringIO()

    output.write(
        "Wallet Address Export\n"
    )

    output.write(
        f"Export time: "
        f"{datetime.now(timezone.utc).isoformat()}\n"
    )

    output.write(
        f"Total: {len(wallets)}\n\n"
    )

    for wallet_id, address, uploaded_at in wallets:

        output.write(
            f"{wallet_id} | "
            f"{address} | "
            f"{uploaded_at}\n"
        )

    data = io.BytesIO(
        output.getvalue().encode("utf-8")
    )

    data.name = "wallets_export.txt"

    await bot.send_document(
        chat_id=REPORT_CHANNEL,
        document=data,
        caption=(
            "📦 <b>خروجی دیتابیس</b>\n"
            f"🔢 تعداد: <code>{len(wallets)}</code>"
        ),
        parse_mode="HTML"
    )

    set_meta(
        "last_export",
        datetime.now(timezone.utc).isoformat()
    )

    logging.info(
        "Export sent"
    )


# ============================================================
# Worker
# ============================================================

def worker_loop():

    logging.info(
        "Worker started"
    )

    while True:

        task = task_queue.get()

        try:

            task_type = task["type"]

            logging.info(
                "Processing: %s",
                task_type
            )

            loop = asyncio.new_event_loop()

            asyncio.set_event_loop(loop)

            try:

                if task_type == "file":

                    loop.run_until_complete(
                        handle_file(
                            task["file_id"],
                            task["file_name"],
                            task["uploaded_at"]
                        )
                    )

                elif task_type == "hourly":

                    loop.run_until_complete(
                        handle_hourly()
                    )

                elif task_type == "export":

                    loop.run_until_complete(
                        handle_export()
                    )

            finally:

                loop.close()

        except Exception:

            logging.exception(
                "Worker task failed"
            )

        finally:

            task_queue.task_done()


# ============================================================
# Scheduler
# ============================================================

def scheduler_loop():

    logging.info(
        "Scheduler started"
    )

    while True:

        try:

            now = datetime.now(
                timezone.utc
            )

            # -------------------------
            # گزارش ساعتی
            # -------------------------

            last_hourly = get_meta(
                "last_hourly"
            )

            should_hourly = False

            if not last_hourly:

                should_hourly = True

            else:

                try:

                    previous = datetime.fromisoformat(
                        last_hourly
                    )

                    elapsed = (
                        now - previous
                    ).total_seconds()

                    if elapsed >= (
                        HOURLY_INTERVAL_MIN * 60
                    ):
                        should_hourly = True

                except Exception:

                    should_hourly = True

            if should_hourly:

                if task_queue.qsize() < 3:

                    task_queue.put({
                        "type": "hourly"
                    })

                    # جلوگیری از queue شدن چندباره
                    set_meta(
                        "last_hourly",
                        now.isoformat()
                    )

                    logging.info(
                        "Hourly job queued"
                    )

            # -------------------------
            # Export
            # -------------------------

            last_export = get_meta(
                "last_export"
            )

            should_export = False

            if not last_export:

                should_export = True

            else:

                try:

                    previous = datetime.fromisoformat(
                        last_export
                    )

                    elapsed = (
                        now - previous
                    ).total_seconds()

                    if elapsed >= (
                        EXPORT_INTERVAL_MIN * 60
                    ):
                        should_export = True

                except Exception:

                    should_export = True

            if should_export:

                if task_queue.qsize() < 3:

                    task_queue.put({
                        "type": "export"
                    })

                    set_meta(
                        "last_export",
                        now.isoformat()
                    )

                    logging.info(
                        "Export job queued"
                    )

        except Exception:

            logging.exception(
                "Scheduler error"
            )

        time.sleep(30)


# ============================================================
# Webhook
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    try:

        data = request.get_json(
            force=True
        )

        update = Update.de_json(
            data,
            bot=None
        )

        if not update:
            return "OK"

        if not update.channel_post:
            return "OK"

        post = update.channel_post

        if post.chat.id != SOURCE_CHANNEL:
            return "OK"

        if not post.document:
            return "OK"

        document = post.document

        uploaded_at = (
            post.date
            if post.date
            else datetime.now(timezone.utc)
        )

        logging.info(
            "New source file: %s",
            document.file_name
        )

        task_queue.put({
            "type": "file",
            "file_id": document.file_id,
            "file_name": document.file_name,
            "uploaded_at": uploaded_at
        })

        return "OK"

    except Exception:

        logging.exception(
            "Webhook error"
        )

        return "OK"


# ============================================================
# Health
# ============================================================

@app.route("/")
def health():

    try:

        count = len(
            get_all_wallets()
        )

        return (
            "OK | "
            f"Queue: {task_queue.qsize()} | "
            f"DB: {count}"
        )

    except Exception as e:

        return f"DB ERROR: {e}", 500


# ============================================================
# Start
# ============================================================

def start_background_workers():

    threading.Thread(
        target=worker_loop,
        daemon=True
    ).start()

    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()


def validate_config():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")

    if not SOURCE_CHANNEL:
        raise RuntimeError("SOURCE_CHANNEL is missing")

    if not REPORT_CHANNEL:
        raise RuntimeError("REPORT_CHANNEL is missing")


# این بخش هنگام import شدن توسط Gunicorn اجرا می‌شود
validate_config()
init_db()
start_background_workers()


if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )

