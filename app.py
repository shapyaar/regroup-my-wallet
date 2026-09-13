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
from psycopg2 import pool
from flask import Flask, request
from telegram import Bot, Update
from telegram.request import HTTPXRequest
from web3 import Web3


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

SOURCE_CHANNEL = int(os.getenv("SOURCE_CHANNEL", "0"))
REPORT_CHANNEL = int(os.getenv("REPORT_CHANNEL", "0"))

HOURLY_INTERVAL_MIN = int(
    os.getenv("HOURLY_INTERVAL_MIN", "60")
)

EXPORT_INTERVAL_MIN = int(
    os.getenv("EXPORT_INTERVAL_MIN", "120")
)

BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "300")
)

RPC_WORKERS = int(
    os.getenv("RPC_WORKERS", "10")
)

DB_MIN_CONNECTIONS = int(
    os.getenv("DB_MIN_CONNECTIONS", "1")
)

DB_MAX_CONNECTIONS = int(
    os.getenv("DB_MAX_CONNECTIONS", "10")
)


NETWORKS = {
    "ETH": "https://eth.llamarpc.com",
    "BSC": "https://bsc-dataseed.binance.org/",
}


# ============================================================
# Flask / Queue
# ============================================================

app = Flask(__name__)

task_queue = queue.Queue()

worker_started = False
worker_lock = threading.Lock()


# ============================================================
# PostgreSQL Connection Pool
# ============================================================

db_pool = None


def init_db_pool():
    global db_pool

    if db_pool is not None:
        return

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")

    logger.info("Creating PostgreSQL connection pool")

    db_pool = psycopg2.pool.ThreadedConnectionPool(
        DB_MIN_CONNECTIONS,
        DB_MAX_CONNECTIONS,
        DATABASE_URL,
        sslmode="require",
        connect_timeout=15
    )

    logger.info("PostgreSQL connection pool created")


def get_conn():
    if db_pool is None:
        init_db_pool()

    return db_pool.getconn()


def release_conn(conn):
    if db_pool is not None and conn is not None:
        db_pool.putconn(conn)


# ============================================================
# Database Initialization
# ============================================================

def init_db():
    conn = None

    try:
        conn = get_conn()
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
            CREATE INDEX IF NOT EXISTS idx_wallets_address
            ON wallets(address)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_id TEXT PRIMARY KEY,
                file_name TEXT,
                processed_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_updates (
                update_id BIGINT PRIMARY KEY,
                processed_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        conn.commit()

        logger.info("PostgreSQL initialized successfully")

    except Exception:
        if conn:
            conn.rollback()

        logger.exception("Database initialization failed")
        raise

    finally:
        release_conn(conn)


# ============================================================
# Wallet Database Functions
# ============================================================

def save_wallet(address, source_uploaded_at):
    """
    Insert wallet.
    Returns True if inserted, False if already existed.
    """

    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO wallets
                (address, source_uploaded_at)
            VALUES
                (%s, %s)
            ON CONFLICT (address) DO NOTHING
            RETURNING id
        """, (
            address.lower(),
            source_uploaded_at
        ))

        inserted = cur.fetchone() is not None

        conn.commit()

        return inserted

    except Exception:
        if conn:
            conn.rollback()

        raise

    finally:
        release_conn(conn)


def get_all_wallets():
    conn = None

    try:
        conn = get_conn()
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
        release_conn(conn)


def get_wallet_count():
    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*)
            FROM wallets
        """)

        row = cur.fetchone()

        return int(row[0]) if row else 0

    finally:
        release_conn(conn)


# ============================================================
# Meta
# ============================================================

def get_meta(key, default=None):
    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT value
            FROM meta
            WHERE key = %s
            """,
            (key,)
        )

        row = cur.fetchone()

        return row[0] if row else default

    finally:
        release_conn(conn)


def set_meta(key, value):
    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO meta
                (key, value)
            VALUES
                (%s, %s)
            ON CONFLICT (key)
            DO UPDATE SET
                value = EXCLUDED.value
        """, (
            key,
            value
        ))

        conn.commit()

    except Exception:
        if conn:
            conn.rollback()

        raise

    finally:
        release_conn(conn)


# ============================================================
# Processed Files / Updates
# ============================================================

def file_already_processed(file_id):
    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT 1
            FROM processed_files
            WHERE file_id = %s
            LIMIT 1
        """, (
            str(file_id),
        ))

        return cur.fetchone() is not None

    finally:
        release_conn(conn)


def mark_file_processed(file_id, file_name):
    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO processed_files
                (file_id, file_name)
            VALUES
                (%s, %s)
            ON CONFLICT (file_id) DO NOTHING
            RETURNING file_id
        """, (
            str(file_id),
            file_name
        ))

        inserted = cur.fetchone() is not None

        conn.commit()

        return inserted

    except Exception:
        if conn:
            conn.rollback()

        raise

    finally:
        release_conn(conn)


def update_already_processed(update_id):
    if not update_id:
        return False

    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO processed_updates
                (update_id)
            VALUES
                (%s)
            ON CONFLICT (update_id) DO NOTHING
            RETURNING update_id
        """, (
            int(update_id),
        ))

        inserted = cur.fetchone() is not None

        conn.commit()

        return not inserted

    except Exception:
        if conn:
            conn.rollback()

        raise

    finally:
        release_conn(conn)


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
# Input File
# ============================================================

ADDRESS_PATTERN = re.compile(
    r"Addr:\s*(0x[a-fA-F0-9]{40})",
    re.IGNORECASE
)


# ============================================================
# File Handler
# ============================================================

async def handle_file(
    file_id,
    file_name,
    uploaded_at
):
    bot = create_bot()

    try:
        logger.info(
            "Processing source file: %s | file_id=%s",
            file_name,
            file_id
        )

        # ----------------------------------------------------
        # Atomic duplicate protection
        # ----------------------------------------------------

        claimed = mark_file_processed(
            file_id,
            file_name
        )

        if not claimed:
            logger.info(
                "File already processed: %s",
                file_id
            )
            return

        # ----------------------------------------------------
        # Download
        # ----------------------------------------------------

        logger.info(
            "Downloading source file: %s",
            file_name
        )

        telegram_file = await bot.get_file(file_id)

        content = await telegram_file.download_as_bytearray()

        text = content.decode(
            "utf-8",
            errors="ignore"
        )

        # ----------------------------------------------------
        # Extract addresses
        # ----------------------------------------------------

        addresses = []

        for line in text.splitlines():

            match = ADDRESS_PATTERN.search(line)

            if match:
                address = match.group(1)

                addresses.append(address)

        if not addresses:

            logger.warning(
                "No addresses found in %s",
                file_name
            )

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "⚠️ <b>فایل دریافت شد ولی آدرسی پیدا نشد</b>\n\n"
                    f"📄 فایل: <code>{file_name}</code>"
                ),
                parse_mode="HTML"
            )

            return

        # ----------------------------------------------------
        # Save addresses
        # ----------------------------------------------------

        saved = 0

        # جلوگیری از تکرار داخل همان فایل
        unique_addresses = list(
            dict.fromkeys(
                address.lower()
                for address in addresses
            )
        )

        for address in unique_addresses:

            try:

                if save_wallet(
                    address,
                    uploaded_at
                ):
                    saved += 1

            except Exception:

                logger.exception(
                    "Failed saving wallet: %s",
                    address
                )

        total = get_wallet_count()

        # ----------------------------------------------------
        # Report
        # ----------------------------------------------------

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "📥 <b>فایل جدید دریافت شد</b>\n\n"
                f"📄 فایل: <code>{file_name}</code>\n"
                f"🕐 زمان انتشار: <code>{uploaded_at}</code>\n"
                f"🔢 آدرس‌های پیدا شده: <code>{len(addresses)}</code>\n"
                f"🔢 آدرس‌های یکتا: <code>{len(unique_addresses)}</code>\n"
                f"➕ آدرس جدید: <code>{saved}</code>\n"
                f"📦 کل دیتابیس: <code>{total}</code>"
            ),
            parse_mode="HTML"
        )

        logger.info(
            "File processed successfully | file=%s | found=%s | unique=%s | new=%s",
            file_name,
            len(addresses),
            len(unique_addresses),
            saved
        )

    except Exception:

        logger.exception(
            "File processing failed: %s",
            file_name
        )


# ============================================================
# Web3 Providers
# ============================================================

WEB3_PROVIDERS = {}


def init_web3():

    logger.info("Initializing Web3 providers")

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

            WEB3_PROVIDERS[network] = w3

            logger.info(
                "Web3 provider initialized: %s",
                network
            )

        except Exception:

            logger.exception(
                "Failed initializing Web3 provider: %s",
                network
            )


# ============================================================
# Balance
# ============================================================

def get_wallet_balance(address):

    result = {
        "ETH": 0.0,
        "BSC": 0.0
    }

    try:
        checksum = Web3.to_checksum_address(
            address
        )
    except Exception:
        logger.warning(
            "Invalid wallet address: %s",
            address
        )
        return result

    for network, w3 in WEB3_PROVIDERS.items():

        try:

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

            logger.warning(
                "Balance error | network=%s | address=%s | error=%s",
                network,
                address,
                e
            )

    return result


# ============================================================
# Hourly Report
# ============================================================

async def handle_hourly():

    bot = create_bot(30)

    wallets = get_all_wallets()

    total_wallets = len(wallets)

    logger.info(
        "Hourly scan started | total=%s",
        total_wallets
    )

    if not wallets:

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text="ℹ️ <b>گزارش ساعتی:</b> دیتابیس خالی است.",
            parse_mode="HTML"
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

    # --------------------------------------------------------
    # Executor واحد برای کل scan
    # --------------------------------------------------------

    loop = asyncio.get_running_loop()

    executor = ThreadPoolExecutor(
        max_workers=RPC_WORKERS
    )

    try:

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

            logger.info(
                "Scanning batch %s/%s",
                part_number,
                total_parts
            )

            futures = [
                loop.run_in_executor(
                    executor,
                    get_wallet_balance,
                    row[1]
                )
                for row in batch
            ]

            results = await asyncio.gather(
                *futures,
                return_exceptions=True
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

                if isinstance(
                    balance,
                    Exception
                ):

                    logger.warning(
                        "Wallet balance failed | address=%s | error=%s",
                        address,
                        balance
                    )

                    continue

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

                    try:

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

                    except Exception:

                        logger.exception(
                            "Failed sending rich wallet report: %s",
                            address
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

    finally:

        executor.shutdown(
            wait=True
        )

    # --------------------------------------------------------
    # Final report
    # --------------------------------------------------------

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

    logger.info(
        "Hourly scan completed | wallets=%s | rich=%s",
        total_wallets,
        rich_count
    )


# ============================================================
# Export
# ============================================================

async def handle_export():

    bot = create_bot()

    wallets = get_all_wallets()

    if not wallets:

        logger.info(
            "Export skipped: database is empty"
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

    logger.info(
        "Export sent | total=%s",
        len(wallets)
    )


# ============================================================
# Worker
# ============================================================

def worker_loop():

    logger.info(
        "Background worker started"
    )

    while True:

        task = task_queue.get()

        try:

            task_type = task.get("type")

            logger.info(
                "Processing task: %s",
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

                else:

                    logger.warning(
                        "Unknown task type: %s",
                        task_type
                    )

            finally:

                loop.close()

        except Exception:

            logger.exception(
                "Worker task failed"
            )

        finally:

            task_queue.task_done()


# ============================================================
# Scheduler Helpers
# ============================================================

def interval_elapsed(meta_key, interval_minutes):

    last_value = get_meta(
        meta_key
    )

    if not last_value:
        return True

    try:

        previous = datetime.fromisoformat(
            last_value
        )

        if previous.tzinfo is None:
            previous = previous.replace(
                tzinfo=timezone.utc
            )

        now = datetime.now(
            timezone.utc
        )

        elapsed = (
            now - previous
        ).total_seconds()

        return elapsed >= (
            interval_minutes * 60
        )

    except Exception:

        logger.exception(
            "Invalid scheduler timestamp | key=%s",
            meta_key
        )

        return True


def task_exists(task_type):

    # queue.Queue استاندارد API برای مشاهده محتوا ندارد.
    # qsize فقط برای جلوگیری از رشد بیش از حد queue استفاده می‌شود.
    if task_queue.qsize() >= 3:
        return True

    return False


# ============================================================
# Scheduler
# ============================================================

def scheduler_loop():

    logger.info(
        "Scheduler started"
    )

    while True:

        try:

            now = datetime.now(
                timezone.utc
            )

            # ------------------------------------------------
            # Hourly
            # ------------------------------------------------

            if interval_elapsed(
                "last_hourly",
                HOURLY_INTERVAL_MIN
            ):

                if not task_exists("hourly"):

                    task_queue.put({
                        "type": "hourly"
                    })

                    # زمان queue شدن ذخیره می‌شود
                    # تا scheduler چند بار همان job را queue نکند.
                    set_meta(
                        "last_hourly",
                        now.isoformat()
                    )

                    logger.info(
                        "Hourly job queued"
                    )

            # ------------------------------------------------
            # Export
            # ------------------------------------------------

            if interval_elapsed(
                "last_export",
                EXPORT_INTERVAL_MIN
            ):

                if not task_exists("export"):

                    task_queue.put({
                        "type": "export"
                    })

                    set_meta(
                        "last_export",
                        now.isoformat()
                    )

                    logger.info(
                        "Export job queued"
                    )

        except Exception:

            logger.exception(
                "Scheduler error"
            )

        time.sleep(30)


# ============================================================
# Background Workers Startup
# ============================================================

def start_background_workers():

    global worker_started

    with worker_lock:

        if worker_started:

            logger.info(
                "Background workers already started"
            )

            return

        logger.info(
            "Starting background workers"
        )

        threading.Thread(
            target=worker_loop,
            name="wallet-worker",
            daemon=True
        ).start()

        threading.Thread(
            target=scheduler_loop,
            name="wallet-scheduler",
            daemon=True
        ).start()

        worker_started = True

        logger.info(
            "Background workers started successfully"
        )


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
            force=True,
            silent=True
        )

        if not data:

            logger.warning(
                "Webhook received without JSON"
            )

            return "OK", 200

        # ----------------------------------------------------
        # Parse Telegram update
        # ----------------------------------------------------

        update = Update.de_json(
            data,
            bot=None
        )

        if not update:
            return "OK", 200

        # ----------------------------------------------------
        # Duplicate update protection
        # ----------------------------------------------------

        update_id = getattr(
            update,
            "update_id",
            None
        )

        if update_id:

            try:

                if update_already_processed(
                    update_id
                ):

                    logger.info(
                        "Duplicate Telegram update ignored: %s",
                        update_id
                    )

                    return "OK", 200

            except Exception:

                logger.exception(
                    "Could not check update_id"
                )

        # ----------------------------------------------------
        # Channel post
        # ----------------------------------------------------

        if not update.channel_post:
            return "OK", 200

        post = update.channel_post

        # ----------------------------------------------------
        # Source channel
        # ----------------------------------------------------

        if post.chat.id != SOURCE_CHANNEL:

            logger.info(
                "Ignoring post from channel: %s",
                post.chat.id
            )

            return "OK", 200

        # ----------------------------------------------------
        # Document
        # ----------------------------------------------------

        if not post.document:
            return "OK", 200

        document = post.document

        file_id = document.file_id

        file_name = (
            document.file_name
            or "unknown_file.txt"
        )

        uploaded_at = (
            post.date
            if post.date
            else datetime.now(timezone.utc)
        )

        logger.info(
            "New source file: %s | file_id=%s | update_id=%s",
            file_name,
            file_id,
            update_id
        )

        # ----------------------------------------------------
        # Queue
        # ----------------------------------------------------

        task_queue.put({
            "type": "file",
            "file_id": file_id,
            "file_name": file_name,
            "uploaded_at": uploaded_at
        })

        return "OK", 200

    except Exception:

        logger.exception(
            "Webhook error"
        )

        # Telegram should receive 200 so it doesn't
        # repeatedly resend the same webhook.
        return "OK", 200


# ============================================================
# Health
# ============================================================

@app.route("/")
def health():

    # این endpoint عمداً به DB وابسته نیست.
    # Render باید بتواند بدون DB هم سرویس HTTP را alive ببیند.

    return (
        "OK | "
        f"Queue: {task_queue.qsize()}",
        200
    )


@app.route("/health/db")
def db_health():

    conn = None

    try:

        conn = get_conn()

        cur = conn.cursor()

        cur.execute(
            "SELECT 1"
        )

        cur.fetchone()

        return (
            "DB OK | "
            f"Wallets: {get_wallet_count()}",
            200
        )

    except Exception as e:

        logger.exception(
            "Database health check failed"
        )

        return (
            f"DB ERROR: {e}",
            500
        )

    finally:

        release_conn(conn)


@app.route("/health/full")
def full_health():

    try:

        count = get_wallet_count()

        return {
            "status": "ok",
            "database": "ok",
            "wallets": count,
            "queue": task_queue.qsize(),
            "worker_started": worker_started
        }, 200

    except Exception as e:

        logger.exception(
            "Full health check failed"
        )

        return {
            "status": "error",
            "error": str(e),
            "queue": task_queue.qsize(),
            "worker_started": worker_started
        }, 500


# ============================================================
# Configuration Validation
# ============================================================

def validate_config():

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing"
        )

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing"
        )

    if not SOURCE_CHANNEL:
        raise RuntimeError(
            "SOURCE_CHANNEL is missing"
        )

    if not REPORT_CHANNEL:
        raise RuntimeError(
            "REPORT_CHANNEL is missing"
        )

    logger.info(
        "Configuration validated"
    )


# ============================================================
# Application Initialization
# ============================================================

def initialize_application():

    logger.info(
        "Initializing application"
    )

    validate_config()

    init_db_pool()

    init_db()

    init_web3()

    start_background_workers()

    logger.info(
        "Application initialization completed"
    )


# ============================================================
# IMPORTANT:
# Gunicorn imports "app" instead of executing
# __main__. Therefore initialization must happen here.
# ============================================================

try:

    initialize_application()

except Exception:

    logger.exception(
        "Application initialization failed"
    )

    raise


# ============================================================
# Local Development
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    logger.info(
        "Starting Flask development server on port %s",
        port
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
