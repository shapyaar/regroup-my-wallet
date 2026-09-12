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

# زمان‌بندی قابل تنظیم (دقیقه)
# برای تست: 60 و 120
# برای واقعی بعداً: 1440 (24 ساعت) و 2880 (48 ساعت) یا هرچی خواستی
HOURLY_INTERVAL_MIN = int(os.environ.get("HOURLY_INTERVAL_MIN", "60"))
EXPORT_INTERVAL_MIN = int(os.environ.get("EXPORT_INTERVAL_MIN", "120"))

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
}

DB_PATH = "wallets.db"
PARTS_PER_FILE = 7          # هر فایل به ۷ قسمت
HOURLY_BATCH_SIZE = 300     # گزارش ساعتی هر ۳۰۰ تا

app = Flask(__name__)
task_queue = queue.Queue()   # صف کارها


# ==================== دیتابیس ====================
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


# ==================== موجودی ====================
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


# ==================== پردازش یک قسمت ====================
async def process_part(bot, items, title, part_num, total_parts):
    """اسکن یک قسمت و ارسال گزارش"""
    totals = {'ETH': 0.0, 'BSC': 0.0}
    rich = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        loop = asyncio.get_running_loop()
        tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr, _ in items]
        results = await asyncio.gather(*tasks)

    for (addr, seed), res in zip(items, results):
        totals['ETH'] += res['ETH']
        totals['BSC'] += res['BSC']
        if res['ETH'] + res['BSC'] > 0.00001:
            rich.append({'address': addr, 'seed': seed, 'balances': res})

    report = (
        f"{title}\n"
        f"📦 قسمت {part_num}/{total_parts}\n"
        f"🔢 تعداد این قسمت: `{len(items)}`\n"
        f"──────────────────\n"
        f"🔹 ETH: `{totals['ETH']:.6f}`\n"
        f"🔹 BSC: `{totals['BSC']:.6f}`\n"
        f"💰 دارای موجودی: `{len(rich)}`"
    )
    await bot.send_message(chat_id=REPORT_CHANNEL, text=report, parse_mode='Markdown')

    for w in rich:
        msg = f"`{w['address']}`\n🔑 `{w['seed']}`\n"
        if w['balances']['ETH'] > 0:
            msg += f"• ETH: `{w['balances']['ETH']:.6f}`\n"
        if w['balances']['BSC'] > 0:
            msg += f"• BSC: `{w['balances']['BSC']:.6f}`\n"
        await bot.send_message(chat_id=REPORT_CHANNEL, text=msg, parse_mode='Markdown')
        await asyncio.sleep(0.35)

    return len(rich)


# ==================== ۱. پردازش فایل (تقسیم به ۷ قسمت) ====================
async def handle_file(doc):
    request_config = HTTPXRequest(connection_pool_size=25, pool_timeout=60.0, connect_timeout=30.0, read_timeout=60.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    try:
        logging.info(f"[FILE] Start: {doc.file_name}")

        file = await bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        pattern = r"Phrase:\s*(.+?)\s*\|\s*Addr:\s*(0x[a-fA-F0-9]{40})"
        matches = re.findall(pattern, text, re.IGNORECASE)
        logging.info(f"[FILE] Found {len(matches)} wallets")

        if not matches:
            await bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ در `{doc.file_name}` آدرسی پیدا نشد.")
            return

        # ذخیره همه در دیتابیس
        for phrase, addr in matches:
            save_wallet(addr, phrase)

        test_id_match = re.search(r"تعداد تست[:\s]*(\d+)", text)
        test_id = test_id_match.group(1) if test_id_match else "نامشخص"

        # تبدیل به لیست (addr, seed)
        items = [(addr, phrase.strip()) for phrase, addr in matches]

        # تقسیم به ۷ قسمت
        total = len(items)
        part_size = (total + PARTS_PER_FILE - 1) // PARTS_PER_FILE

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=f"📥 **فایل جدید**\n📄 `{doc.file_name}`\n🆔 تست: `{test_id}`\n🔢 کل ولت: `{total}`\n📦 تقسیم به {PARTS_PER_FILE} قسمت"
        )

        total_rich = 0
        for i in range(PARTS_PER_FILE):
            start = i * part_size
            end = min(start + part_size, total)
            if start >= total:
                break
            part_items = items[start:end]
            title = f"📄 **گزارش فایل** `{doc.file_name}`"
            rich_count = await process_part(bot, part_items, title, i + 1, PARTS_PER_FILE)
            total_rich += rich_count
            logging.info(f"[FILE] Part {i+1}/{PARTS_PER_FILE} done")

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=f"✅ **پایان فایل** `{doc.file_name}`\n💰 مجموع دارای موجودی: `{total_rich}`"
        )
        logging.info(f"[FILE] Finished: {doc.file_name}")

    except Exception as e:
        logging.error(f"[FILE] Error: {e}", exc_info=True)
        try:
            await bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ خطا در فایل: {str(e)[:180]}")
        except Exception:
            pass


# ==================== ۳. گزارش ساعتی (هر ۳۰۰ تا) ====================
async def handle_hourly():
    request_config = HTTPXRequest(connection_pool_size=25, pool_timeout=60.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        logging.info("[HOURLY] No wallets")
        return

    logging.info(f"[HOURLY] Start: {len(wallets)} wallets")

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=f"⏰ **شروع گزارش ساعتی کل دیتابیس**\n🔢 تعداد کل: `{len(wallets)}`\n📅 `{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC`"
    )

    total_rich = 0
    total_parts = (len(wallets) + HOURLY_BATCH_SIZE - 1) // HOURLY_BATCH_SIZE

    for i in range(0, len(wallets), HOURLY_BATCH_SIZE):
        part_items = wallets[i:i + HOURLY_BATCH_SIZE]
        part_num = (i // HOURLY_BATCH_SIZE) + 1
        title = f"⏰ **گزارش ساعتی دیتابیس**"
        rich_count = await process_part(bot, part_items, title, part_num, total_parts)
        total_rich += rich_count
        logging.info(f"[HOURLY] Part {part_num}/{total_parts} done")

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=f"✅ **پایان گزارش ساعتی**\n💰 مجموع دارای موجودی: `{total_rich}`"
    )
    set_meta("last_hourly", datetime.utcnow().isoformat())
    logging.info("[HOURLY] Finished")


# ==================== ۴. خروجی فایل دیتابیس ====================
async def handle_export():
    request_config = HTTPXRequest(connection_pool_size=10)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        logging.info("[EXPORT] No wallets")
        return

    content = f"Database Export - {datetime.utcnow().isoformat()} UTC\nTotal: {len(wallets)}\n\n"
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


# ==================== Worker صف ====================
def worker():
    logging.info("Worker started")
    while True:
        try:
            task = task_queue.get()
            task_type = task.get("type")

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


# ==================== زمان‌بندی ====================
def scheduler():
    logging.info("Scheduler started")
    while True:
        try:
            now = datetime.utcnow()

            last_hourly = get_meta("last_hourly")
            if last_hourly is None or (now - datetime.fromisoformat(last_hourly)).total_seconds() >= HOURLY_INTERVAL_MIN * 60:
                task_queue.put({"type": "hourly"})
                logging.info("Queued hourly report")

            last_export = get_meta("last_export")
            if last_export is None or (now - datetime.fromisoformat(last_export)).total_seconds() >= EXPORT_INTERVAL_MIN * 60:
                task_queue.put({"type": "export"})
                logging.info("Queued export")

        except Exception as e:
            logging.error(f"Scheduler error: {e}")

        threading.Event().wait(30)


# ==================== شروع ====================
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
            logging.info(f"Queued file | Queue size: {task_queue.qsize()}")

    return "OK"


@app.route('/')
def health():
    return f"Bot running | Queue: {task_queue.qsize()} | DB: {len(get_all_wallets())}"
