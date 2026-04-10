import os
import re
import logging
import asyncio
from flask import Flask, request
from concurrent.futures import ThreadPoolExecutor
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3

# تنظیمات لاگ
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- تنظیمات اصلی ---
BOT_TOKEN = '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
SOURCE_CHANNEL = 'rrxfq'
REPORT_CHANNEL = '@regroupmywallet'
# آدرس اپلیکیشن خودت در رندر را اینجا جایگزین کن (مثلا https://my-bot.onrender.com)
RENDER_URL = "https://regroup-my-wallet.onrender.com" 

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
    'POLYGON': 'https://polygon-rpc.com',
    'ARB': 'https://arb1.arbitrum.io/rpc',
    'OP': 'https://mainnet.optimism.io'
}

app = Flask(__name__)
tg_app = ApplicationBuilder().token(BOT_TOKEN).build()

def get_wallet_total(address):
    local_totals = {net: 0.0 for net in NETWORKS}
    for net, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 5}))
            checksum = Web3.to_checksum_address(address.strip())
            balance = w3.eth.get_balance(checksum)
            local_totals[net] = float(w3.from_wei(balance, 'ether'))
        except: continue
    return local_totals

async def process_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.channel_post or not update.channel_post.document:
        return
    
    chat = update.channel_post.chat
    if not chat.username or chat.username.lower() != SOURCE_CHANNEL.lower():
        return

    doc = update.channel_post.document
    logging.info(f"Target file found: {doc.file_name}")

    try:
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=f"📥 فایل `{doc.file_name}` رویت شد. در حال پردازش موازی...")

        file = await context.bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        addresses = list(set(re.findall(r"0x[a-fA-F0-9]{40}", text)))
        
        if not addresses:
            await context.bot.send_message(chat_id=REPORT_CHANNEL, text="❌ آدرسی یافت نشد.")
            return

        file_totals = {net: 0.0 for net in NETWORKS}
        with ThreadPoolExecutor(max_workers=30) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr in addresses]
            results = await asyncio.gather(*tasks)

        for res in results:
            for net in NETWORKS:
                file_totals[net] += res[net]

        report_msg = f"📊 **گزارش فایل جدید**\n🔢 ولت‌ها: `{len(addresses)}`\n"
        for net, amount in file_totals.items():
            report_msg += f"🔹 {net}: `{amount:.6f}`\n"
        
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=report_msg, parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Error: {e}")

# مسیر دریافت آپدیت از تلگرام
@app.route('/webhook', methods=['POST'])
async def webhook():
    update = Update.de_json(request.get_json(force=True), tg_app.bot)
    await tg_app.process_update(update)
    return "OK"

@app.route('/')
def index():
    return "Bot is Running!"

async def start_webhook():
    # تنظیم آدرس وب‌هوک در سرور تلگرام
    webhook_url = f"{RENDER_URL}/webhook"
    await tg_app.bot.set_webhook(url=webhook_url)
    logging.info(f"Webhook set to {webhook_url}")

if __name__ == '__main__':
    # اضافه کردن هندلر
    tg_app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.Document.ALL, process_report))
    
    # اجرای هم‌زمان Flask و تنظیم وب‌هوک
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_webhook())
    
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
