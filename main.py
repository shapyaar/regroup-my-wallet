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
BOT_TOKEN = '8644566801:AAGlL5vD4o4IvtV4yYHiukuLqLMfC1ouEMY'
SOURCE_CHANNEL = 'rrxfq'
REPORT_CHANNEL = '@regroupmywallet'
# آدرس اپلیکیشن خود در رندر را اینجا وارد کنید (مثلاً https://mybot.onrender.com)
RENDER_URL = "https://regroupmywallet-1.onrender.com" 

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
    'POLYGON': 'https://polygon-rpc.com',
    'ARB': 'https://arb1.arbitrum.io/rpc',
    'OP': 'https://mainnet.optimism.io'
}

app = Flask(__name__)
# ساخت اپلیکیشن تلگرام بدون استارت خودکار
tg_app = ApplicationBuilder().token(BOT_TOKEN).build()

def get_wallet_total(address):
    local_totals = {net: 0.0 for net in NETWORKS}
    for net, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 5}))
            checksum = Web3.to_checksum_address(address.strip())
            balance = w3.eth.get_balance(checksum)
            if balance > 0:
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
    logging.info(f"Processing target file: {doc.file_name}")

    try:
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=f"📥 فایل `{doc.file_name}` دریافت شد.\n⏳ در حال جمع‌آوری موجودی کل...")

        file = await context.bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        addresses = list(set(re.findall(r"0x[a-fA-F0-9]{40}", text)))
        
        if not addresses:
            await context.bot.send_message(chat_id=REPORT_CHANNEL, text="❌ آدرس معتبری در فایل یافت نشد.")
            return

        file_totals = {net: 0.0 for net in NETWORKS}
        
        # استفاده از ThreadPool برای پردازش موازی سریع
        with ThreadPoolExecutor(max_workers=30) as executor:
            loop = asyncio.get_running_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr in addresses]
            results = await asyncio.gather(*tasks)

        for res in results:
            for net in NETWORKS:
                file_totals[net] += res[net]

        report_msg = f"📊 **گزارش مجموع موجودی فایل**\n"
        report_msg += f"📄 فایل: `{doc.file_name}`\n"
        report_msg += f"🔢 ولت‌های اسکن شده: `{len(addresses)}`\n"
        report_msg += "──────────────────\n"
        for net, amount in file_totals.items():
            report_msg += f"🔹 {net}: `{amount:.6f}`\n"
        
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=report_msg, parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Error: {e}")

@app.route('/webhook', methods=['POST'])
async def webhook():
    """دریافت آپدیت‌ها از تلگرام"""
    if request.method == "POST":
        update = Update.de_json(request.get_json(force=True), tg_app.bot)
        await tg_app.process_update(update)
    return "OK"

@app.route('/')
def health_check():
    return "Bot is running on Webhook mode!"

async def main():
    # تنظیم هندلرها
    tg_app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.Document.ALL, process_report))
    
    # تنظیم وب‌هوک
    webhook_url = f"{RENDER_URL}/webhook"
    await tg_app.bot.set_webhook(url=webhook_url, allowed_updates=Update.ALL_TYPES)
    logging.info(f"Webhook set to {webhook_url}")
    
    # اجرای سرور Flask روی پورت رندر
    port = int(os.environ.get('PORT', 10000))
    from werkzeug.serving import run_simple
    run_simple('0.0.0.0', port, app, use_reloader=False)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
