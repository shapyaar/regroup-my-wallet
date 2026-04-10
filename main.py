import os
import re
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3
from keep_alive import keep_alive

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- تنظیمات ---
BOT_TOKEN = '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
SOURCE_CHANNEL = 'rrxfq'
REPORT_CHANNEL = '@regroupmywallet'

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
    'POLYGON': 'https://polygon-rpc.com',
    'ARB': 'https://arb1.arbitrum.io/rpc',
    'OP': 'https://mainnet.optimism.io'
}

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
    # لاگ برای هر پیام دریافتی در کنسول رندر
    logging.info("New message received in a channel...")

    if not update.channel_post or not update.channel_post.document:
        return

    # بررسی منبع
    chat = update.channel_post.chat
    if not chat.username or chat.username.lower() != SOURCE_CHANNEL.lower():
        logging.info(f"Message from unknown channel: {chat.username}")
        return

    doc = update.channel_post.document
    logging.info(f"Target file found: {doc.file_name}")

    try:
        # اطلاع‌رسانی فوری در کانال مقصد
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=f"📥 فایل `{doc.file_name}` در کانال سورس رویت شد.\n⏳ شروع استخراج آدرس‌ها و استعلام موجودی...")

        file = await context.bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        text = content.decode('utf-8', errors='ignore')

        # استخراج تمام آدرس‌ها
        addresses = list(set(re.findall(r"0x[a-fA-F0-9]{40}", text)))
        
        if not addresses:
            await context.bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ هیچ آدرس ولتی در فایل `{doc.file_name}` یافت نشد.")
            return

        # پردازش موازی سریع
        file_totals = {net: 0.0 for net in NETWORKS}
        with ThreadPoolExecutor(max_workers=30) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr in addresses]
            results = await asyncio.gather(*tasks)

        for res in results:
            for net in NETWORKS:
                file_totals[net] += res[net]

        # ارسال گزارش نهایی
        report_msg = f"📊 **گزارش موجودی کل فایل**\n"
        report_msg += f"📄 نام فایل: `{doc.file_name}`\n"
        report_msg += f"🔢 تعداد آدرس اسکن شده: `{len(addresses)}`\n"
        report_msg += "──────────────────\n"
        for net, amount in file_totals.items():
            report_msg += f"🔹 {net}: `{amount:.6f}`\n"
        report_msg += "──────────────────"
        
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=report_msg, parse_mode='Markdown')
        logging.info("Done!")

    except Exception as e:
        logging.error(f"Error: {e}")
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=f"❌ خطای سیستمی: {str(e)}")

if __name__ == '__main__':
    keep_alive()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.Document.ALL, process_report))
    logging.info("Bot is running and waiting for file...")
    app.run_polling(drop_pending_updates=True)
