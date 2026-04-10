import os
import re
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3
from keep_alive import keep_alive

# تنظیمات لاگ
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- تنظیمات اصلی ---
BOT_TOKEN = '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
SOURCE_CHANNEL = 'rrxfq' # یوزرنیم کانال منبع بدون @
REPORT_CHANNEL = '@regroupmywallet' # کانال مقصد گزارش

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
            checksum = Web3.to_checksum_address(address)
            balance = w3.eth.get_balance(checksum)
            if balance > 0:
                local_totals[net] = float(w3.from_wei(balance, 'ether'))
        except:
            continue
    return local_totals

async def process_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ۱. بررسی منبع پیام: فقط پست‌های کانال سورس پردازش شوند
    if not update.channel_post:
        return
    
    current_chat = update.channel_post.chat
    if not current_chat.username or current_chat.username.lower() != SOURCE_CHANNEL.lower():
        return

    # ۲. بررسی وجود فایل
    doc = update.channel_post.document
    if not doc or not doc.file_name or 'report' not in doc.file_name.lower():
        return

    logging.info(f"🚀 فایل هدف پیدا شد: {doc.file_name}")
    
    try:
        # ارسال پیام شروع به کانال ریپورت
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=f"📥 فایل جدید در `{SOURCE_CHANNEL}` دریافت شد.\n🔍 در حال محاسبه موجودی کل آدرس‌های داخل فایل...")

        # دانلود فایل
        tg_file = await context.bot.get_file(doc.file_id)
        file_content = await tg_file.download_as_bytearray()
        text = file_content.decode('utf-8', errors='ignore')

        # استخراج آدرس‌ها
        addresses = re.findall(r"0x[a-fA-F0-9]{40}", text)
        unique_addrs = list(set(addresses))
        
        if not unique_addrs:
            await context.bot.send_message(chat_id=REPORT_CHANNEL, text=f"⚠️ فایل `{doc.file_name}` خالی از آدرس بود.")
            return

        # پردازش موازی برای سرعت بالا
        file_totals = {net: 0.0 for net in NETWORKS}
        with ThreadPoolExecutor(max_workers=25) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, get_wallet_total, addr) for addr in unique_addrs]
            results = await asyncio.gather(*tasks)

        for res in results:
            for net in NETWORKS:
                file_totals[net] += res[net]

        # ساخت گزارش نهایی
        report_msg = f"✅ **گزارش مجموع موجودی فایل**\n"
        report_msg += f"📄 فایل: `{doc.file_name}`\n"
        report_msg += f"🔢 تعداد ولت: `{len(unique_addrs)}`\n"
        report_msg += "──────────────────\n"
        for net, amount in file_totals.items():
            report_msg += f"🔹 {net}: `{amount:.6f}`\n"
        report_msg += "──────────────────"
        
        await context.bot.send_message(chat_id=REPORT_CHANNEL, text=report_msg, parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Error: {e}")

if __name__ == '__main__':
    keep_alive()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # فیلتر فقط برای اسناد در کانال
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.Document.ALL, process_report))
    
    logging.info(f"🤖 ربات فعال شد. در حال مانیتور کانال @{SOURCE_CHANNEL}...")
    app.run_polling(drop_pending_updates=True)
