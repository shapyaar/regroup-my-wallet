import os
import re
import asyncio
import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3
from keep_alive import keep_alive

# تنظیمات لاگ برای دیدن خطاها در کنسول
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- تنظیمات ---
BOT_TOKEN = '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
SOURCE_CHANNEL = 'rrxfq' # بدون @ برای مقایسه راحت‌تر
REPORT_CHANNEL_ID = '@regroupmywallet'

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
    'POLYGON': 'https://polygon-rpc.com',
    'ARB': 'https://arb1.arbitrum.io/rpc',
    'OP': 'https://mainnet.optimism.io'
}

def check_all_balances(address):
    results = {}
    for name, rpc in NETWORKS.items():
        try:
            # ایجاد اتصال جدید برای هر درخواست جهت جلوگیری از بلاک شدن
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 10}))
            checksum_addr = Web3.to_checksum_address(address.strip())
            bal_wei = w3.eth.get_balance(checksum_addr)
            results[name] = float(w3.from_wei(bal_wei, 'ether'))
        except Exception as e:
            logging.error(f"Error checking {name} for {address}: {e}")
            results[name] = 0.0
    return results

async def process_report_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # لاگ کردن برای تست: آیا پیامی دریافت شد؟
    logging.info("A new post detected in a channel...")

    if not update.channel_post:
        return

    # بررسی یوزرنیم کانال (حساس نبودن به بزرگ و کوچکی حروف)
    current_chat = update.channel_post.chat
    if not current_chat.username or current_chat.username.lower() != SOURCE_CHANNEL.lower():
        logging.info(f"Message ignored from: {current_chat.username}")
        return

    doc = update.channel_post.document
    if not doc:
        logging.info("Post has no document.")
        return

    logging.info(f"Processing file: {doc.file_name}")

    try:
        # اطلاع‌رسانی اولیه در کانال ریپورت
        await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=f"📥 دریافت فایل `{doc.file_name}` از سورس. در حال محاسبه...")

        file = await context.bot.get_file(doc.file_id)
        content_bytes = await file.download_as_bytearray()
        content = content_bytes.decode('utf-8', errors='ignore')

        addresses = re.findall(r"0x[a-fA-F0-9]{40}", content)
        
        # اگر آدرسی پیدا نشد، باز هم گزارش بده
        if not addresses:
            await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=f"⚠️ فایل `{doc.file_name}` اسکن شد اما هیچ آدرس ولتی (0x...) در آن یافت نشد.")
            return

        total_summary = {net: 0.0 for net in NETWORKS.keys()}
        
        # پیمایش آدرس‌ها و جمع زدن موجودی
        for addr in set(addresses): # استفاده از set برای حذف آدرس‌های تکراری در یک فایل
            balances = check_all_balances(addr)
            for net, amount in balances.items():
                total_summary[net] += amount

        # ساخت گزارش نهایی
        report = f"📋 **گزارش موجودی کل فایل**\n"
        report += f"📄 نام فایل: `{doc.file_name}`\n"
        report += f"🔢 تعداد آدرس‌ها: {len(addresses)}\n"
        report += "──────────────────\n"
        for net, total in total_summary.items():
            report += f"🔹 {net}: `{total:.6f}`\n"
        report += "──────────────────"

        await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=report, parse_mode='Markdown')
        logging.info("Report sent successfully.")

    except Exception as e:
        logging.error(f"General error: {e}")
        await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=f"❌ خطا در پردازش فایل `{doc.file_name}`: {e}")

if __name__ == '__main__':
    keep_alive()
    # استفاده از تنظیمات برای اطمینان از دریافت پست‌های کانال
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # هندلر بدون فیلتر سختگیرانه برای تست اولیه
    application.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.CHANNEL, process_report_file))
    
    logging.info("Bot started. Listening for channel posts...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
