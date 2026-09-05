```python
import os
import re
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    filters,
)
from web3 import Web3


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# SETTINGS
# =========================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

SOURCE_CHANNEL = int(
    os.environ.get("SOURCE_CHANNEL", "-1003533610913")
)

REPORT_CHANNEL = int(
    os.environ.get("REPORT_CHANNEL", "-1003893481541")
)

RENDER_URL = os.environ.get(
    "RENDER_URL",
    "https://regroupmywallet.onrender.com"
).rstrip("/")


# =========================================================
# NETWORKS
# =========================================================

NETWORKS = {
    "ETH": "https://eth.llamarpc.com",
    "BSC": "https://bsc-dataseed.binance.org/",
    "POLYGON": "https://polygon-rpc.com",
    "ARB": "https://arb1.arbitrum.io/rpc",
    "OP": "https://mainnet.optimism.io",
}


# =========================================================
# FLASK
# =========================================================

app = Flask(__name__)


# =========================================================
# TELEGRAM APPLICATION
# =========================================================

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN environment variable is not set."
    )

tg_app = ApplicationBuilder().token(BOT_TOKEN).build()


# =========================================================
# WEB3
# =========================================================

def get_wallet_total(address):
    """
    بررسی موجودی Native Coin یک آدرس در تمام شبکه‌ها.
    """

    totals = {
        network: 0.0
        for network in NETWORKS
    }

    address = address.strip()

    try:
        checksum = Web3.to_checksum_address(address)
    except Exception:
        return totals

    for network, rpc in NETWORKS.items():

        try:
            w3 = Web3(
                Web3.HTTPProvider(
                    rpc,
                    request_kwargs={
                        "timeout": 10
                    }
                )
            )

            if not w3.is_connected():
                continue

            balance_wei = w3.eth.get_balance(checksum)

            if balance_wei > 0:
                totals[network] = float(
                    w3.from_wei(
                        balance_wei,
                        "ether"
                    )
                )

        except Exception as e:
            logger.warning(
                "RPC error %s / %s: %s",
                network,
                address,
                e
            )

    return totals


# =========================================================
# REPORT PROCESSOR
# =========================================================

async def process_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not update.channel_post:
        return

    message = update.channel_post

    if not message.document:
        return

    chat = message.chat

    # فقط کانال منبع
    if chat.id != SOURCE_CHANNEL:
        logger.info(
            "Ignored message from channel %s",
            chat.id
        )
        return

    doc = message.document

    file_name = doc.file_name or "unknown_report"

    # زمان دقیق پیام تلگرام
    message_date = message.date

    logger.info(
        "Processing report: %s | message_id=%s | date=%s",
        file_name,
        message.message_id,
        message_date
    )

    try:

        # -------------------------------------------------
        # دریافت اولیه
        # -------------------------------------------------

        await context.bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "📥 فایل Report دریافت شد.\n\n"
                f"📄 فایل: `{file_name}`\n"
                f"🆔 Message ID: `{message.message_id}`\n"
                f"🕒 تاریخ: `{message_date.strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                "⏳ در حال بررسی ولت‌ها..."
            ),
            parse_mode="Markdown"
        )

        # -------------------------------------------------
        # دانلود فایل
        # -------------------------------------------------

        telegram_file = await context.bot.get_file(
            doc.file_id
        )

        content = await telegram_file.download_as_bytearray()

        text = content.decode(
            "utf-8",
            errors="ignore"
        )

        # -------------------------------------------------
        # استخراج آدرس‌های EVM
        # -------------------------------------------------

        addresses = list(
            dict.fromkeys(
                re.findall(
                    r"0x[a-fA-F0-9]{40}",
                    text
                )
            )
        )

        logger.info(
            "Found %s wallet addresses in %s",
            len(addresses),
            file_name
        )

        if not addresses:

            await context.bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "❌ هیچ آدرس معتبر EVM در فایل پیدا نشد.\n\n"
                    f"📄 فایل: `{file_name}`"
                ),
                parse_mode="Markdown"
            )

            return

        # -------------------------------------------------
        # مجموع موجودی‌ها
        # -------------------------------------------------

        file_totals = {
            network: 0.0
            for network in NETWORKS
        }

        # حداکثر 30 پردازش همزمان
        with ThreadPoolExecutor(
            max_workers=30
        ) as executor:

            loop = asyncio.get_running_loop()

            tasks = [
                loop.run_in_executor(
                    executor,
                    get_wallet_total,
                    address
                )
                for address in addresses
            ]

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True
            )

        # -------------------------------------------------
        # جمع نتایج
        # -------------------------------------------------

        for result in results:

            if isinstance(
                result,
                Exception
            ):
                continue

            for network in NETWORKS:
                file_totals[network] += result.get(
                    network,
                    0.0
                )

        # -------------------------------------------------
        # گزارش نهایی
        # -------------------------------------------------

        report = (
            "📊 *گزارش مجموع موجودی فایل*\n\n"
            f"📄 فایل: `{file_name}`\n"
            f"🆔 Message ID: `{message.message_id}`\n"
            f"🕒 تاریخ دریافت: `{message_date.strftime('%Y-%m-%d %H:%M:%S')}`\n"
            f"🔢 ولت‌های اسکن شده: `{len(addresses)}`\n"
            "──────────────────\n"
        )

        for network, amount in file_totals.items():

            report += (
                f"🔹 {network}: "
                f"`{amount:.6f}`\n"
            )

        report += "──────────────────\n"

        # اگر حداقل یک شبکه موجودی داشته باشد
        has_balance = any(
            amount > 0
            for amount in file_totals.values()
        )

        if has_balance:
            report += "💰 *موجودی پیدا شد*"
        else:
            report += "⚪ *موجودی Native پیدا نشد*"

        # -------------------------------------------------
        # ارسال گزارش
        # -------------------------------------------------

        await context.bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=report,
            parse_mode="Markdown"
        )

        logger.info(
            "Report completed: %s",
            file_name
        )

    except Exception as e:

        logger.exception(
            "Error processing report"
        )

        try:

            await context.bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "❌ خطا هنگام پردازش Report\n\n"
                    f"📄 فایل: `{file_name}`\n"
                    f"❗ خطا: `{str(e)[:1000]}`"
                ),
                parse_mode="Markdown"
            )

        except Exception:
            pass


# =========================================================
# TELEGRAM HANDLER
# =========================================================

tg_app.add_handler(
    MessageHandler(
        filters.ChatType.CHANNEL
        & filters.Document.ALL,
        process_report
    )
)


# =========================================================
# WEBHOOK
# =========================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
async def webhook():

    try:

        data = request.get_json(
            force=True,
            silent=True
        )

        if not data:
            return "OK", 200

        update = Update.de_json(
            data,
            tg_app.bot
        )

        await tg_app.process_update(
            update
        )

        return "OK", 200

    except Exception as e:

        logger.exception(
            "Webhook error"
        )

        return "ERROR", 500


# =========================================================
# HEALTH CHECK
# =========================================================

@app.route("/")
def health_check():

    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Wallet Scanner</title>
    </head>

    <body style="
        background:#1a1a1a;
        color:#00ff00;
        font-family:monospace;
        text-align:center;
        padding-top:50px;
    ">

        <h1>📡 Wallet Scanner</h1>

        <p>
            Status:
            <span style="color:#00ff00;">
                ACTIVE
            </span>
        </p>

        <hr style="
            width:50%;
            border:1px solid #333;
        ">

        <p style="color:white;">
            Monitoring Telegram Channel for new reports...
        </p>

    </body>
    </html>
    """


# =========================================================
# STARTUP
# =========================================================

async def setup_webhook():

    webhook_url = (
        f"{RENDER_URL}/webhook"
    )

    logger.info(
        "Setting Telegram webhook: %s",
        webhook_url
    )

    await tg_app.bot.delete_webhook(
        drop_pending_updates=False
    )

    await tg_app.bot.set_webhook(
        url=webhook_url,
        allowed_updates=[
            "channel_post"
        ]
    )

    logger.info(
        "Telegram webhook successfully configured."
    )


def main():

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    # تنظیم webhook قبل از اجرای Flask
    asyncio.run(
        setup_webhook()
    )

    logger.info(
        "Starting Flask on port %s",
        port
    )

    # برای Render
    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )


if __name__ == "__main__":
    main()
```
