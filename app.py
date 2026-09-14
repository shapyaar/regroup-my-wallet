import os
import re
import io
import time
import queue
import asyncio
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from flask import Flask, request
from telegram import Bot, Update
from telegram.request import HTTPXRequest
from web3 import Web3


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

SOURCE_CHANNEL_RAW = os.getenv("SOURCE_CHANNEL", "0")
REPORT_CHANNEL_RAW = os.getenv("REPORT_CHANNEL", "0")

HOURLY_INTERVAL_MIN = int(
    os.getenv("HOURLY_INTERVAL_MIN", "60")
)

EXPORT_INTERVAL_MIN = int(
    os.getenv("EXPORT_INTERVAL_MIN", "120")
)

BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "300")
)

BALANCE_WORKERS = int(
    os.getenv("BALANCE_WORKERS", "10")
)

QUEUE_LIMIT = int(
    os.getenv("QUEUE_LIMIT", "100")
)


# ============================================================
# Helpers
# ============================================================

def parse_chat_id(value):
    """
    Supports normal integer Telegram chat IDs.
    """
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


SOURCE_CHANNEL = parse_chat_id(SOURCE_CHANNEL_RAW)
REPORT_CHANNEL = parse_chat_id(REPORT_CHANNEL_RAW)


NETWORKS = {
    "ETH": os.getenv(
        "ETH_RPC",
        "https://eth.llamarpc.com"
    ),
    "BSC": os.getenv(
        "BSC_RPC",
        "https://bsc-dataseed.binance.org/"
    ),
}


# ============================================================
# Flask / Queue
# ============================================================

app = Flask(__name__)

# Scheduled jobs use a bounded queue.
# Telegram source files use a separate unbounded queue so they are
# never dropped just because an hourly/export job is running.
task_queue = queue.Queue(
    maxsize=QUEUE_LIMIT
)
file_queue = queue.Queue()

_workers_started = False
_workers_lock = threading.Lock()


# ============================================================
# Configuration Validation
# ============================================================

def validate_configuration():

    logger.info("Validating configuration")

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing"
        )

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing"
        )

    if not SOURCE_CHANNEL:
        raise RuntimeError(
            "SOURCE_CHANNEL is missing or invalid"
        )

    if not REPORT_CHANNEL:
        raise RuntimeError(
            "REPORT_CHANNEL is missing or invalid"
        )

    if HOURLY_INTERVAL_MIN <= 0:
        raise RuntimeError(
            "HOURLY_INTERVAL_MIN must be greater than 0"
        )

    if EXPORT_INTERVAL_MIN <= 0:
        raise RuntimeError(
            "EXPORT_INTERVAL_MIN must be greater than 0"
        )

    if BATCH_SIZE <= 0:
        raise RuntimeError(
            "BATCH_SIZE must be greater than 0"
        )

    logger.info(
        "Configuration validated"
    )


# ============================================================
# PostgreSQL
# ============================================================

def get_conn():

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing"
        )

    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        connect_timeout=15
    )


def init_db():
    """
    Creates/migrates the database safely.

    Important:
    Existing wallet data is preserved.

    This specifically fixes the situation where an old
    `wallets` table exists without an `id` column.
    """

    logger.info(
        "Initializing PostgreSQL"
    )

    conn = None
    cur = None

    try:

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        # ----------------------------------------------------
        # Check wallets table
        # ----------------------------------------------------

        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'wallets'
            )
        """)

        wallets_exists = cur.fetchone()[0]

        # ----------------------------------------------------
        # Create wallets if missing
        # ----------------------------------------------------

        if not wallets_exists:

            logger.info(
                "wallets table does not exist - creating"
            )

            cur.execute("""
                CREATE TABLE wallets (
                    id BIGSERIAL PRIMARY KEY,
                    address TEXT NOT NULL UNIQUE,
                    source_uploaded_at TIMESTAMPTZ,
                    added_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

        else:

            logger.info(
                "wallets table exists - checking schema"
            )

            # ------------------------------------------------
            # Get existing columns
            # ------------------------------------------------

            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'wallets'
            """)

            existing_columns = {
                row[0]
                for row in cur.fetchall()
            }

            logger.info(
                "Existing wallets columns: %s",
                sorted(existing_columns)
            )

            # ------------------------------------------------
            # Address is mandatory
            # ------------------------------------------------

            if "address" not in existing_columns:

                raise RuntimeError(
                    "The existing wallets table does not "
                    "contain an 'address' column. "
                    "Automatic migration was stopped to "
                    "avoid destroying existing data."
                )

            # ------------------------------------------------
            # Add ID if missing
            # ------------------------------------------------

            if "id" not in existing_columns:

                logger.warning(
                    "wallets.id is missing - starting migration"
                )

                # Create sequence
                cur.execute("""
                    CREATE SEQUENCE IF NOT EXISTS
                    wallets_id_seq
                    AS BIGINT
                    START WITH 1
                """)

                # Add column
                cur.execute("""
                    ALTER TABLE wallets
                    ADD COLUMN id BIGINT
                """)

                # Fill old rows
                cur.execute("""
                    UPDATE wallets
                    SET id = nextval('wallets_id_seq')
                    WHERE id IS NULL
                """)

                # Set default
                cur.execute("""
                    ALTER TABLE wallets
                    ALTER COLUMN id
                    SET DEFAULT nextval('wallets_id_seq')
                """)

                # Find max id
                cur.execute("""
                    SELECT COALESCE(MAX(id), 0)
                    FROM wallets
                """)

                max_id = int(
                    cur.fetchone()[0]
                )

                # Correct sequence position
                if max_id > 0:

                    cur.execute(
                        """
                        SELECT setval(
                            'wallets_id_seq',
                            %s,
                            true
                        )
                        """,
                        (max_id,)
                    )

                else:

                    cur.execute(
                        """
                        SELECT setval(
                            'wallets_id_seq',
                            1,
                            false
                        )
                        """
                    )

                # NOT NULL
                cur.execute("""
                    ALTER TABLE wallets
                    ALTER COLUMN id
                    SET NOT NULL
                """)

                # Check primary key
                cur.execute("""
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'public'
                      AND table_name = 'wallets'
                      AND constraint_type = 'PRIMARY KEY'
                """)

                primary_key = cur.fetchone()

                if not primary_key:

                    cur.execute("""
                        ALTER TABLE wallets
                        ADD CONSTRAINT wallets_pkey
                        PRIMARY KEY (id)
                    """)

                logger.info(
                    "wallets.id migration completed"
                )

            # ------------------------------------------------
            # Add source_uploaded_at
            # ------------------------------------------------

            if "source_uploaded_at" not in existing_columns:

                logger.info(
                    "Adding wallets.source_uploaded_at"
                )

                cur.execute("""
                    ALTER TABLE wallets
                    ADD COLUMN source_uploaded_at TIMESTAMPTZ
                """)

            # ------------------------------------------------
            # Add added_at
            # ------------------------------------------------

            if "added_at" not in existing_columns:

                logger.info(
                    "Adding wallets.added_at"
                )

                cur.execute("""
                    ALTER TABLE wallets
                    ADD COLUMN added_at TIMESTAMPTZ
                    DEFAULT NOW()
                """)

                cur.execute("""
                    UPDATE wallets
                    SET added_at = NOW()
                    WHERE added_at IS NULL
                """)

        # ----------------------------------------------------
        # Normalize old addresses
        # ----------------------------------------------------

        cur.execute("""
            UPDATE wallets
            SET address = LOWER(TRIM(address))
            WHERE address IS NOT NULL
        """)

        # ----------------------------------------------------
        # Remove duplicate addresses case-insensitively
        # Keep the oldest ID.
        # ----------------------------------------------------

        cur.execute("""
            DELETE FROM wallets a
            USING wallets b
            WHERE a.id > b.id
              AND LOWER(a.address) = LOWER(b.address)
        """)

        deleted_duplicates = cur.rowcount

        if deleted_duplicates:
            logger.warning(
                "Removed %s duplicate wallet rows",
                deleted_duplicates
            )

        # ----------------------------------------------------
        # Create unique index
        # ----------------------------------------------------

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            wallets_address_unique_idx
            ON wallets (LOWER(address))
        """)

        # ----------------------------------------------------
        # Meta table
        # ----------------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()

        logger.info(
            "PostgreSQL initialized successfully"
        )

    except Exception:

        if conn:
            conn.rollback()

        logger.exception(
            "PostgreSQL initialization failed"
        )

        raise

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()


# ============================================================
# Wallet Database Functions
# ============================================================

def save_wallet(address, source_uploaded_at):

    address = address.strip().lower()

    conn = get_conn()

    try:

        cur = conn.cursor()

        cur.execute("""
            INSERT INTO wallets (
                address,
                source_uploaded_at
            )
            VALUES (
                %s,
                %s
            )
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (
            address,
            source_uploaded_at
        ))

        row = cur.fetchone()

        conn.commit()

        return row is not None

    except Exception:

        conn.rollback()

        raise

    finally:

        conn.close()


def get_wallet_count():

    conn = get_conn()

    try:

        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*)
            FROM wallets
        """)

        return int(
            cur.fetchone()[0]
        )

    finally:

        conn.close()


def get_all_wallets():

    conn = get_conn()

    try:

        cur = conn.cursor()

        cur.execute("""
            SELECT
                id,
                address,
                source_uploaded_at
            FROM wallets
            ORDER BY id ASC
        """)

        return cur.fetchall()

    finally:

        conn.close()


# ============================================================
# Meta
# ============================================================

def get_meta(key, default=None):

    conn = get_conn()

    try:

        cur = conn.cursor()

        cur.execute(
            """
            SELECT value
            FROM meta
            WHERE key = %s
            """,
            (key,)
        )

        row = cur.fetchone()

        if row:
            return row[0]

        return default

    finally:

        conn.close()


def set_meta(key, value):

    conn = get_conn()

    try:

        cur = conn.cursor()

        cur.execute("""
            INSERT INTO meta (
                key,
                value
            )
            VALUES (
                %s,
                %s
            )
            ON CONFLICT (key)
            DO UPDATE SET
                value = EXCLUDED.value
        """, (
            key,
            str(value)
        ))

        conn.commit()

    except Exception:

        conn.rollback()

        raise

    finally:

        conn.close()


# ============================================================
# Telegram
# ============================================================

def create_bot(pool_size=20):

    return Bot(
        token=BOT_TOKEN,
        request=HTTPXRequest(
            connection_pool_size=pool_size,
            pool_timeout=60,
            connect_timeout=30,
            read_timeout=60,
            write_timeout=60
        )
    )


# ============================================================
# Input File
# ============================================================

ADDRESS_PATTERN = re.compile(
    r"Addr:\s*(0x[a-fA-F0-9]{40})",
    re.IGNORECASE
)


async def handle_file(
    file_id,
    file_name,
    uploaded_at
):

    bot = create_bot()

    try:

        logger.info(
            "Downloading source file: %s",
            file_name
        )

        telegram_file = await bot.get_file(
            file_id
        )

        content = await telegram_file.download_as_bytearray()

        text = content.decode(
            "utf-8",
            errors="ignore"
        )

        addresses = []

        # Preserve file order
        for line in text.splitlines():

            match = ADDRESS_PATTERN.search(
                line
            )

            if match:

                address = match.group(1)

                addresses.append(
                    address
                )

        if not addresses:

            logger.warning(
                "No addresses found in %s",
                file_name
            )

            try:

                await bot.send_message(
                    chat_id=REPORT_CHANNEL,
                    text=(
                        "⚠️ <b>فایل پردازش شد اما آدرسی پیدا نشد</b>\n\n"
                        f"📄 فایل: <code>{file_name}</code>"
                    ),
                    parse_mode="HTML"
                )

            except Exception:

                logger.exception(
                    "Failed to send empty-file report"
                )

            return

        # ----------------------------------------------------
        # Remove duplicates inside the same file
        # while preserving order.
        # ----------------------------------------------------

        unique_addresses = []
        seen = set()

        for address in addresses:

            normalized = address.lower()

            if normalized not in seen:

                seen.add(normalized)

                unique_addresses.append(
                    address
                )

        saved = 0

        for address in unique_addresses:

            try:

                if save_wallet(
                    address,
                    uploaded_at
                ):
                    saved += 1

            except Exception:

                logger.exception(
                    "Failed saving wallet: %s",
                    address
                )

        total = get_wallet_count()

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "📥 <b>فایل جدید دریافت شد</b>\n\n"
                f"📄 فایل: <code>{file_name}</code>\n"
                f"🕐 زمان انتشار: <code>{uploaded_at}</code>\n"
                f"🔢 آدرس‌های داخل فایل: <code>{len(addresses)}</code>\n"
                f"🔄 آدرس‌های یکتا: <code>{len(unique_addresses)}</code>\n"
                f"➕ آدرس جدید: <code>{saved}</code>\n"
                f"📦 کل دیتابیس: <code>{total}</code>"
            ),
            parse_mode="HTML"
        )

        logger.info(
            "File processed: %s | addresses=%s | unique=%s | new=%s",
            file_name,
            len(addresses),
            len(unique_addresses),
            saved
        )

    except Exception:

        logger.exception(
            "File processing failed: %s",
            file_name
        )

        try:

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "❌ <b>خطا در پردازش فایل</b>\n\n"
                    f"📄 فایل: <code>{file_name}</code>"
                ),
                parse_mode="HTML"
            )

        except Exception:

            logger.exception(
                "Failed to send file error report"
            )

    finally:

        try:
            await bot.shutdown()
        except Exception:
            pass


# ============================================================
# Web3 Providers
# ============================================================

WEB3_PROVIDERS = {}


def initialize_web3():

    logger.info(
        "Initializing Web3 providers"
    )

    for network, rpc in NETWORKS.items():

        try:

            provider = Web3.HTTPProvider(
                rpc,
                request_kwargs={
                    "timeout": 15
                }
            )

            w3 = Web3(provider)

            WEB3_PROVIDERS[network] = w3

            logger.info(
                "Web3 provider initialized: %s",
                network
            )

        except Exception:

            logger.exception(
                "Failed to initialize Web3 provider: %s",
                network
            )


def get_wallet_balance(address):

    result = {
        "ETH": 0.0,
        "BSC": 0.0
    }

    try:

        checksum = Web3.to_checksum_address(
            address
        )

    except Exception:

        logger.warning(
            "Invalid wallet address: %s",
            address
        )

        return result

    for network, w3 in WEB3_PROVIDERS.items():

        try:

            if not w3.is_connected():

                logger.warning(
                    "%s RPC not connected",
                    network
                )

                continue

            balance = w3.eth.get_balance(
                checksum
            )

            result[network] = float(
                w3.from_wei(
                    balance,
                    "ether"
                )
            )

        except Exception as e:

            logger.warning(
                "Balance error | network=%s | address=%s | error=%s",
                network,
                address,
                e
            )

    return result


# ============================================================
# Hourly Report
# ============================================================

async def handle_hourly():

    bot = create_bot(
        pool_size=30
    )

    try:

        wallets = get_all_wallets()

        total_wallets = len(wallets)

        logger.info(
            "Hourly scan started | total=%s",
            total_wallets
        )

        if not wallets:

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "ℹ️ <b>گزارش ساعتی</b>\n\n"
                    "📦 دیتابیس در حال حاضر خالی است."
                ),
                parse_mode="HTML"
            )

            set_meta(
                "last_hourly",
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            return

        total_parts = (
            total_wallets + BATCH_SIZE - 1
        ) // BATCH_SIZE

        total_eth = 0.0
        total_bsc = 0.0
        rich_count = 0

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "⏰ <b>شروع گزارش ساعتی</b>\n\n"
                f"📦 تعداد کل: <code>{total_wallets}</code>\n"
                f"🔢 تعداد بخش‌ها: <code>{total_parts}</code>"
            ),
            parse_mode="HTML"
        )

        loop = asyncio.get_running_loop()

        # One executor for the whole scan
        # instead of creating a new executor per batch.
        with ThreadPoolExecutor(
            max_workers=BALANCE_WORKERS
        ) as executor:

            for start in range(
                0,
                total_wallets,
                BATCH_SIZE
            ):

                batch = wallets[
                    start:start + BATCH_SIZE
                ]

                part_number = (
                    start // BATCH_SIZE
                ) + 1

                logger.info(
                    "Scanning batch %s/%s",
                    part_number,
                    total_parts
                )

                futures = [
                    loop.run_in_executor(
                        executor,
                        get_wallet_balance,
                        row[1]
                    )
                    for row in batch
                ]

                results = await asyncio.gather(
                    *futures,
                    return_exceptions=True
                )

                batch_eth = 0.0
                batch_bsc = 0.0
                batch_rich = 0

                for row, balance in zip(
                    batch,
                    results
                ):

                    wallet_id = row[0]
                    address = row[1]
                    uploaded_at = row[2]

                    # If one RPC check crashed, continue
                    # with the rest of the batch.
                    if isinstance(
                        balance,
                        Exception
                    ):

                        logger.warning(
                            "Wallet balance task failed | id=%s | address=%s | error=%s",
                            wallet_id,
                            address,
                            balance
                        )

                        continue

                    eth = float(
                        balance.get(
                            "ETH",
                            0.0
                        )
                    )

                    bsc = float(
                        balance.get(
                            "BSC",
                            0.0
                        )
                    )

                    batch_eth += eth
                    batch_bsc += bsc

                    total_eth += eth
                    total_bsc += bsc

                    if eth > 0 or bsc > 0:

                        batch_rich += 1
                        rich_count += 1

                        if uploaded_at:

                            if hasattr(
                                uploaded_at,
                                "isoformat"
                            ):
                                uploaded_text = (
                                    uploaded_at.isoformat()
                                )
                            else:
                                uploaded_text = str(
                                    uploaded_at
                                )

                        else:

                            uploaded_text = (
                                "نامشخص"
                            )

                        try:

                            await bot.send_message(
                                chat_id=REPORT_CHANNEL,
                                text=(
                                    "💰 <b>آدرس دارای موجودی</b>\n\n"
                                    f"🔢 شناسه: <code>{wallet_id}</code>\n"
                                    f"📍 آدرس:\n"
                                    f"<code>{address}</code>\n\n"
                                    f"🕐 زمان انتشار فایل:\n"
                                    f"<code>{uploaded_text}</code>\n\n"
                                    f"🔹 ETH: <code>{eth:.8f}</code>\n"
                                    f"🔹 BSC: <code>{bsc:.8f}</code>"
                                ),
                                parse_mode="HTML"
                            )

                            await asyncio.sleep(
                                0.5
                            )

                        except Exception:

                            logger.exception(
                                "Failed to send rich-wallet report | address=%s",
                                address
                            )

                try:

                    await bot.send_message(
                        chat_id=REPORT_CHANNEL,
                        text=(
                            f"📊 <b>بخش {part_number}/{total_parts}</b>\n\n"
                            f"🔢 تعداد آدرس: <code>{len(batch)}</code>\n"
                            f"🔹 ETH: <code>{batch_eth:.8f}</code>\n"
                            f"🔹 BSC: <code>{batch_bsc:.8f}</code>\n"
                            f"💰 دارای موجودی: <code>{batch_rich}</code>"
                        ),
                        parse_mode="HTML"
                    )

                except Exception:

                    logger.exception(
                        "Failed to send batch report"
                    )

        # ----------------------------------------------------
        # Final report
        # ----------------------------------------------------

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "✅ <b>گزارش ساعتی تمام شد</b>\n\n"
                f"📦 کل آدرس‌ها: <code>{total_wallets}</code>\n"
                f"🔹 مجموع ETH: <code>{total_eth:.8f}</code>\n"
                f"🔹 مجموع BSC: <code>{total_bsc:.8f}</code>\n"
                f"💰 تعداد دارای موجودی: <code>{rich_count}</code>"
            ),
            parse_mode="HTML"
        )

        set_meta(
            "last_hourly",
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        logger.info(
            "Hourly scan completed"
        )

    except Exception:

        logger.exception(
            "Hourly scan failed"
        )

        try:

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "❌ <b>گزارش ساعتی با خطا متوقف شد</b>"
                ),
                parse_mode="HTML"
            )

        except Exception:

            logger.exception(
                "Failed to send hourly error"
            )

    finally:

        try:
            await bot.shutdown()
        except Exception:
            pass


# ============================================================
# Export
# ============================================================

async def handle_export():

    bot = create_bot()

    try:

        wallets = get_all_wallets()

        if not wallets:

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "ℹ️ <b>خروجی دیتابیس</b>\n\n"
                    "دیتابیس خالی است."
                ),
                parse_mode="HTML"
            )

            set_meta(
                "last_export",
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            return

        output = io.StringIO()

        output.write(
            "Wallet Address Export\n"
        )

        output.write(
            "========================================\n"
        )

        output.write(
            "Export time: "
            f"{datetime.now(timezone.utc).isoformat()}\n"
        )

        output.write(
            f"Total: {len(wallets)}\n"
        )

        output.write(
            "========================================\n\n"
        )

        for (
            wallet_id,
            address,
            uploaded_at
        ) in wallets:

            output.write(
                f"{wallet_id} | "
                f"{address} | "
                f"{uploaded_at}\n"
            )

        data = io.BytesIO(
            output.getvalue().encode(
                "utf-8"
            )
        )

        data.name = (
            "wallets_export.txt"
        )

        await bot.send_document(
            chat_id=REPORT_CHANNEL,
            document=data,
            caption=(
                "📦 <b>خروجی دیتابیس</b>\n"
                f"🔢 تعداد: <code>{len(wallets)}</code>"
            ),
            parse_mode="HTML"
        )

        set_meta(
            "last_export",
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        logger.info(
            "Export sent | total=%s",
            len(wallets)
        )

    except Exception:

        logger.exception(
            "Export failed"
        )

        try:

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "❌ <b>خطا در ساخت خروجی دیتابیس</b>"
                ),
                parse_mode="HTML"
            )

        except Exception:

            logger.exception(
                "Failed to send export error"
            )

    finally:

        try:
            await bot.shutdown()
        except Exception:
            pass


# ============================================================
# Queue
# ============================================================

def enqueue_task(task):

    # Used only for hourly/export jobs.
    try:
        task_queue.put_nowait(task)

        logger.info(
            "Task queued: %s | queue=%s",
            task.get("type"),
            task_queue.qsize()
        )

        return True

    except queue.Full:

        logger.error(
            "Scheduled task queue is full - dropping task: %s",
            task.get("type")
        )

        return False


def enqueue_file_task(task):

    # File queue is intentionally unbounded. Incoming Telegram files
    # must not be lost while hourly/export processing is running.
    file_queue.put(task)

    logger.info(
        "Source file queued: %s | file_queue=%s",
        task.get("file_name"),
        file_queue.qsize()
    )

    return True


# ============================================================
# Workers
# ============================================================

def run_async(coro):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(coro)
    finally:
        try:
            loop.run_until_complete(asyncio.sleep(0))
        except Exception:
            pass
        loop.close()


def worker_loop():

    # Dedicated worker for hourly/export jobs.
    logger.info(
        "Scheduled background worker started"
    )

    while True:

        task = task_queue.get()

        try:

            task_type = task.get("type")

            logger.info(
                "Processing scheduled task: %s",
                task_type
            )

            if task_type == "hourly":
                run_async(handle_hourly())

            elif task_type == "export":
                run_async(handle_export())

            else:
                logger.warning(
                    "Unknown scheduled task type: %s",
                    task_type
                )

        except Exception:

            logger.exception(
                "Scheduled worker task failed"
            )

        finally:
            task_queue.task_done()


def file_worker_loop():

    # Dedicated worker for Telegram source files.
    logger.info(
        "Source-file background worker started"
    )

    while True:

        task = file_queue.get()

        try:

            logger.info(
                "Processing source file: %s | remaining=%s",
                task.get("file_name"),
                file_queue.qsize()
            )

            run_async(
                handle_file(
                    task["file_id"],
                    task["file_name"],
                    task["uploaded_at"]
                )
            )

        except Exception:

            logger.exception(
                "Source file worker task failed"
            )

        finally:
            file_queue.task_done()


# ============================================================
# Scheduler Helpers
# ============================================================

def is_due(
    meta_key,
    interval_minutes,
    now
):

    last_value = get_meta(
        meta_key
    )

    if not last_value:
        return True

    try:

        previous = datetime.fromisoformat(
            last_value
        )

        if previous.tzinfo is None:

            previous = previous.replace(
                tzinfo=timezone.utc
            )

        elapsed = (
            now - previous
        ).total_seconds()

        return elapsed >= (
            interval_minutes * 60
        )

    except Exception:

        logger.warning(
            "Invalid %s value: %s",
            meta_key,
            last_value
        )

        return True


def scheduler_loop():

    logger.info(
        "Scheduler started"
    )

    while True:

        try:

            now = datetime.now(
                timezone.utc
            )

            # =================================================
            # Hourly
            # =================================================

            if is_due(
                "last_hourly",
                HOURLY_INTERVAL_MIN,
                now
            ):

                # Do not queue duplicate hourly jobs
                # if one is already waiting.
                if task_queue.qsize() < 3:

                    if enqueue_task({
                        "type": "hourly"
                    }):

                        # Mark when queued, not when finished.
                        set_meta(
                            "last_hourly",
                            now.isoformat()
                        )

                        logger.info(
                            "Hourly job queued"
                        )

            # =================================================
            # Export
            # =================================================

            if is_due(
                "last_export",
                EXPORT_INTERVAL_MIN,
                now
            ):

                if task_queue.qsize() < 3:

                    if enqueue_task({
                        "type": "export"
                    }):

                        set_meta(
                            "last_export",
                            now.isoformat()
                        )

                        logger.info(
                            "Export job queued"
                        )

        except Exception:

            logger.exception(
                "Scheduler error"
            )

        time.sleep(30)


# ============================================================
# Start Background Workers
# ============================================================

def start_background_workers():

    global _workers_started

    with _workers_lock:

        if _workers_started:

            logger.info(
                "Background workers already started"
            )

            return

        logger.info(
            "Starting background workers | pid=%s | scheduled_queue_limit=%s",
            os.getpid(),
            QUEUE_LIMIT
        )

        worker_thread = threading.Thread(
            target=worker_loop,
            name="wallet-scheduled-worker",
            daemon=True
        )

        file_worker_thread = threading.Thread(
            target=file_worker_loop,
            name="wallet-file-worker",
            daemon=True
        )

        scheduler_thread = threading.Thread(
            target=scheduler_loop,
            name="wallet-scheduler",
            daemon=True
        )

        worker_thread.start()
        file_worker_thread.start()
        scheduler_thread.start()

        _workers_started = True

        logger.info(
            "Background workers started successfully"
        )


# ============================================================
# Application Initialization
# ============================================================

_application_initialized = False
_application_lock = threading.Lock()


def initialize_application():

    global _application_initialized

    with _application_lock:

        if _application_initialized:

            return

        logger.info(
            "Initializing application"
        )

        validate_configuration()

        init_db()

        initialize_web3()

        start_background_workers()

        _application_initialized = True

        logger.info(
            "Application initialization completed"
        )


# ============================================================
# Webhook
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    try:

        data = request.get_json(
            force=True,
            silent=True
        )

        if not data:

            logger.warning(
                "Webhook received empty/invalid JSON"
            )

            return "OK", 200

        update = Update.de_json(
            data,
            bot=None
        )

        if not update:

            return "OK", 200

        # ----------------------------------------------------
        # Only channel posts
        # ----------------------------------------------------

        if not update.channel_post:

            return "OK", 200

        post = update.channel_post

        # ----------------------------------------------------
        # Only source channel
        # ----------------------------------------------------

        if post.chat.id != SOURCE_CHANNEL:

            return "OK", 200

        # ----------------------------------------------------
        # Only documents/files
        # ----------------------------------------------------

        if not post.document:

            return "OK", 200

        document = post.document

        uploaded_at = (
            post.date
            if post.date
            else datetime.now(
                timezone.utc
            )
        )

        file_name = (
            document.file_name
            or "unknown.txt"
        )

        logger.info(
            "New source file: %s | file_id=%s | update_id=%s",
            file_name,
            document.file_id,
            getattr(
                update,
                "update_id",
                None
            )
        )

        queued = enqueue_file_task({
            "type": "file",
            "file_id": document.file_id,
            "file_name": file_name,
            "uploaded_at": uploaded_at
        })

        if not queued:

            logger.error(
                "Could not queue source file: %s",
                file_name
            )

        return "OK", 200

    except Exception:

        logger.exception(
            "Webhook error"
        )

        # Always return 200 so Telegram does not
        # continuously retry the webhook.
        return "OK", 200


# ============================================================
# Health
# ============================================================

@app.route(
    "/",
    methods=["GET", "HEAD"]
)
def health():

    try:

        count = get_wallet_count()

        return (
            "OK | "
            f"Queue: {task_queue.qsize()} | "
            f"FileQueue: {file_queue.qsize()} | "
            f"DB: {count}"
        ), 200

    except Exception as e:

        logger.exception(
            "Health check failed"
        )

        return (
            f"DB ERROR: {e}"
        ), 500


@app.route(
    "/health",
    methods=["GET", "HEAD"]
)
def health_check():

    try:

        count = get_wallet_count()

        return {
            "status": "ok",
            "database": "ok",
            "wallets": count,
            "queue": task_queue.qsize(),
            "file_queue": file_queue.qsize()
        }, 200

    except Exception as e:

        logger.exception(
            "Health endpoint failed"
        )

        return {
            "status": "error",
            "database": "error",
            "error": str(e)
        }, 500


# ============================================================
# Optional manual endpoints
# ============================================================

@app.route(
    "/status",
    methods=["GET", "HEAD"]
)
def status():

    try:

        return {
            "status": "running",
            "queue": task_queue.qsize(),
            "file_queue": file_queue.qsize(),
            "wallets": get_wallet_count(),
            "hourly_interval_minutes": HOURLY_INTERVAL_MIN,
            "export_interval_minutes": EXPORT_INTERVAL_MIN,
            "batch_size": BATCH_SIZE,
            "balance_workers": BALANCE_WORKERS
        }, 200

    except Exception as e:

        return {
            "status": "error",
            "error": str(e)
        }, 500


# ============================================================
# Initialize when Gunicorn imports app.py
# ============================================================

try:

    initialize_application()

except Exception:

    logger.exception(
        "Application initialization failed"
    )

    # Re-raise so Gunicorn correctly marks
    # the deployment as failed.
    raise


# ============================================================
# Local development
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
