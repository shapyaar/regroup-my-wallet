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
from psycopg2.pool import ThreadedConnectionPool

from flask import Flask, request

from telegram import Bot, Update
from telegram.request import HTTPXRequest

from web3 import Web3


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

SOURCE_CHANNEL = int(os.getenv("SOURCE_CHANNEL", "0"))
REPORT_CHANNEL = int(os.getenv("REPORT_CHANNEL", "0"))

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

QUEUE_MAX_SIZE = int(
    os.getenv("QUEUE_MAX_SIZE", "100")
)


# ============================================================
# Network configuration
# ============================================================

NETWORKS = {
    "ETH": "https://eth.llamarpc.com",
    "BSC": "https://bsc-dataseed.binance.org/",
}


# ============================================================
# Flask
# ============================================================

app = Flask(__name__)

task_queue = queue.Queue(
    maxsize=QUEUE_MAX_SIZE
)


# ============================================================
# Global state
# ============================================================

db_pool = None

web3_providers = {}

background_started = False
background_lock = threading.Lock()

balance_executor = ThreadPoolExecutor(
    max_workers=BALANCE_WORKERS,
    thread_name_prefix="balance"
)


# ============================================================
# Validation
# ============================================================

def validate_config():
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
            "SOURCE_CHANNEL is missing"
        )

    if not REPORT_CHANNEL:
        raise RuntimeError(
            "REPORT_CHANNEL is missing"
        )

    logger.info("Configuration validated")


# ============================================================
# PostgreSQL
# ============================================================

def create_db_pool():
    global db_pool

    if db_pool is not None:
        return

    logger.info(
        "Creating PostgreSQL connection pool"
    )

    db_pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        dsn=DATABASE_URL,
        sslmode="require",
        connect_timeout=15,
    )

    logger.info(
        "PostgreSQL connection pool created"
    )


def get_db_connection():
    if db_pool is None:
        create_db_pool()

    return db_pool.getconn()


def release_db_connection(conn):
    if db_pool is not None and conn is not None:
        db_pool.putconn(conn)


def init_db():
    """
    Initialize and migrate PostgreSQL schema safely.

    Important:
    - Existing wallet data is preserved.
    - Old wallets table without `id` is migrated.
    - Missing columns are added automatically.
    - Existing meta table is preserved.
    """

    logging.info("Initializing PostgreSQL")

    conn = None
    cur = None

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        # ====================================================
        # Check whether wallets table exists
        # ====================================================

        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'wallets'
            )
        """)

        wallets_exists = cur.fetchone()[0]

        # ====================================================
        # Create wallets table if it does not exist
        # ====================================================

        if not wallets_exists:

            logging.info(
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

            logging.info(
                "wallets table already exists - checking schema"
            )

            # ------------------------------------------------
            # Check columns
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

            logging.info(
                "Existing wallets columns: %s",
                sorted(existing_columns)
            )

            # ------------------------------------------------
            # Add id column if missing
            # ------------------------------------------------

            if "id" not in existing_columns:

                logging.warning(
                    "wallets.id is missing - migrating table"
                )

                # Create sequence if necessary
                cur.execute("""
                    CREATE SEQUENCE IF NOT EXISTS wallets_id_seq
                    AS BIGINT
                    START WITH 1
                """)

                # Add id column
                cur.execute("""
                    ALTER TABLE wallets
                    ADD COLUMN id BIGINT
                """)

                # Fill existing rows
                cur.execute("""
                    UPDATE wallets
                    SET id = nextval('wallets_id_seq')
                    WHERE id IS NULL
                """)

                # Make future inserts automatic
                cur.execute("""
                    ALTER TABLE wallets
                    ALTER COLUMN id
                    SET DEFAULT nextval('wallets_id_seq')
                """)

                # Set sequence after existing IDs
                cur.execute("""
                    SELECT COALESCE(MAX(id), 0)
                    FROM wallets
                """)

                max_id = cur.fetchone()[0]

                cur.execute(
                    """
                    SELECT setval(
                        'wallets_id_seq',
                        %s,
                        %s
                    )
                    """,
                    (
                        max_id if max_id > 0 else 1,
                        max_id > 0
                    )
                )

                # Make id NOT NULL
                cur.execute("""
                    ALTER TABLE wallets
                    ALTER COLUMN id SET NOT NULL
                """)

                # Add primary key only if one does not already exist
                cur.execute("""
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = 'public'
                      AND table_name = 'wallets'
                      AND constraint_type = 'PRIMARY KEY'
                """)

                has_primary_key = cur.fetchone() is not None

                if not has_primary_key:

                    cur.execute("""
                        ALTER TABLE wallets
                        ADD CONSTRAINT wallets_pkey
                        PRIMARY KEY (id)
                    """)

                logging.info(
                    "wallets.id migration completed"
                )

            # ------------------------------------------------
            # Add address if missing
            # ------------------------------------------------

            if "address" not in existing_columns:

                raise RuntimeError(
                    "Existing wallets table does not contain "
                    "an 'address' column. Automatic migration "
                    "cannot safely determine the wallet address column."
                )

            # ------------------------------------------------
            # Add source_uploaded_at if missing
            # ------------------------------------------------

            if "source_uploaded_at" not in existing_columns:

                logging.info(
                    "Adding wallets.source_uploaded_at"
                )

                cur.execute("""
                    ALTER TABLE wallets
                    ADD COLUMN source_uploaded_at TIMESTAMPTZ
                """)

            # ------------------------------------------------
            # Add added_at if missing
            # ------------------------------------------------

            if "added_at" not in existing_columns:

                logging.info(
                    "Adding wallets.added_at"
                )

                cur.execute("""
                    ALTER TABLE wallets
                    ADD COLUMN added_at TIMESTAMPTZ DEFAULT NOW()
                """)

                cur.execute("""
                    UPDATE wallets
                    SET added_at = NOW()
                    WHERE added_at IS NULL
                """)

        # ====================================================
        # Ensure address is unique
        # ====================================================

        cur.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'public'
              AND table_name = 'wallets'
              AND constraint_type = 'UNIQUE'
        """)

        unique_constraints = {
            row[0]
            for row in cur.fetchall()
        }

        # Check for an existing unique index on address
        cur.execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'wallets'
        """)

        indexes = {
            row[0]
            for row in cur.fetchall()
        }

        address_unique_exists = any(
            "address" in index_name.lower()
            and "uniq" in index_name.lower()
            for index_name in indexes
        )

        if not address_unique_exists:

            try:

                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS
                    wallets_address_unique_idx
                    ON wallets (LOWER(address))
                """)

                logging.info(
                    "Wallet address unique index ensured"
                )

            except psycopg2.errors.UniqueViolation:

                conn.rollback()

                logging.warning(
                    "Duplicate wallet addresses detected. "
                    "Cleaning duplicates before creating index."
                )

                cur = conn.cursor()

                cur.execute("""
                    DELETE FROM wallets a
                    USING wallets b
                    WHERE a.id > b.id
                      AND LOWER(a.address) = LOWER(b.address)
                """)

                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS
                    wallets_address_unique_idx
                    ON wallets (LOWER(address))
                """)

        # ====================================================
        # Meta table
        # ====================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # ====================================================
        # Commit
        # ====================================================

        conn.commit()

        logging.info(
            "PostgreSQL initialized successfully"
        )

    except Exception:

        if conn:
            conn.rollback()

        logging.exception(
            "PostgreSQL initialization failed"
        )

        raise

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

def save_wallet(address, source_uploaded_at):
    conn = None

    try:
        conn = get_db_connection()

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
            ON CONFLICT (address)
            DO NOTHING
            RETURNING id
        """, (
            address.lower(),
            source_uploaded_at,
        ))

        row = cur.fetchone()

        conn.commit()

        cur.close()

        # True یعنی wallet جدید بوده
        return row is not None

    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        raise

    finally:
        release_db_connection(conn)


def get_all_wallets():
    conn = None

    try:
        conn = get_db_connection()

        cur = conn.cursor()

        cur.execute("""
            SELECT
                id,
                address,
                source_uploaded_at
            FROM wallets
            ORDER BY id ASC
        """)

        rows = cur.fetchall()

        cur.close()

        return rows

    finally:
        release_db_connection(conn)


def get_wallet_count():
    conn = None

    try:
        conn = get_db_connection()

        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*)
            FROM wallets
        """)

        count = cur.fetchone()[0]

        cur.close()

        return int(count)

    finally:
        release_db_connection(conn)


def get_meta(key, default=None):
    conn = None

    try:
        conn = get_db_connection()

        cur = conn.cursor()

        cur.execute(
            """
            SELECT value
            FROM meta
            WHERE key = %s
            """,
            (key,),
        )

        row = cur.fetchone()

        cur.close()

        if row:
            return row[0]

        return default

    finally:
        release_db_connection(conn)


def set_meta(key, value):
    conn = None

    try:
        conn = get_db_connection()

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
            str(value),
        ))

        conn.commit()

        cur.close()

    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        raise

    finally:
        release_db_connection(conn)


# ============================================================
# Telegram
# ============================================================

def create_bot(pool_size=20):
    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing"
        )

    return Bot(
        token=BOT_TOKEN,
        request=HTTPXRequest(
            connection_pool_size=pool_size,
            pool_timeout=60,
            connect_timeout=30,
            read_timeout=60,
            write_timeout=60,
        ),
    )


# ============================================================
# Input file parser
# ============================================================

ADDRESS_PATTERN = re.compile(
    r"Addr:\s*(0x[a-fA-F0-9]{40})",
    re.IGNORECASE,
)


def normalize_uploaded_at(value):
    if value is None:
        return datetime.now(timezone.utc)

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value


# ============================================================
# Duplicate file protection
# ============================================================

def file_already_processed(file_unique_id):
    if not file_unique_id:
        return False

    key = f"processed_file:{file_unique_id}"

    value = get_meta(key)

    return value == "1"


def mark_file_processed(file_unique_id):
    if not file_unique_id:
        return

    key = f"processed_file:{file_unique_id}"

    set_meta(
        key,
        "1"
    )


# ============================================================
# File processing
# ============================================================

async def handle_file(
    file_id,
    file_name,
    uploaded_at,
    file_unique_id=None,
):
    bot = create_bot(20)

    uploaded_at = normalize_uploaded_at(
        uploaded_at
    )

    try:
        logger.info(
            "Processing source file: %s | file_id=%s",
            file_name,
            file_id,
        )

        # ----------------------------------------
        # Duplicate protection
        # ----------------------------------------

        if file_unique_id:

            if file_already_processed(
                file_unique_id
            ):
                logger.info(
                    "File already processed: %s",
                    file_name,
                )
                return

        # ----------------------------------------
        # Download
        # ----------------------------------------

        logger.info(
            "Downloading source file: %s",
            file_name,
        )

        telegram_file = await bot.get_file(
            file_id
        )

        content = await telegram_file.download_as_bytearray()

        logger.info(
            "Downloaded %s | bytes=%s",
            file_name,
            len(content),
        )

        # ----------------------------------------
        # Decode
        # ----------------------------------------

        text = content.decode(
            "utf-8",
            errors="ignore",
        )

        # ----------------------------------------
        # Extract addresses
        # ----------------------------------------

        addresses = []

        seen_addresses = set()

        for line in text.splitlines():

            match = ADDRESS_PATTERN.search(
                line
            )

            if not match:
                continue

            address = match.group(1)

            address_lower = address.lower()

            # جلوگیری از duplicate داخل خود فایل
            if address_lower in seen_addresses:
                continue

            seen_addresses.add(
                address_lower
            )

            addresses.append(
                address
            )

        logger.info(
            "Parsed file: %s | addresses=%s",
            file_name,
            len(addresses),
        )

        if not addresses:

            await bot.send_message(
                chat_id=REPORT_CHANNEL,
                text=(
                    "⚠️ <b>فایل دریافت شد ولی آدرسی پیدا نشد</b>\n\n"
                    f"📄 فایل: <code>{file_name}</code>"
                ),
                parse_mode="HTML",
            )

            if file_unique_id:
                mark_file_processed(
                    file_unique_id
                )

            return

        # ----------------------------------------
        # Save wallets
        # ----------------------------------------

        saved = 0

        for address in addresses:

            try:

                is_new = save_wallet(
                    address,
                    uploaded_at,
                )

                if is_new:
                    saved += 1

            except Exception:
                logger.exception(
                    "Failed saving wallet: %s",
                    address,
                )

        # ----------------------------------------
        # Database count
        # ----------------------------------------

        total = get_wallet_count()

        # ----------------------------------------
        # Report
        # ----------------------------------------

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "📥 <b>فایل جدید دریافت شد</b>\n\n"
                f"📄 فایل: <code>{file_name}</code>\n"
                f"🕐 زمان انتشار: "
                f"<code>{uploaded_at.isoformat()}</code>\n"
                f"🔢 آدرس‌های یکتا: "
                f"<code>{len(addresses)}</code>\n"
                f"➕ آدرس جدید: "
                f"<code>{saved}</code>\n"
                f"📦 کل دیتابیس: "
                f"<code>{total}</code>"
            ),
            parse_mode="HTML",
        )

        # ----------------------------------------
        # Mark processed only after success
        # ----------------------------------------

        if file_unique_id:
            mark_file_processed(
                file_unique_id
            )

        logger.info(
            "File processed successfully: %s | addresses=%s | new=%s",
            file_name,
            len(addresses),
            saved,
        )

    except Exception:

        logger.exception(
            "File processing failed: %s",
            file_name,
        )


# ============================================================
# Web3
# ============================================================

def init_web3():
    global web3_providers

    logger.info(
        "Initializing Web3 providers"
    )

    for network, rpc in NETWORKS.items():

        try:

            provider = Web3.HTTPProvider(
                rpc,
                request_kwargs={
                    "timeout": 15
                },
            )

            w3 = Web3(provider)

            web3_providers[network] = w3

            logger.info(
                "Web3 provider initialized: %s",
                network,
            )

        except Exception:

            logger.exception(
                "Failed to initialize Web3: %s",
                network,
            )


def get_wallet_balance(address):
    result = {
        "ETH": 0.0,
        "BSC": 0.0,
    }

    try:

        checksum = Web3.to_checksum_address(
            address
        )

    except Exception:

        logger.warning(
            "Invalid wallet address: %s",
            address,
        )

        return result

    for network, w3 in web3_providers.items():

        try:

            if not w3.is_connected():

                logger.warning(
                    "%s RPC not connected",
                    network,
                )

                continue

            balance = w3.eth.get_balance(
                checksum
            )

            result[network] = float(
                w3.from_wei(
                    balance,
                    "ether",
                )
            )

        except Exception as e:

            logger.warning(
                "Balance error | network=%s | address=%s | error=%s",
                network,
                address,
                e,
            )

    return result


# ============================================================
# HTML escape helper
# ============================================================

def html_escape(value):
    if value is None:
        return ""

    value = str(value)

    return (
        value
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ============================================================
# Hourly report
# ============================================================

async def handle_hourly():
    bot = create_bot(30)

    wallets = get_all_wallets()

    total_wallets = len(wallets)

    logger.info(
        "Hourly scan started | total=%s",
        total_wallets,
    )

    if not wallets:

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                "⏰ <b>گزارش ساعتی</b>\n\n"
                "📭 دیتابیس خالی است."
            ),
            parse_mode="HTML",
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
            f"📦 تعداد کل: "
            f"<code>{total_wallets}</code>\n"
            f"🔢 تعداد بخش‌ها: "
            f"<code>{total_parts}</code>"
        ),
        parse_mode="HTML",
    )

    loop = asyncio.get_running_loop()

    for start in range(
        0,
        total_wallets,
        BATCH_SIZE,
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
            total_parts,
        )

        # ----------------------------------------
        # Parallel balance requests
        # ----------------------------------------

        futures = [
            loop.run_in_executor(
                balance_executor,
                get_wallet_balance,
                row[1],
            )
            for row in batch
        ]

        results = await asyncio.gather(
            *futures,
            return_exceptions=True,
        )

        batch_eth = 0.0
        batch_bsc = 0.0
        batch_rich = 0

        for row, balance in zip(
            batch,
            results,
        ):

            wallet_id = row[0]
            address = row[1]
            uploaded_at = row[2]

            if isinstance(
                balance,
                Exception,
            ):

                logger.error(
                    "Balance task failed | address=%s | error=%s",
                    address,
                    balance,
                )

                continue

            eth = balance.get(
                "ETH",
                0.0,
            )

            bsc = balance.get(
                "BSC",
                0.0,
            )

            batch_eth += eth
            batch_bsc += bsc

            total_eth += eth
            total_bsc += bsc

            if eth > 0 or bsc > 0:

                batch_rich += 1
                rich_count += 1

                if uploaded_at:

                    uploaded_text = (
                        uploaded_at.isoformat()
                    )

                else:

                    uploaded_text = "نامشخص"

                await bot.send_message(
                    chat_id=REPORT_CHANNEL,
                    text=(
                        "💰 <b>آدرس دارای موجودی</b>\n\n"
                        f"🔢 شناسه: "
                        f"<code>{wallet_id}</code>\n"
                        f"📍 آدرس:\n"
                        f"<code>{html_escape(address)}</code>\n\n"
                        f"🕐 زمان انتشار فایل:\n"
                        f"<code>{html_escape(uploaded_text)}</code>\n\n"
                        f"🔹 ETH: "
                        f"<code>{eth:.8f}</code>\n"
                        f"🔹 BSC: "
                        f"<code>{bsc:.8f}</code>"
                    ),
                    parse_mode="HTML",
                )

                # جلوگیری از فشار به Telegram
                await asyncio.sleep(
                    0.5
                )

        # ----------------------------------------
        # Batch report
        # ----------------------------------------

        await bot.send_message(
            chat_id=REPORT_CHANNEL,
            text=(
                f"📊 <b>بخش "
                f"{part_number}/{total_parts}</b>\n\n"
                f"🔢 تعداد آدرس: "
                f"<code>{len(batch)}</code>\n"
                f"🔹 ETH: "
                f"<code>{batch_eth:.8f}</code>\n"
                f"🔹 BSC: "
                f"<code>{batch_bsc:.8f}</code>\n"
                f"💰 دارای موجودی: "
                f"<code>{batch_rich}</code>"
            ),
            parse_mode="HTML",
        )

    # ----------------------------------------
    # Final report
    # ----------------------------------------

    await bot.send_message(
        chat_id=REPORT_CHANNEL,
        text=(
            "✅ <b>گزارش ساعتی تمام شد</b>\n\n"
            f"📦 کل آدرس‌ها: "
            f"<code>{total_wallets}</code>\n"
            f"🔹 مجموع ETH: "
            f"<code>{total_eth:.8f}</code>\n"
            f"🔹 مجموع BSC: "
            f"<code>{total_bsc:.8f}</code>\n"
            f"💰 تعداد دارای موجودی: "
            f"<code>{rich_count}</code>"
        ),
        parse_mode="HTML",
    )

    logger.info(
        "Hourly scan completed | total=%s | rich=%s",
        total_wallets,
        rich_count,
    )


# ============================================================
# Export
# ============================================================

async def handle_export():
    bot = create_bot(20)

    wallets = get_all_wallets()

    if not wallets:

        logger.info(
            "Export skipped: database empty"
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
        f"Export time: "
        f"{datetime.now(timezone.utc).isoformat()}\n"
    )

    output.write(
        f"Total: {len(wallets)}\n"
    )

    output.write(
        "========================================\n\n"
    )

    for wallet_id, address, uploaded_at in wallets:

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

    data.name = "wallets_export.txt"

    await bot.send_document(
        chat_id=REPORT_CHANNEL,
        document=data,
        caption=(
            "📦 <b>خروجی دیتابیس</b>\n"
            f"🔢 تعداد: "
            f"<code>{len(wallets)}</code>"
        ),
        parse_mode="HTML",
    )

    logger.info(
        "Export sent | total=%s",
        len(wallets),
    )


# ============================================================
# Queue helper
# ============================================================

def enqueue_task(task):
    try:

        task_queue.put_nowait(
            task
        )

        logger.info(
            "Task queued | type=%s | queue=%s",
            task.get("type"),
            task_queue.qsize(),
        )

        return True

    except queue.Full:

        logger.error(
            "Task queue is full"
        )

        return False


# ============================================================
# Worker
# ============================================================

def worker_loop():
    logger.info(
        "Background worker started"
    )

    while True:

        task = task_queue.get()

        try:

            task_type = task.get(
                "type"
            )

            logger.info(
                "Processing task: %s",
                task_type,
            )

            loop = asyncio.new_event_loop()

            asyncio.set_event_loop(
                loop
            )

            try:

                if task_type == "file":

                    loop.run_until_complete(
                        handle_file(
                            file_id=task[
                                "file_id"
                            ],
                            file_name=task[
                                "file_name"
                            ],
                            uploaded_at=task[
                                "uploaded_at"
                            ],
                            file_unique_id=task.get(
                                "file_unique_id"
                            ),
                        )
                    )

                elif task_type == "hourly":

                    loop.run_until_complete(
                        handle_hourly()
                    )

                elif task_type == "export":

                    loop.run_until_complete(
                        handle_export()
                    )

                else:

                    logger.warning(
                        "Unknown task type: %s",
                        task_type,
                    )

            finally:

                loop.close()

        except Exception:

            logger.exception(
                "Worker task failed"
            )

        finally:

            task_queue.task_done()


# ============================================================
# Scheduler
# ============================================================

def should_run_job(
    meta_key,
    interval_minutes,
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

        now = datetime.now(
            timezone.utc
        )

        elapsed = (
            now - previous
        ).total_seconds()

        return elapsed >= (
            interval_minutes * 60
        )

    except Exception:

        logger.exception(
            "Invalid scheduler meta: %s",
            meta_key,
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

            # ==================================================
            # Hourly
            # ==================================================

            if should_run_job(
                "last_hourly",
                HOURLY_INTERVAL_MIN,
            ):

                # فقط اگر قبلاً همین task داخل queue نیست
                if task_queue.qsize() < 3:

                    success = enqueue_task({
                        "type": "hourly"
                    })

                    if success:

                        set_meta(
                            "last_hourly",
                            now.isoformat(),
                        )

                        logger.info(
                            "Hourly job queued"
                        )

            # ==================================================
            # Export
            # ==================================================

            if should_run_job(
                "last_export",
                EXPORT_INTERVAL_MIN,
            ):

                if task_queue.qsize() < 3:

                    success = enqueue_task({
                        "type": "export"
                    })

                    if success:

                        set_meta(
                            "last_export",
                            now.isoformat(),
                        )

                        logger.info(
                            "Export job queued"
                        )

        except Exception:

            logger.exception(
                "Scheduler error"
            )

        # هر 30 ثانیه بررسی
        time.sleep(30)


# ============================================================
# Webhook
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"],
)
def webhook():

    try:

        data = request.get_json(
            force=True,
            silent=True,
        )

        if not data:

            logger.warning(
                "Webhook received empty JSON"
            )

            return "OK", 200

        update = Update.de_json(
            data,
            bot=None,
        )

        if not update:

            return "OK", 200

        # ----------------------------------------
        # فقط channel post
        # ----------------------------------------

        if not update.channel_post:

            return "OK", 200

        post = update.channel_post

        # ----------------------------------------
        # Source channel
        # ----------------------------------------

        if post.chat.id != SOURCE_CHANNEL:

            return "OK", 200

        # ----------------------------------------
        # فقط document
        # ----------------------------------------

        if not post.document:

            logger.info(
                "Source channel post has no document"
            )

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
            update.update_id,
        )

        # ----------------------------------------
        # Queue file
        # ----------------------------------------

        queued = enqueue_task({
            "type": "file",
            "file_id": document.file_id,
            "file_name": file_name,
            "uploaded_at": uploaded_at,
            "file_unique_id": getattr(
                document,
                "file_unique_id",
                None,
            ),
        })

        if not queued:

            logger.error(
                "Could not queue file: %s",
                file_name,
            )

        return "OK", 200

    except Exception:

        logger.exception(
            "Webhook error"
        )

        # Telegram نباید retry سنگین انجام دهد
        return "OK", 200


# ============================================================
# Health check
# ============================================================

@app.route(
    "/",
    methods=["GET", "HEAD"],
)
def health():

    try:

        count = get_wallet_count()

        queue_size = task_queue.qsize()

        return (
            "OK | "
            f"Queue: {queue_size} | "
            f"DB: {count}"
        ), 200

    except Exception as e:

        logger.exception(
            "Health check failed"
        )

        return (
            f"DB ERROR: {e}"
        ), 500


# ============================================================
# Health API
# ============================================================

@app.route(
    "/health",
    methods=["GET", "HEAD"],
)
def health_api():

    try:

        count = get_wallet_count()

        return {
            "status": "ok",
            "database": "ok",
            "wallets": count,
            "queue": task_queue.qsize(),
        }, 200

    except Exception as e:

        logger.exception(
            "Health API failed"
        )

        return {
            "status": "error",
            "error": str(e),
        }, 500


# ============================================================
# Startup
# ============================================================

def start_background_workers():

    global background_started

    with background_lock:

        if background_started:

            logger.info(
                "Background workers already started"
            )

            return

        logger.info(
            "Starting background workers"
        )

        worker_thread = threading.Thread(
            target=worker_loop,
            name="background-worker",
            daemon=True,
        )

        scheduler_thread = threading.Thread(
            target=scheduler_loop,
            name="scheduler",
            daemon=True,
        )

        worker_thread.start()
        scheduler_thread.start()

        background_started = True

        logger.info(
            "Background workers started successfully"
        )


# ============================================================
# Application initialization
# ============================================================

def initialize_application():

    logger.info(
        "Initializing application"
    )

    validate_config()

    create_db_pool()

    init_db()

    init_web3()

    start_background_workers()

    logger.info(
        "Application initialization completed"
    )


# ============================================================
# IMPORTANT:
# Gunicorn executes:
#
# gunicorn app:app
#
# Therefore initialization must happen when this
# module is imported, NOT only under __main__.
# ============================================================

try:

    initialize_application()

except Exception:

    logger.exception(
        "Application initialization failed"
    )

    raise


# ============================================================
# Local development
# ============================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000",
        )
    )

    logger.info(
        "Starting Flask development server on port %s",
        port,
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
    )
