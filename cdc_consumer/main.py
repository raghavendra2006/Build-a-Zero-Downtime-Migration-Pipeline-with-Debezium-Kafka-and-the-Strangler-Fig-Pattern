"""
CDC Consumer Service — Consumes Debezium CDC events from Kafka,
writes them to /output/cdc_events.jsonl, and exposes a status endpoint.
"""

import os
import json
import asyncio
import logging
from contextlib import asynccontextmanager

import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cdc_consumer")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "legacyserver.public.orders")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "cdc-consumer-group")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/output/cdc_events.jsonl")

LEGACY_DB_HOST = os.getenv("LEGACY_DB_HOST", "legacy_db")
LEGACY_DB_PORT = int(os.getenv("LEGACY_DB_PORT", "5432"))
LEGACY_DB_USER = os.getenv("LEGACY_DB_USER", "postgres")
LEGACY_DB_PASSWORD = os.getenv("LEGACY_DB_PASSWORD", "postgres")
LEGACY_DB_NAME = os.getenv("LEGACY_DB_NAME", "postgres")

# ---------------------------------------------------------------------------
# State tracking
# ---------------------------------------------------------------------------
snapshot_complete = False
snapshot_row_count = 0
streaming_row_count = 0
total_cdc_events = 0
consumer_task: asyncio.Task | None = None
db_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def convert_numeric(obj):
    """Convert Decimal types to float for JSON serialization."""
    from decimal import Decimal
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: convert_numeric(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_numeric(i) for i in obj]
    return obj


def parse_debezium_event(raw_value: bytes) -> dict | None:
    """Parse a Debezium envelope and extract op, before, after, ts_ms."""
    try:
        envelope = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Unable to decode Kafka message")
        return None

    # Debezium wraps in a 'payload' key when using the default envelope
    payload = envelope.get("payload", envelope)
    if not isinstance(payload, dict):
        return None

    op = payload.get("op")
    if op is None:
        return None

    return {
        "op": op,
        "before": convert_numeric(payload.get("before")),
        "after": convert_numeric(payload.get("after")),
        "ts_ms": payload.get("ts_ms", 0),
    }


def is_snapshot_event(raw_value: bytes) -> bool | None:
    """Check if the event is part of the initial snapshot.
    
    Returns True if snapshot, False if streaming, None if unknown.
    """
    try:
        envelope = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    payload = envelope.get("payload", envelope)
    if not isinstance(payload, dict):
        return None

    source = payload.get("source", {})
    snapshot_val = source.get("snapshot", "false")

    # Debezium uses "true", "last", or "false" for the snapshot field
    if snapshot_val in ("true", "last"):
        return True
    return False


# ---------------------------------------------------------------------------
# Kafka consumer background task
# ---------------------------------------------------------------------------
async def consume_cdc_events():
    """Background coroutine that consumes CDC events from Kafka."""
    global snapshot_complete, snapshot_row_count, streaming_row_count, total_cdc_events

    logger.info("Starting Kafka consumer for topic '%s'", KAFKA_TOPIC)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,  # raw bytes
    )

    # Retry connecting to Kafka
    for attempt in range(60):
        try:
            await consumer.start()
            logger.info("Kafka consumer started")
            break
        except Exception as exc:
            logger.warning("Kafka connect attempt %d: %s", attempt + 1, exc)
            await asyncio.sleep(3)
    else:
        logger.error("Could not connect to Kafka after 60 attempts")
        return

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    try:
        async for msg in consumer:
            raw = msg.value
            if raw is None:
                continue

            event = parse_debezium_event(raw)
            if event is None:
                continue

            # Determine if this is a snapshot or streaming event
            is_snap = is_snapshot_event(raw)

            # Write to JSONL file
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, default=str) + "\n")

            total_cdc_events += 1

            if is_snap and not snapshot_complete:
                snapshot_row_count += 1
                # Check if this is the last snapshot record
                try:
                    envelope = json.loads(raw.decode("utf-8"))
                    payload = envelope.get("payload", envelope)
                    source = payload.get("source", {})
                    if source.get("snapshot") == "last":
                        snapshot_complete = True
                        logger.info(
                            "Snapshot complete — %d rows captured", snapshot_row_count
                        )
                except Exception:
                    pass
            elif not is_snap:
                if not snapshot_complete:
                    # First non-snapshot event means snapshot is done
                    snapshot_complete = True
                    logger.info(
                        "Snapshot complete (detected via first streaming event) — %d rows",
                        snapshot_row_count,
                    )
                streaming_row_count += 1

            if total_cdc_events % 1000 == 0:
                logger.info(
                    "Processed %d CDC events (snap=%d, stream=%d)",
                    total_cdc_events,
                    snapshot_row_count,
                    streaming_row_count,
                )
    except asyncio.CancelledError:
        logger.info("Kafka consumer task cancelled")
    except Exception as exc:
        logger.error("Kafka consumer error: %s", exc)
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped")


# ---------------------------------------------------------------------------
# Database pool for row count queries
# ---------------------------------------------------------------------------
async def init_db_pool() -> asyncpg.Pool:
    """Create a connection pool to legacy_db for row count verification."""
    for attempt in range(30):
        try:
            p = await asyncpg.create_pool(
                host=LEGACY_DB_HOST,
                port=LEGACY_DB_PORT,
                user=LEGACY_DB_USER,
                password=LEGACY_DB_PASSWORD,
                database=LEGACY_DB_NAME,
                min_size=1,
                max_size=3,
            )
            logger.info("Connected to legacy_db for row count queries")
            return p
        except Exception as exc:
            logger.warning("DB pool attempt %d: %s", attempt + 1, exc)
            await asyncio.sleep(2)
    raise RuntimeError("Could not connect to legacy_db for row counts")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task, db_pool
    db_pool = await init_db_pool()
    consumer_task = asyncio.create_task(consume_cdc_events())
    yield
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    if db_pool:
        await db_pool.close()


app = FastAPI(title="CDC Consumer Service", lifespan=lifespan)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "consumer_running": consumer_task is not None and not consumer_task.done()}


@app.get("/cdc/status")
async def cdc_status():
    """Return the current status of the CDC pipeline."""
    try:
        async with db_pool.acquire() as conn:
            db_count = await conn.fetchval("SELECT COUNT(*) FROM orders")
    except Exception as exc:
        logger.error("Failed to query db_row_count: %s", exc)
        raise HTTPException(status_code=500, detail=f"DB query failed: {exc}")

    return {
        "snapshot_complete": snapshot_complete,
        "snapshot_row_count": snapshot_row_count,
        "streaming_row_count": streaming_row_count,
        "total_cdc_events": total_cdc_events,
        "db_row_count": db_count,
        "discrepancy": db_count - total_cdc_events,
    }
