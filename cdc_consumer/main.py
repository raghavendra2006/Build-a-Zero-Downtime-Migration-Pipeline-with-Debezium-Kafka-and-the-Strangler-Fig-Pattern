"""
CDC Consumer Service — Consumes Debezium CDC events from Kafka,
writes them to /output/cdc_events.jsonl, and exposes a status endpoint.

Design:
  - Single parse per message (no double JSON.loads)
  - Buffered async file I/O via aiofiles
  - Robust snapshot detection using both source.snapshot and op fields
  - Thread-safe counters via asyncio.Lock
  - Graceful shutdown with proper consumer cleanup
"""

import os
import json
import asyncio
import logging
from contextlib import asynccontextmanager

import aiofiles
import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
)
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
# Thread-safe state
# ---------------------------------------------------------------------------
class CDCState:
    """Thread-safe container for CDC consumer state."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.snapshot_complete: bool = False
        self.snapshot_row_count: int = 0
        self.streaming_row_count: int = 0
        self.total_cdc_events: int = 0


cdc_state = CDCState()
consumer_task: asyncio.Task | None = None
db_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Debezium event parsing — SINGLE PARSE per message
# ---------------------------------------------------------------------------
def parse_and_classify(raw_value: bytes) -> tuple[dict | None, str]:
    """Parse a raw Debezium message in a single pass.

    Returns:
        (event_dict, snapshot_status) where:
        - event_dict is {op, before, after, ts_ms} or None on parse failure
        - snapshot_status is "snapshot", "last", "streaming", or "unknown"
    """
    try:
        envelope = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Unable to decode Kafka message")
        return None, "unknown"

    # Handle both wrapped (schema+payload) and schema-less formats
    if not isinstance(envelope, dict):
        return None, "unknown"
    payload = envelope.get("payload", envelope)
    if not isinstance(payload, dict):
        return None, "unknown"

    op = payload.get("op")
    if op is None:
        return None, "unknown"

    # --- Build output event ---
    event = {
        "op": op,
        "before": _sanitize_values(payload.get("before")),
        "after": _sanitize_values(payload.get("after")),
        "ts_ms": payload.get("ts_ms", 0),
    }

    # --- Classify snapshot status ---
    snap_status = "streaming"
    source = payload.get("source", {})
    if isinstance(source, dict):
        snapshot_val = source.get("snapshot")
        if snapshot_val == "last":
            snap_status = "last"
        elif snapshot_val in ("true", True):
            snap_status = "snapshot"
        elif op == "r":
            # Fallback: op=r means snapshot read even if source.snapshot missing
            snap_status = "snapshot"
    elif op == "r":
        snap_status = "snapshot"

    return event, snap_status


def _sanitize_values(obj):
    """Recursively convert Decimal-like values for safe JSON serialization."""
    from decimal import Decimal

    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _sanitize_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_values(i) for i in obj]
    return obj


# ---------------------------------------------------------------------------
# Kafka consumer background task
# ---------------------------------------------------------------------------
async def consume_cdc_events():
    """Background coroutine that consumes CDC events from Kafka."""
    logger.info("Starting Kafka consumer for topic '%s'", KAFKA_TOPIC)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,  # raw bytes
    )

    # Retry connecting to Kafka with backoff
    for attempt in range(60):
        try:
            await consumer.start()
            logger.info("Kafka consumer connected to %s", KAFKA_BROKER)
            break
        except Exception as exc:
            wait = min(3 + attempt * 0.5, 10)
            logger.warning("Kafka connect attempt %d: %s (retry in %.1fs)", attempt + 1, exc, wait)
            await asyncio.sleep(wait)
    else:
        logger.error("FATAL: Could not connect to Kafka after 60 attempts")
        return

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Use async file handle for buffered I/O
    try:
        async with aiofiles.open(OUTPUT_FILE, mode="a", encoding="utf-8") as outfile:
            async for msg in consumer:
                raw = msg.value
                if raw is None:
                    continue

                # SINGLE parse per message
                event, snap_status = parse_and_classify(raw)
                if event is None:
                    continue

                # Async write to JSONL
                await outfile.write(json.dumps(event, default=str) + "\n")
                await outfile.flush()

                # Update state under lock
                async with cdc_state.lock:
                    cdc_state.total_cdc_events += 1

                    if snap_status in ("snapshot", "last") and not cdc_state.snapshot_complete:
                        cdc_state.snapshot_row_count += 1
                        if snap_status == "last":
                            cdc_state.snapshot_complete = True
                            logger.info(
                                "Snapshot complete (last marker) — %d rows",
                                cdc_state.snapshot_row_count,
                            )
                    elif snap_status == "streaming":
                        if not cdc_state.snapshot_complete:
                            cdc_state.snapshot_complete = True
                            logger.info(
                                "Snapshot complete (first streaming event) — %d rows",
                                cdc_state.snapshot_row_count,
                            )
                        cdc_state.streaming_row_count += 1

                    if cdc_state.total_cdc_events % 1000 == 0:
                        logger.info(
                            "Processed %d events (snap=%d, stream=%d)",
                            cdc_state.total_cdc_events,
                            cdc_state.snapshot_row_count,
                            cdc_state.streaming_row_count,
                        )
    except asyncio.CancelledError:
        logger.info("Kafka consumer task cancelled")
    except Exception as exc:
        logger.error("Kafka consumer error: %s", exc, exc_info=True)
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
    raise RuntimeError("Could not connect to legacy_db after 30 attempts")


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
    logger.info("CDC Consumer shutdown complete")


app = FastAPI(
    title="CDC Consumer Service",
    description="Debezium CDC event consumer with status reporting",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Health check endpoint."""
    running = consumer_task is not None and not consumer_task.done()
    return {"status": "healthy", "consumer_running": running}


@app.get("/cdc/status")
async def cdc_status():
    """Return the current status of the CDC pipeline."""
    try:
        async with db_pool.acquire() as conn:
            db_count = await conn.fetchval("SELECT COUNT(*) FROM orders")
    except Exception as exc:
        logger.error("Failed to query db_row_count: %s", exc)
        raise HTTPException(status_code=500, detail=f"DB query failed: {exc}")

    async with cdc_state.lock:
        return {
            "snapshot_complete": cdc_state.snapshot_complete,
            "snapshot_row_count": cdc_state.snapshot_row_count,
            "streaming_row_count": cdc_state.streaming_row_count,
            "total_cdc_events": cdc_state.total_cdc_events,
            "db_row_count": db_count,
            "discrepancy": db_count - cdc_state.total_cdc_events,
        }
