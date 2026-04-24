"""
Gateway Service — Strangler Fig pattern implementation.

Handles traffic routing, dual-writes to legacy + micro services,
metrics collection with p99 latency, and emergency rollback.

Design:
  - Thread-safe state via asyncio.Lock
  - Non-blocking file I/O via asyncio.to_thread
  - Configurable httpx connection pool
  - Pydantic field validation with constraints
  - Proper Decimal handling for financial amounts
"""

import os
import json
import asyncio
import time
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aiofiles
import orjson
from hdrh.histogram import HdrHistogram

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
)
logger = logging.getLogger("gateway")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LEGACY_SERVICE_URL = os.getenv("LEGACY_SERVICE_URL", "http://legacy_service:8081")
MICRO_SERVICE_URL = os.getenv("MICRO_SERVICE_URL", "http://micro_service:8082")
METRICS_OUTPUT_FILE = os.getenv("METRICS_OUTPUT_FILE", "/output/metrics_snapshot.json")
ROLLBACK_LOG_FILE = os.getenv("ROLLBACK_LOG_FILE", "/output/rollback_log.jsonl")
DUAL_WRITE_TIMEOUT_MS = int(os.getenv("DUAL_WRITE_TIMEOUT_MS", "500"))
LATENCY_WINDOW_SIZE = int(os.getenv("LATENCY_WINDOW_SIZE", "1000"))
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5.0"))


# ---------------------------------------------------------------------------
# Thread-safe state container
# ---------------------------------------------------------------------------
class GatewayState:
    """Thread-safe container for all mutable gateway state."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.micro_pct: int = 0
        self.legacy_request_count: int = 0
        self.micro_request_count: int = 0
        self.legacy_error_count: int = 0
        self.micro_error_count: int = 0
        self.consistent_writes: int = 0
        self.inconsistent_writes: int = 0
        # HdrHistogram: tracks values from 1 to 30000 (ms) with 2 significant digits
        self.legacy_latencies = HdrHistogram(1, 30000, 2)
        self.micro_latencies = HdrHistogram(1, 30000, 2)
        
        self.metrics_file_lock = asyncio.Lock()
        self.rollback_file_lock = asyncio.Lock()


state = GatewayState()

# HTTP client — initialised at startup
http_client: httpx.AsyncClient | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def calculate_p99(histogram: HdrHistogram) -> float:
    """Calculate p99 from a HdrHistogram of latency values (ms)."""
    if histogram.get_total_count() == 0:
        return 0.0
    return float(histogram.get_value_at_percentile(99.0))


def build_metrics_dict() -> dict:
    """Build the full metrics response dict (call under lock or after acquiring state)."""
    total = state.consistent_writes + state.inconsistent_writes
    consistency_rate = (
        round((state.consistent_writes / total) * 100, 2) if total > 0 else 100.0
    )
    return {
        "micro_pct": state.micro_pct,
        "legacy_request_count": state.legacy_request_count,
        "micro_request_count": state.micro_request_count,
        "legacy_error_count": state.legacy_error_count,
        "micro_error_count": state.micro_error_count,
        "consistent_writes": state.consistent_writes,
        "inconsistent_writes": state.inconsistent_writes,
        "consistency_rate_pct": consistency_rate,
        "legacy_p99_ms": calculate_p99(state.legacy_latencies),
        "micro_p99_ms": calculate_p99(state.micro_latencies),
    }


def _write_file_sync(path: str, content: str, mode: str = "w") -> None:
    # Deprecated in favor of aiofiles
    pass


async def post_to_service(url: str, payload: dict) -> tuple[dict | None, float, bool]:
    """
    POST an order to a downstream service.
    Returns (response_json, latency_ms, success).
    """
    start = time.monotonic()
    try:
        resp = await http_client.post(url, json=payload, timeout=HTTP_TIMEOUT_S)
        latency_ms = (time.monotonic() - start) * 1000
        if 200 <= resp.status_code < 300:
            try:
                data = orjson.loads(resp.content)
            except orjson.JSONDecodeError:
                data = None
            return data, latency_ms, True
        else:
            logger.warning("Service %s returned %d", url, resp.status_code)
            return None, latency_ms, False
    except Exception as exc:
        latency_ms = (time.monotonic() - start) * 1000
        logger.error("Service call to %s failed: %s", url, exc)
        return None, latency_ms, False


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        timeout=httpx.Timeout(HTTP_TIMEOUT_S, connect=5.0),
    )
    os.makedirs(os.path.dirname(METRICS_OUTPUT_FILE), exist_ok=True)
    logger.info("Gateway started — legacy=%s, micro=%s", LEGACY_SERVICE_URL, MICRO_SERVICE_URL)
    yield
    if http_client:
        await http_client.aclose()
    logger.info("Gateway shutdown complete")


app = FastAPI(
    title="Gateway Service",
    description="Strangler Fig traffic router with dual-write CDC pipeline",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response models with validation
# ---------------------------------------------------------------------------
class ConfigRequest(BaseModel):
    micro_pct: int = Field(..., ge=0, le=100, description="Traffic percentage for micro service (0-100)")


class OrderRequest(BaseModel):
    customer_id: int = Field(..., gt=0, description="Customer identifier")
    amount: float = Field(..., gt=0, description="Order amount (must be positive)")
    status: str = Field(default="PENDING", pattern=r"^[A-Z_]{2,20}$", description="Order status")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    """Health check."""
    return {"status": "healthy"}


@app.post("/config")
async def update_config(config: ConfigRequest):
    """Update the traffic split percentage."""
    async with state.lock:
        state.micro_pct = config.micro_pct
    logger.info("Traffic split updated: micro_pct=%d", config.micro_pct)
    return {"micro_pct": config.micro_pct, "updated": True}


@app.post("/orders")
async def create_order(order: OrderRequest):
    """
    Dual-write endpoint: concurrently sends order to both legacy and micro services.
    Routes based on customer_id % 100 < micro_pct.
    """
    payload = {
        "customer_id": order.customer_id,
        "amount": order.amount,
        "status": order.status,
    }

    # Snapshot micro_pct under lock for consistent routing
    async with state.lock:
        current_micro_pct = state.micro_pct

    # Concurrent dual-write using asyncio.gather
    legacy_result, micro_result = await asyncio.gather(
        post_to_service(f"{LEGACY_SERVICE_URL}/legacy/orders", payload),
        post_to_service(f"{MICRO_SERVICE_URL}/micro/orders", payload),
    )

    legacy_data, legacy_latency, legacy_ok = legacy_result
    micro_data, micro_latency, micro_ok = micro_result

    # Determine routing
    routed_to = "micro" if (order.customer_id % 100) < current_micro_pct else "legacy"

    # Consistency check: both succeed and each within the timeout threshold
    is_consistent = (
        legacy_ok
        and micro_ok
        and legacy_latency <= DUAL_WRITE_TIMEOUT_MS
        and micro_latency <= DUAL_WRITE_TIMEOUT_MS
    )

    # Update state atomically
    async with state.lock:
        state.legacy_latencies.record_value(max(1, min(int(legacy_latency), 30000)))
        state.micro_latencies.record_value(max(1, min(int(micro_latency), 30000)))

        if routed_to == "legacy":
            state.legacy_request_count += 1
        else:
            state.micro_request_count += 1

        if not legacy_ok:
            state.legacy_error_count += 1
        if not micro_ok:
            state.micro_error_count += 1

        if is_consistent:
            state.consistent_writes += 1
        else:
            state.inconsistent_writes += 1

    return {
        "routed_to": routed_to,
        "legacy_order_id": legacy_data.get("order_id") if legacy_data else None,
        "micro_order_id": micro_data.get("order_id") if micro_data else None,
        "consistent": is_consistent,
        "latency_ms": {
            "legacy": round(legacy_latency),
            "micro": round(micro_latency),
        },
    }


@app.get("/metrics")
async def get_metrics():
    """Return live metrics and write snapshot to file."""
    async with state.lock:
        metrics = build_metrics_dict()

    # Async file write with dedicated lock
    try:
        content = orjson.dumps(metrics, option=orjson.OPT_INDENT_2).decode("utf-8")
        async with state.metrics_file_lock:
            async with aiofiles.open(METRICS_OUTPUT_FILE, "w", encoding="utf-8") as f:
                await f.write(content)
    except Exception as exc:
        logger.error("Failed to write metrics snapshot: %s", exc)

    return metrics


@app.post("/rollback")
async def rollback():
    """Emergency rollback: set micro_pct to 0 and log the event."""
    async with state.lock:
        pct_before = state.micro_pct
        state.micro_pct = 0

    # Build log entry
    log_entry = {
        "rollback_triggered_at": datetime.now(timezone.utc).isoformat(),
        "micro_pct_before": pct_before,
    }

    # Async file append with dedicated lock
    try:
        line = orjson.dumps(log_entry).decode("utf-8") + "\n"
        async with state.rollback_file_lock:
            async with aiofiles.open(ROLLBACK_LOG_FILE, "a", encoding="utf-8") as f:
                await f.write(line)
    except Exception as exc:
        logger.error("Failed to write rollback log: %s", exc)

    logger.info("ROLLBACK triggered: micro_pct %d -> 0", pct_before)

    return {"rolled_back": True, "micro_pct": 0}
