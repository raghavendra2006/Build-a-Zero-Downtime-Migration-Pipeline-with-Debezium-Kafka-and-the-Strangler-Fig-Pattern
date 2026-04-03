"""
Gateway Service — Strangler Fig pattern implementation.

Handles traffic routing, dual-writes to legacy + micro services,
metrics collection with p99 latency, and emergency rollback.
"""

import os
import json
import asyncio
import time
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gateway")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LEGACY_SERVICE_URL = os.getenv("LEGACY_SERVICE_URL", "http://legacy_service:8081")
MICRO_SERVICE_URL = os.getenv("MICRO_SERVICE_URL", "http://micro_service:8082")
METRICS_OUTPUT_FILE = os.getenv("METRICS_OUTPUT_FILE", "/output/metrics_snapshot.json")
ROLLBACK_LOG_FILE = os.getenv("ROLLBACK_LOG_FILE", "/output/rollback_log.jsonl")
DUAL_WRITE_TIMEOUT = float(os.getenv("DUAL_WRITE_TIMEOUT_MS", "500")) / 1000  # seconds
LATENCY_WINDOW_SIZE = int(os.getenv("LATENCY_WINDOW_SIZE", "1000"))

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
micro_pct: int = 0  # 0–100: percentage of traffic routed to micro

legacy_request_count: int = 0
micro_request_count: int = 0
legacy_error_count: int = 0
micro_error_count: int = 0
consistent_writes: int = 0
inconsistent_writes: int = 0

# Sliding windows for p99 latency calculation (fixed memory)
legacy_latencies: deque = deque(maxlen=LATENCY_WINDOW_SIZE)
micro_latencies: deque = deque(maxlen=LATENCY_WINDOW_SIZE)

# HTTP client
http_client: httpx.AsyncClient | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def calculate_p99(latencies: deque) -> float:
    """Calculate p99 from a deque of latency values (ms)."""
    if not latencies:
        return 0.0
    sorted_vals = sorted(latencies)
    idx = int(len(sorted_vals) * 0.99)
    idx = min(idx, len(sorted_vals) - 1)
    return round(sorted_vals[idx], 2)


def build_metrics_response() -> dict:
    """Build the full metrics response object."""
    total = consistent_writes + inconsistent_writes
    consistency_rate = round((consistent_writes / total) * 100, 2) if total > 0 else 100.0

    return {
        "micro_pct": micro_pct,
        "legacy_request_count": legacy_request_count,
        "micro_request_count": micro_request_count,
        "legacy_error_count": legacy_error_count,
        "micro_error_count": micro_error_count,
        "consistent_writes": consistent_writes,
        "inconsistent_writes": inconsistent_writes,
        "consistency_rate_pct": consistency_rate,
        "legacy_p99_ms": calculate_p99(legacy_latencies),
        "micro_p99_ms": calculate_p99(micro_latencies),
    }


async def post_to_service(url: str, payload: dict) -> tuple[dict | None, float, bool]:
    """
    POST an order to a downstream service.
    Returns (response_json, latency_ms, success).
    """
    start = time.monotonic()
    try:
        resp = await http_client.post(url, json=payload, timeout=5.0)
        latency_ms = (time.monotonic() - start) * 1000
        if 200 <= resp.status_code < 300:
            return resp.json(), latency_ms, True
        else:
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
    http_client = httpx.AsyncClient()
    # Ensure output directory exists
    os.makedirs(os.path.dirname(METRICS_OUTPUT_FILE), exist_ok=True)
    yield
    if http_client:
        await http_client.aclose()


app = FastAPI(title="Gateway Service", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class ConfigRequest(BaseModel):
    micro_pct: int


class OrderRequest(BaseModel):
    customer_id: int
    amount: float
    status: str = "PENDING"


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
    global micro_pct
    if not (0 <= config.micro_pct <= 100):
        raise HTTPException(status_code=400, detail="micro_pct must be 0-100")
    micro_pct = config.micro_pct
    logger.info("Traffic split updated: micro_pct=%d", micro_pct)
    return {"micro_pct": micro_pct, "updated": True}


@app.post("/orders")
async def create_order(order: OrderRequest):
    """
    Dual-write endpoint: concurrently sends order to both legacy and micro services.
    Routes based on customer_id % 100 < micro_pct.
    """
    global legacy_request_count, micro_request_count
    global legacy_error_count, micro_error_count
    global consistent_writes, inconsistent_writes

    payload = {
        "customer_id": order.customer_id,
        "amount": order.amount,
        "status": order.status,
    }

    # Concurrent dual-write
    legacy_result, micro_result = await asyncio.gather(
        post_to_service(f"{LEGACY_SERVICE_URL}/legacy/orders", payload),
        post_to_service(f"{MICRO_SERVICE_URL}/micro/orders", payload),
    )

    legacy_data, legacy_latency, legacy_ok = legacy_result
    micro_data, micro_latency, micro_ok = micro_result

    # Track latencies in sliding window
    legacy_latencies.append(legacy_latency)
    micro_latencies.append(micro_latency)

    # Determine routing
    routed_to = "micro" if (order.customer_id % 100) < micro_pct else "legacy"

    # Update routed request counts
    if routed_to == "legacy":
        legacy_request_count += 1
    else:
        micro_request_count += 1

    # Track errors
    if not legacy_ok:
        legacy_error_count += 1
    if not micro_ok:
        micro_error_count += 1

    # Consistency check: both succeed and within timeout threshold
    is_consistent = (
        legacy_ok
        and micro_ok
        and legacy_latency <= (DUAL_WRITE_TIMEOUT * 1000)
        and micro_latency <= (DUAL_WRITE_TIMEOUT * 1000)
    )
    if is_consistent:
        consistent_writes += 1
    else:
        inconsistent_writes += 1

    # Build response
    response = {
        "routed_to": routed_to,
        "legacy_order_id": legacy_data.get("order_id") if legacy_data else None,
        "micro_order_id": micro_data.get("order_id") if micro_data else None,
        "consistent": is_consistent,
        "latency_ms": {
            "legacy": round(legacy_latency),
            "micro": round(micro_latency),
        },
    }

    return response


@app.get("/metrics")
async def get_metrics():
    """Return live metrics and write snapshot to file."""
    metrics = build_metrics_response()

    # Write metrics snapshot to file (overwrite)
    try:
        with open(METRICS_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
    except Exception as exc:
        logger.error("Failed to write metrics snapshot: %s", exc)

    return metrics


@app.post("/rollback")
async def rollback():
    """Emergency rollback: set micro_pct to 0 and log the event."""
    global micro_pct

    pct_before = micro_pct
    micro_pct = 0

    # Append to rollback log
    log_entry = {
        "rollback_triggered_at": datetime.now(timezone.utc).isoformat(),
        "micro_pct_before": pct_before,
    }

    try:
        with open(ROLLBACK_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")
    except Exception as exc:
        logger.error("Failed to write rollback log: %s", exc)

    logger.info("Rollback triggered: micro_pct %d -> 0", pct_before)

    return {"rolled_back": True, "micro_pct": 0}
