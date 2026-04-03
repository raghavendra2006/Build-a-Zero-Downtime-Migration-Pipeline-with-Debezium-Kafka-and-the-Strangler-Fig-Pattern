"""
Micro Service — Handles order creation in the new microservice PostgreSQL database.

Exposes:
  POST /micro/orders  — Insert a new order
  GET  /health        — Health check with DB connectivity verification

Design:
  - asyncpg connection pool with retry logic
  - Proper Decimal handling for NUMERIC columns
  - Structured logging
  - Graceful shutdown
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
)
logger = logging.getLogger("micro_service")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "micro_db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "postgres")

# ---------------------------------------------------------------------------
# Database pool
# ---------------------------------------------------------------------------
pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """Create a connection pool with retries for container startup ordering."""
    for attempt in range(30):
        try:
            p = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                min_size=2,
                max_size=10,
            )
            logger.info("Connected to micro_db at %s:%d", DB_HOST, DB_PORT)
            return p
        except Exception as exc:
            wait = min(2 + attempt * 0.5, 10)
            logger.warning("DB connection attempt %d failed: %s (retry in %.1fs)", attempt + 1, exc, wait)
            await asyncio.sleep(wait)
    raise RuntimeError("Could not connect to micro_db after 30 attempts")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await init_pool()
    yield
    if pool:
        await pool.close()
    logger.info("Micro service shutdown complete")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Micro Service",
    description="Order creation endpoint for the new microservice PostgreSQL database",
    version="1.0.0",
    lifespan=lifespan,
)


class OrderRequest(BaseModel):
    customer_id: int = Field(..., gt=0, description="Customer identifier")
    amount: float = Field(..., gt=0, description="Order amount (must be positive)")
    status: str = Field(default="PENDING", description="Order status")


@app.get("/health")
async def health():
    """Health check — verifies DB connectivity."""
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/micro/orders")
async def create_order(order: OrderRequest):
    """Insert a new order into the microservice database."""
    try:
        amount_decimal = Decimal(str(order.amount)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        raise HTTPException(status_code=400, detail="Invalid amount value")

    try:
        async with pool.acquire() as conn:
            order_id = await conn.fetchval(
                """
                INSERT INTO orders (customer_id, amount, status)
                VALUES ($1, $2, $3)
                RETURNING order_id
                """,
                order.customer_id,
                amount_decimal,
                order.status,
            )
        return JSONResponse(
            status_code=201,
            content={"order_id": order_id, "status": "created"},
        )
    except Exception as exc:
        logger.error("Failed to create order: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
