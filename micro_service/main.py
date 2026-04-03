"""
Micro Service — Handles order creation in the new microservice PostgreSQL database.
Exposes POST /micro/orders for inserting new orders.
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
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
            logger.info("Connected to micro_db")
            return p
        except Exception as exc:
            logger.warning("DB connection attempt %d failed: %s", attempt + 1, exc)
            await asyncio.sleep(2)
    raise RuntimeError("Could not connect to micro_db after 30 attempts")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await init_pool()
    yield
    if pool:
        await pool.close()


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(title="Micro Service", lifespan=lifespan)


class OrderRequest(BaseModel):
    customer_id: int
    amount: float
    status: str = "PENDING"


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
        async with pool.acquire() as conn:
            order_id = await conn.fetchval(
                """
                INSERT INTO orders (customer_id, amount, status)
                VALUES ($1, $2, $3)
                RETURNING order_id
                """,
                order.customer_id,
                round(float(order.amount), 2),
                order.status,
            )
        return JSONResponse(
            status_code=201,
            content={"order_id": order_id, "status": "created"},
        )
    except Exception as exc:
        logger.error("Failed to create order: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
