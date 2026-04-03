# Zero-Downtime Migration Pipeline

A production-grade simulation of a **zero-downtime database migration** using the **Strangler Fig pattern** and **Change Data Capture (CDC)** with Debezium, Kafka, and PostgreSQL.

## Table of Contents

- [Architecture](#architecture)
- [Services](#services)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Output Files](#output-files)
- [Design Decisions](#design-decisions)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

## Architecture

```
┌──────────┐       ┌──────────────┐       ┌──────────────┐       ┌──────────┐
│  Client  │──────▶│   Gateway    │──────▶│ Legacy Svc   │──────▶│legacy_db │
│          │       │   :8080      │       │   :8081      │       │ (PG 14)  │
│          │       │              │──────▶│ Micro  Svc   │──────▶│micro_db  │
│          │       │   dual-write │       │   :8082      │       │ (PG 14)  │
└──────────┘       └──────────────┘       └──────────────┘       └──────────┘
                                                                       │
                   ┌──────────────┐       ┌──────────────┐             │ WAL
                   │ CDC Consumer │◀──────│    Kafka      │◀── Debezium ◀┘
                   │   :8083      │       │  (KRaft 3.7)  │   (Connect 2.5)
                   └──────┬───────┘       └──────────────┘
                          │
                   output/cdc_events.jsonl
```

### Key Patterns

| Pattern | Description |
|---------|-------------|
| **Strangler Fig** | Gateway routes traffic between legacy and microservice based on a configurable percentage (`customer_id % 100 < micro_pct`), allowing gradual migration with instant rollback capability |
| **Change Data Capture** | Debezium captures every INSERT/UPDATE/DELETE from `legacy_db`'s WAL (Write-Ahead Log) and streams them to Kafka in real-time |
| **Dual-Write** | Every order is written to **both** databases concurrently via `asyncio.gather` to verify consistency between legacy and new systems |

## Services

| Service | Image | Host Port | Description |
|---------|-------|-----------|-------------|
| `gateway` | Python 3.11 / FastAPI | 8080 | Traffic routing, dual-writes, metrics, rollback |
| `legacy_service` | Python 3.11 / FastAPI | 8081 | Order creation in legacy database |
| `micro_service` | Python 3.11 / FastAPI | 8082 | Order creation in new microservice database |
| `cdc_consumer` | Python 3.11 / FastAPI | 8083 | Kafka consumer for CDC events + status API |
| `legacy_db` | postgres:14 | 5433 | Legacy PostgreSQL (WAL-enabled, seeded with 5,000 orders) |
| `micro_db` | postgres:14 | 5434 | New microservice PostgreSQL (starts empty) |
| `kafka` | apache/kafka:3.7.0 | — (internal) | Kafka broker in KRaft mode (no ZooKeeper) |
| `debezium` | debezium/connect:2.5 | — (internal) | Kafka Connect with Debezium PostgreSQL connector |

## Quick Start

### Prerequisites

- **Docker** ≥ 20.10
- **Docker Compose** ≥ 2.0
- ~4 GB free RAM for all containers

### Start the Pipeline

```bash
# Clone the repository
git clone <repository-url>
cd <repository-name>

# (Optional) Copy and customise environment variables
cp .env.example .env

# Start all 8 services
docker compose up --build -d

# Wait for all services to initialise (~60 seconds for CDC snapshot)
sleep 60

# Verify all containers are healthy
docker compose ps
```

### Run Verification

```bash
# 1. Check CDC snapshot completed
#    Expect: snapshot_complete: true, snapshot_row_count: 5000, discrepancy: 0
curl -s http://localhost:8083/cdc/status | jq

# 2. Set 0% traffic to micro and send 50 orders
curl -s -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d '{"micro_pct": 0}'

for i in $(seq 1 50); do
  curl -s -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" \
    -d "{\"customer_id\": $i, \"amount\": 19.99, \"status\": \"PENDING\"}"
done

# Check metrics — all 50 should route to legacy
curl -s http://localhost:8080/metrics | jq

# 3. Ramp to 50% and send 100 orders
curl -s -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d '{"micro_pct": 50}'

for i in $(seq 1 100); do
  curl -s -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" \
    -d "{\"customer_id\": $i, \"amount\": 49.99, \"status\": \"PENDING\"}"
done

# Check metrics — should show ~50/50 split
curl -s http://localhost:8080/metrics | jq

# 4. Trigger emergency rollback
curl -s -X POST http://localhost:8080/rollback | jq
# Expect: {"rolled_back": true, "micro_pct": 0}

# 5. Verify output files
ls -la output/
wc -l output/cdc_events.jsonl    # Should be ≥ 5000
cat output/metrics_snapshot.json | jq
cat output/rollback_log.jsonl
```

### Tear Down

```bash
docker compose down -v
```

## API Reference

### Gateway (`:8080`)

| Method | Endpoint | Description | Request Body | Response |
|--------|----------|-------------|--------------|----------|
| `POST` | `/config` | Set traffic split | `{"micro_pct": 0-100}` | `{"micro_pct": N, "updated": true}` |
| `POST` | `/orders` | Create order (dual-write) | `{"customer_id": int, "amount": float, "status": str}` | `{"routed_to", "legacy_order_id", "micro_order_id", "consistent", "latency_ms"}` |
| `GET` | `/metrics` | Live metrics + p99 latency | — | All 10 metric fields |
| `POST` | `/rollback` | Emergency rollback | — | `{"rolled_back": true, "micro_pct": 0}` |

### Legacy Service (`:8081`)

| Method | Endpoint | Request Body | Response |
|--------|----------|--------------|----------|
| `POST` | `/legacy/orders` | `{"customer_id", "amount", "status"}` | `{"order_id": int, "status": "created"}` |

### Micro Service (`:8082`)

| Method | Endpoint | Request Body | Response |
|--------|----------|--------------|----------|
| `POST` | `/micro/orders` | `{"customer_id", "amount", "status"}` | `{"order_id": int, "status": "created"}` |

### CDC Consumer (`:8083`)

| Method | Endpoint | Response |
|--------|----------|----------|
| `GET` | `/cdc/status` | `{"snapshot_complete", "snapshot_row_count", "streaming_row_count", "total_cdc_events", "db_row_count", "discrepancy"}` |

## Output Files

| File | Format | Written By | Description |
|------|--------|------------|-------------|
| `output/cdc_events.jsonl` | JSONL | CDC Consumer | Every CDC event: `{"op": "r\|c\|u\|d", "before": {...}, "after": {...}, "ts_ms": int}` |
| `output/metrics_snapshot.json` | JSON | Gateway | Latest metrics (overwritten on each `GET /metrics`) |
| `output/rollback_log.jsonl` | JSONL | Gateway | Rollback events: `{"rollback_triggered_at": "ISO8601", "micro_pct_before": int}` |

## Design Decisions

### Thread-Safe State Management
All mutable gateway and CDC consumer state is protected by `asyncio.Lock` to prevent race conditions under concurrent request handling.

### Non-Blocking File I/O
Gateway uses `asyncio.to_thread()` for file writes. CDC consumer uses `aiofiles` for async JSONL output. Neither service blocks the event loop during I/O operations.

### Concurrent Dual-Writes
The gateway uses `asyncio.gather()` to POST to both legacy and micro services simultaneously, keeping dual-write overhead near the latency of the slower service rather than the sum.

### p99 Latency Calculation
Uses a sliding window of the last 1,000 measurements per service via `collections.deque(maxlen=1000)`. On `/metrics`, the window is sorted and the 99th percentile index is selected. This provides accurate estimation with constant O(1) memory.

### Decimal Precision
Order amounts are converted from `float` to `Decimal` via `Decimal(str(amount)).quantize(Decimal("0.01"))` before database insertion. This prevents floating-point imprecision when storing in PostgreSQL `NUMERIC(10,2)` columns.

### CDC Snapshot Detection
Uses a dual-strategy approach: first checks Debezium's `source.snapshot` field (`"true"`, `"last"`, `"false"`), then falls back to the `op` field (`"r"` = snapshot read). This handles both schema-enabled and schema-disabled Debezium envelope formats.

### Container Resilience
All services use `restart: unless-stopped` with graceful shutdown via `stop_grace_period`. Connection retries with progressive backoff ensure proper startup ordering even without `depends_on` guarantees.

## Project Structure

```
├── docker-compose.yml          # All 8 services with health checks
├── .env.example                # Environment variable documentation
├── .gitignore                  # Git ignore rules
├── README.md                   # This file
├── legacy_service/
│   ├── Dockerfile              # Python 3.11-slim + curl
│   ├── .dockerignore           # Exclude unnecessary files from build
│   ├── requirements.txt        # fastapi, uvicorn, asyncpg
│   ├── init.sql                # Schema + 5,000 seed rows
│   └── main.py                 # POST /legacy/orders, GET /health
├── micro_service/
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── requirements.txt
│   ├── init.sql                # Schema only (empty table)
│   └── main.py                 # POST /micro/orders, GET /health
├── cdc_consumer/
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── requirements.txt        # + aiokafka, aiofiles
│   └── main.py                 # Kafka consumer + GET /cdc/status
├── gateway/
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── requirements.txt        # + httpx
│   └── main.py                 # /config, /orders, /metrics, /rollback
├── debezium/
│   ├── entrypoint.sh           # Custom entrypoint wrapper
│   └── register-connector.sh   # Auto-registers Debezium connector
└── output/                     # Mounted volume for runtime output
    └── .gitkeep
```

## Technology Stack

| Component | Technology | Version | Rationale |
|-----------|-----------|---------|-----------|
| Application Services | Python + FastAPI | 3.11 / 0.111.0 | Native async/await, high performance, automatic OpenAPI docs |
| HTTP Client | httpx | 0.27.0 | Async HTTP with connection pooling for concurrent dual-writes |
| Database Driver | asyncpg | 0.29.0 | Fastest async PostgreSQL driver for Python |
| Kafka Client | aiokafka | 0.10.0 | Async Kafka consumer for non-blocking event processing |
| Async File I/O | aiofiles | 23.2.1 | Non-blocking file writes for CDC event output |
| Databases | PostgreSQL | 14 | Robust ACID compliance, logical replication for CDC |
| Message Broker | Apache Kafka (KRaft) | 3.7.0 | Simplified architecture without ZooKeeper dependency |
| CDC Engine | Debezium | 2.5 | Industry-standard CDC with PostgreSQL WAL integration |

## Troubleshooting

### Debezium connector not starting
1. Check `legacy_db` has `wal_level=logical`: `docker exec legacy_db psql -U postgres -c "SHOW wal_level;"`
2. Check Debezium logs: `docker logs debezium`
3. Verify connector: `docker exec debezium curl -s http://localhost:8083/connectors`

### CDC snapshot not completing
1. Check CDC consumer logs: `docker logs cdc_consumer`
2. Verify Kafka topic exists: `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`
3. Check status endpoint: `curl -s http://localhost:8083/cdc/status | jq`

### Orders failing
1. Check gateway logs: `docker logs gateway`
2. Verify downstream services: `curl http://localhost:8081/health` and `curl http://localhost:8082/health`
3. Check database connectivity: `docker exec legacy_db psql -U postgres -c "SELECT COUNT(*) FROM orders;"`