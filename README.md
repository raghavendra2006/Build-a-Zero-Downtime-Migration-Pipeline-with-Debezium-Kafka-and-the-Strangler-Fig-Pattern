# Zero-Downtime Migration Pipeline

A production-grade simulation of a **zero-downtime database migration** using the **Strangler Fig pattern** and **Change Data Capture (CDC)** with Debezium, Kafka, and PostgreSQL.

## Architecture

```
┌──────────┐       ┌──────────────┐       ┌──────────────┐       ┌──────────┐
│  Client  │──────▶│   Gateway    │──────▶│ Legacy Svc   │──────▶│legacy_db │
│          │       │   :8080      │       │   :8081      │       │ (PG 14)  │
│          │       │              │──────▶│ Micro  Svc   │──────▶│micro_db  │
│          │       │              │       │   :8082      │       │ (PG 14)  │
└──────────┘       └──────────────┘       └──────────────┘       └──────────┘
                                                                       │
                   ┌──────────────┐       ┌──────────────┐             │ WAL
                   │ CDC Consumer │◀──────│    Kafka      │◀── Debezium ◀┘
                   │   :8083      │       │  (KRaft)      │   (Connect)
                   └──────┬───────┘       └──────────────┘
                          │
                   output/cdc_events.jsonl
```

### Key Patterns

| Pattern | Description |
|---------|-------------|
| **Strangler Fig** | Gateway routes traffic between legacy and microservice based on a configurable percentage, allowing gradual migration |
| **Change Data Capture** | Debezium captures every change from legacy_db's WAL and streams it to Kafka for downstream processing |
| **Dual-Write** | Every order is written to both databases concurrently to verify consistency between legacy and new systems |

## Services

| Service | Image | Host Port | Description |
|---------|-------|-----------|-------------|
| `gateway` | Python / FastAPI | 8080 | Traffic routing, dual-writes, metrics, rollback |
| `legacy_service` | Python / FastAPI | 8081 | Order creation in legacy database |
| `micro_service` | Python / FastAPI | 8082 | Order creation in new microservice database |
| `cdc_consumer` | Python / FastAPI | 8083 | Kafka consumer for CDC events + status API |
| `legacy_db` | postgres:14 | 5433 | Legacy PostgreSQL (WAL-enabled, seeded with 5000 orders) |
| `micro_db` | postgres:14 | 5434 | New microservice PostgreSQL (starts empty) |
| `kafka` | bitnami/kafka:3.6 | 9092 | Kafka broker in KRaft mode (no ZooKeeper) |
| `debezium` | debezium/connect:2.5 | 8083 | Kafka Connect with Debezium PostgreSQL connector |

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
docker-compose up --build -d

# Wait for all services to initialise (~60 seconds for CDC snapshot)
sleep 60

# Verify all containers are healthy
docker-compose ps
```

### Run Verification

```bash
# 1. Check CDC snapshot completed (should see snapshot_complete: true, snapshot_row_count: 5000)
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

curl -s http://localhost:8080/metrics | jq

# 4. Trigger rollback
curl -s -X POST http://localhost:8080/rollback | jq

# 5. Check output files
ls -la output/
wc -l output/cdc_events.jsonl
cat output/metrics_snapshot.json | jq
cat output/rollback_log.jsonl
```

### Tear Down

```bash
docker-compose down -v
```

## API Reference

### Gateway (`:8080`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/config` | Set traffic split percentage `{"micro_pct": 0-100}` |
| `POST` | `/orders` | Create order (dual-writes to both services) |
| `GET` | `/metrics` | Live metrics with p99 latency |
| `POST` | `/rollback` | Emergency rollback (sets micro_pct to 0) |

### Legacy Service (`:8081`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/legacy/orders` | Create order in legacy database |

### Micro Service (`:8082`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/micro/orders` | Create order in microservice database |

### CDC Consumer (`:8083`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/cdc/status` | CDC pipeline status (snapshot progress, discrepancy) |

## Output Files

| File | Format | Description |
|------|--------|-------------|
| `output/cdc_events.jsonl` | JSONL | All CDC events `{op, before, after, ts_ms}` |
| `output/metrics_snapshot.json` | JSON | Latest metrics snapshot (overwritten on each `/metrics` call) |
| `output/rollback_log.jsonl` | JSONL | Rollback event log `{rollback_triggered_at, micro_pct_before}` |

## Design Decisions

### Concurrency
The gateway uses `asyncio.gather` for concurrent dual-writes to both downstream services, ensuring minimal latency overhead.

### p99 Latency
Uses a sliding window of the last 1,000 measurements per service (via `collections.deque(maxlen=1000)`), providing accurate percentile estimation with O(1) memory.

### CDC Event Parsing
The CDC consumer parses Debezium's envelope format, detecting snapshot completion via the `source.snapshot` field (`"true"` / `"last"` / `"false"`).

### Error Handling
- Connection retries with exponential backoff during startup
- Graceful handling of downstream service failures in dual-writes
- `consistent` flag tracks write consistency across both services

## Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Application Services | Python 3.11 + FastAPI | Native async/await, high performance, clean API design |
| HTTP Client | httpx | Async HTTP client for concurrent dual-writes |
| Database Driver | asyncpg | High-performance async PostgreSQL driver |
| Kafka Client | aiokafka | Async Kafka consumer for non-blocking event processing |
| Databases | PostgreSQL 14 | Robust, supports logical replication for CDC |
| Message Broker | Apache Kafka 3.6 (KRaft) | Simplified architecture without ZooKeeper dependency |
| CDC Engine | Debezium 2.5 | Industry-standard CDC with PostgreSQL WAL integration |

## License

MIT