#!/bin/bash
set -e

echo "1. Verify CDC snapshot"
curl -s http://localhost:8083/cdc/status | jq

echo "2. Set 0% traffic and send 50 orders"
curl -s -X POST http://localhost:8080/config -H "Content-Type: application/json" -d '{"micro_pct": 0}' > /dev/null
for i in $(seq 1 50); do
  curl -s -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d "{\"customer_id\": $i, \"amount\": 19.99, \"status\": \"PENDING\"}" > /dev/null
done
curl -s http://localhost:8080/metrics | jq

echo "3. Ramp to 50% traffic and send 100 orders"
curl -s -X POST http://localhost:8080/config -H "Content-Type: application/json" -d '{"micro_pct": 50}' > /dev/null
for i in $(seq 1 100); do
  curl -s -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d "{\"customer_id\": $i, \"amount\": 49.99, \"status\": \"PENDING\"}" > /dev/null
done
curl -s http://localhost:8080/metrics | jq

echo "4. Trigger rollback"
curl -s -X POST http://localhost:8080/rollback | jq

echo "5. Check output files"
ls -l output/
wc -l output/cdc_events.jsonl
