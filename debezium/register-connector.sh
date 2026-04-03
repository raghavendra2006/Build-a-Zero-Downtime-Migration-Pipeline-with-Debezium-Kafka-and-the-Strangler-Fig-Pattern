#!/bin/bash
# register-connector.sh
# Waits for Kafka Connect REST API to be ready, then registers the
# Debezium PostgreSQL source connector for the legacy_db.

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="legacy-orders-connector"

echo "==> Waiting for Kafka Connect REST API to become available..."

# Poll until the REST API responds
MAX_ATTEMPTS=120
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/" 2>/dev/null || true)
  if [ "$HTTP_CODE" = "200" ]; then
    echo "==> Kafka Connect REST API is ready."
    break
  fi
  ATTEMPT=$((ATTEMPT + 1))
  if [ $((ATTEMPT % 10)) -eq 0 ]; then
    echo "    Attempt $ATTEMPT/$MAX_ATTEMPTS — still waiting..."
  fi
  sleep 3
done

if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
  echo "==> ERROR: Kafka Connect REST API did not become available after $MAX_ATTEMPTS attempts."
  exit 1
fi

# Check if connector already exists
EXISTING=$(curl -s "$CONNECT_URL/connectors" 2>/dev/null || echo "[]")
if echo "$EXISTING" | grep -q "$CONNECTOR_NAME"; then
  echo "==> Connector '$CONNECTOR_NAME' already registered. Skipping."
  exit 0
fi

echo "==> Registering connector: $CONNECTOR_NAME"

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'"$CONNECTOR_NAME"'",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "legacy_db",
      "database.port": "5432",
      "database.user": "'"${DB_USER:-postgres}"'",
      "database.password": "'"${DB_PASSWORD:-postgres}"'",
      "database.dbname": "'"${DB_NAME:-postgres}"'",
      "topic.prefix": "legacyserver",
      "plugin.name": "pgoutput",
      "table.include.list": "public.orders",
      "snapshot.mode": "initial",
      "slot.name": "debezium_orders",
      "publication.name": "dbz_publication"
    }
  }')

HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_STATUS" = "201" ] || [ "$HTTP_STATUS" = "200" ]; then
  echo "==> Connector '$CONNECTOR_NAME' registered successfully (HTTP $HTTP_STATUS)."
else
  echo "==> WARNING: Connector registration returned HTTP $HTTP_STATUS"
  echo "    Response: $BODY"
  exit 1
fi
