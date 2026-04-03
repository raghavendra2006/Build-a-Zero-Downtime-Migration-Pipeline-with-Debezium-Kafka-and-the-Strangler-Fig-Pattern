#!/bin/bash
# entrypoint.sh — Custom entrypoint for the Debezium container.
# Starts the Kafka Connect worker in the background,
# waits for it to be ready, registers the connector, then waits.

# Start the default Debezium / Kafka Connect entrypoint in background
/docker-entrypoint.sh start &
CONNECT_PID=$!

# Register the connector (script handles its own retries)
/scripts/register-connector.sh || echo "WARNING: Connector registration failed, will retry via health check"

# Keep the container alive by waiting on the main process
wait $CONNECT_PID
