#!/bin/bash
# entrypoint.sh — Custom entrypoint for the Debezium container.
# Starts the Kafka Connect worker in the background,
# registers the connector, then waits for the worker.

# Start the default Debezium / Kafka Connect entrypoint in background
/docker-entrypoint.sh start &
CONNECT_PID=$!

# Wait a moment for the process to initialize, then register the connector
/scripts/register-connector.sh

# Keep the container alive by waiting on the main process
wait $CONNECT_PID
