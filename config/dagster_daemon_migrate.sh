#!/bin/bash

set -e
# Wait for the PostgreSQL database to be available.

until pg_isready -h "$DAGSTER_PG_HOST" -p "$DAGSTER_PG_PORT" -U "$DAGSTER_PG_USER" -d "$DAGSTER_PG_DB"; do
echo "Waiting for dagster-metadata database to be ready..."
sleep 1
done

echo "Dagster-metadata database is ready. Starting migration..."

# Run the database migration
dagster instance migrate

# Start the Dagster webserver and daemon
dagster-webserver start -h 0.0.0.0 -p 80 -w /dagster_project/config/load_definitions.yaml
