#!/usr/bin/env bash
set -euo pipefail

# Runs only when postgres-data is initialized for the first time.
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD is required}"
: "${DWH_DB_PASSWORD:?DWH_DB_PASSWORD is required}"

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set="airflow_password=$AIRFLOW_DB_PASSWORD" \
  --set="dwh_password=$DWH_DB_PASSWORD" \
  --file=/opt/bootstrap/init.sql
