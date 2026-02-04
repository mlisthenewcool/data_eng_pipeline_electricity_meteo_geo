#!/usr/bin/env bash
# Custom entrypoint that exports Airflow connections from Docker secrets
# before delegating to the default Airflow entrypoint.
#
# Why: AIRFLOW_CONN_*_CMD is NOT supported by Airflow (only AIRFLOW__*_CMD is).
# Connections via env vars must use AIRFLOW_CONN_<ID>=<URI> directly.
# This script reads Docker secrets at container startup and builds the URI.

set -euo pipefail

SECRETS_DIR="/run/secrets"

if [[ -f "${SECRETS_DIR}/postgres_root_username" && -f "${SECRETS_DIR}/postgres_root_password" ]]; then
    PG_USER="$(cat "${SECRETS_DIR}/postgres_root_username")"
    PG_PASS="$(cat "${SECRETS_DIR}/postgres_root_password")"
    export AIRFLOW_CONN_POSTGRES_PROJET_ENERGIE="postgresql://${PG_USER}:${PG_PASS}@postgres_service:5432/projet_energie"
fi

# Delegate to the default Airflow entrypoint
exec /entrypoint "$@"
