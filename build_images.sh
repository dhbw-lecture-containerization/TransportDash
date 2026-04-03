#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AIRFLOW_DIR="$SCRIPT_DIR/dags"
STREAMLIT_DIR="$SCRIPT_DIR/streamlit"

echo "Pulling postgres base image (postgres:16)"
docker pull postgres:16

echo "Building transportdash-airflow:latest from $SCRIPT_DIR (Dockerfile: dags/Dockerfile)"
docker build -t transportdash-airflow:latest -f "$AIRFLOW_DIR/Dockerfile" "$SCRIPT_DIR"

echo "Building transportdash-streamlit:latest from $SCRIPT_DIR (Dockerfile: streamlit/Dockerfile)"
docker build -t transportdash-streamlit:latest -f "$STREAMLIT_DIR/Dockerfile" "$SCRIPT_DIR"

echo "Done. Built images:"
echo "- transportdash-airflow:latest"
echo "- transportdash-streamlit:latest"
