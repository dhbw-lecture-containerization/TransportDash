#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AIRFLOW_DIR="$SCRIPT_DIR/dags"
STREAMLIT_DIR="$SCRIPT_DIR/streamlit"

echo "Pulling postgres base image (postgres:16)"
docker pull postgres:16

echo "Building transportdash-airflow:v2 from $SCRIPT_DIR (Dockerfile: dags/Dockerfile)"
docker build -t transportdash-airflow:v2 -f "$AIRFLOW_DIR/Dockerfile" "$SCRIPT_DIR"

echo "Building transportdash-streamlit:v2 from $SCRIPT_DIR (Dockerfile: streamlit/Dockerfile)"
docker build -t transportdash-streamlit:v2 -f "$STREAMLIT_DIR/Dockerfile" "$SCRIPT_DIR"

echo "Done. Built images:"
echo "- transportdash-airflow:v2"
echo "- transportdash-streamlit:v2"
