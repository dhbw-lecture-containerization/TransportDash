#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AIRFLOW_DIR="$SCRIPT_DIR/dags"
STREAMLIT_DIR="$SCRIPT_DIR/streamlit"

echo "Building transportdash-airflow:v1 from $AIRFLOW_DIR"
docker build -t transportdash-airflow:v1 "$AIRFLOW_DIR"

echo "Building transportdash-streamlit:v1 from $STREAMLIT_DIR"
docker build -t transportdash-streamlit:v1 "$STREAMLIT_DIR"

echo "Done. Built images:"
echo "- transportdash-airflow:v1"
echo "- transportdash-streamlit:v1"
