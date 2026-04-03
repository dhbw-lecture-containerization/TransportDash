#!/usr/bin/env bash
set -euo pipefail

# Override these with environment variables if needed.
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
STREAMLIT_IMAGE="${STREAMLIT_IMAGE:-ghcr.io/dhbw-lecture-containerization/transportdash-streamlit:latest}"
AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-ghcr.io/dhbw-lecture-containerization/transportdash-airflow:latest}"

IMAGES=(
  "$POSTGRES_IMAGE"
  "$STREAMLIT_IMAGE"
  "$AIRFLOW_IMAGE"
)

echo "Preparing to pull Kubernetes images:"
printf ' - %s\n' "${IMAGES[@]}"

# If credentials are provided, log in to GHCR first.
if [[ -n "${GHCR_USERNAME:-}" && -n "${GHCR_TOKEN:-}" ]]; then
  echo "Logging in to ghcr.io as $GHCR_USERNAME"
  echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin
fi

for image in "${IMAGES[@]}"; do
  echo "Pulling $image"
  if ! docker pull "$image"; then
    echo ""
    echo "Failed to pull: $image"
    if [[ "$image" == ghcr.io/* ]]; then
      echo "This GHCR image may be private. Set GHCR_USERNAME and GHCR_TOKEN and rerun."
      echo "Example: GHCR_USERNAME=<user> GHCR_TOKEN=<token> ./pull_images.sh"
    fi
    exit 1
  fi
done

echo "All images pulled successfully."