#!/usr/bin/env bash
# Copy monthly files from the open DB archive on Hugging Face into the volume.
# Needs the Databricks CLI and DATABRICKS_HOST / DATABRICKS_TOKEN in the environment.
#
#   scripts/backfill_archive.sh 2026-05 2026-06 2026-07
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 YYYY-MM [YYYY-MM ...]" >&2
  exit 1
fi

BASE_URL="https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data"
DEST="${ARCHIVE_VOLUME:-dbfs:/Volumes/bahn/bronze/archive/monthly}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

for month in "$@"; do
  file="data-${month}.parquet"
  echo "downloading $file"
  curl -fL --retry 3 --progress-bar -o "$TMP_DIR/$file" "$BASE_URL/$file"
  echo "uploading $file to $DEST"
  databricks fs cp "$TMP_DIR/$file" "$DEST/$file" --overwrite
  rm "$TMP_DIR/$file"
done
echo "done"
