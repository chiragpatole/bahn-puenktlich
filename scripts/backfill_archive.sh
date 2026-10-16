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
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# fs cp does not create missing folders in a volume
databricks fs mkdir "$DEST"

# Large uploads occasionally drop mid-transfer ("broken pipe"), so try a few times
upload() {
  local attempt
  for attempt in 1 2 3 4; do
    if databricks fs cp "$1" "$2" --overwrite; then
      return 0
    fi
    echo "upload attempt $attempt failed, retrying in $((attempt * 15))s" >&2
    sleep $((attempt * 15))
  done
  echo "giving up on $1" >&2
  return 1
}

for month in "$@"; do
  file="data-${month}.parquet"
  parts_dir="$TMP_DIR/data-${month}"
  echo "downloading $file"
  curl -fL --retry 3 --progress-bar -o "$TMP_DIR/$file" "$BASE_URL/$file"
  echo "converting timestamps and splitting into parts"
  python "$SCRIPT_DIR/convert_timestamps.py" "$TMP_DIR/$file" "$parts_dir"
  rm "$TMP_DIR/$file"

  # one folder per month; clear out an older single-file upload of the same month
  databricks fs rm "$DEST/$file" >/dev/null 2>&1 || true
  databricks fs mkdir "$DEST/data-${month}"
  for part in "$parts_dir"/part-*.parquet; do
    echo "uploading data-${month}/$(basename "$part")"
    upload "$part" "$DEST/data-${month}/$(basename "$part")"
  done
  rm -rf "$parts_dir"
done
echo "done"
