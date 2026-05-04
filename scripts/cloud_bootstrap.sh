#!/usr/bin/env bash
set -euo pipefail

export TRADE_INSPECTOR_CACHE_DIR="${TRADE_INSPECTOR_CACHE_DIR:-$PWD/.cache/trade_inspector}"
export TRADE_INSPECTOR_OUTPUT_DIR="${TRADE_INSPECTOR_OUTPUT_DIR:-$PWD/dist/remote_jobs}"

mkdir -p "$TRADE_INSPECTOR_CACHE_DIR"
mkdir -p "$TRADE_INSPECTOR_OUTPUT_DIR"

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if ! python -m pip install -r requirements-dev.txt; then
  echo "Warning: requirements-dev.txt installation failed. Continuing with runtime dependencies only."
fi

echo "TRADE_INSPECTOR_CACHE_DIR=$TRADE_INSPECTOR_CACHE_DIR"
echo "TRADE_INSPECTOR_OUTPUT_DIR=$TRADE_INSPECTOR_OUTPUT_DIR"
echo "Cloud bootstrap complete."
