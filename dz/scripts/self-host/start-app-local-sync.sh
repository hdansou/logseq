#!/usr/bin/env bash
set -euo pipefail

# Navigate to the logseq root directory
cd "$(dirname "$0")"

# Enable local DB sync
export ENABLE_DB_SYNC_LOCAL=true

echo "Starting Logseq with Local DB Sync enabled..."
echo "ENABLE_DB_SYNC_LOCAL: ${ENABLE_DB_SYNC_LOCAL}"
echo "Local sync server: ws://127.0.0.1:8787/sync/%s"
echo ""

# Start the development server
yarn watch
