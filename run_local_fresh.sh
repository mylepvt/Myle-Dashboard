#!/usr/bin/env bash
# Local pe fresh DB + bypass login — koi password mat dalo
set -e
cd "$(dirname "$0")"
rm -f leads.db
export DEV_BYPASS_AUTH=1
echo "Starting app... Open http://localhost:5001 (auto admin, no login)"
exec python app.py
