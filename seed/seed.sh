#!/bin/sh
set -eu

DB_PATH="${DB_PATH:-/data/encounters.db}"

rm -f "$DB_PATH"
sqlite3 "$DB_PATH" <<'SQL'
.mode csv
.import /seed/encounters.csv encounters
SQL
chmod 666 "$DB_PATH"

echo "Seeded $DB_PATH with table 'encounters'"
