#!/usr/bin/env bash
# DuckDB and ClickHouse over the query set: load once, then query RUNS times.
#
#   S3_SECRET=... ./scripts/run_engines.sh ~/ssb_synth 2>&1 | tee ~/engines.log
#
# The database files go on real disk, not /tmp: the fact table does not fit in
# a small tmpfs. Override with ENGINE_DIR.

set -uo pipefail

DATA="${1:-$HOME/ssb_synth}"
RUNS="${RUNS:-5}"
ENGINE_DIR="${ENGINE_DIR:-$HOME/engine-data}"
HERE="$(cd "$(dirname "$0")" && pwd)"
SQL="$(dirname "$HERE")/sql"
mkdir -p "$ENGINE_DIR"

S3_ENDPOINT="${S3_ENDPOINT:-http://192.168.1.37:9010}"
S3_KEY="${S3_KEY:-minioadmin}"

# ---- DuckDB -----------------------------------------------------------------
DUCK="${DUCKDB:-}"
[[ -z "$DUCK" ]] && command -v duckdb >/dev/null 2>&1 && DUCK="duckdb"
[[ -z "$DUCK" && -x "$HOME/.duckdb/cli/latest/duckdb" ]] && DUCK="$HOME/.duckdb/cli/latest/duckdb"
[[ -z "$DUCK" && -x "$HOME/.local/bin/duckdb" ]] && DUCK="$HOME/.local/bin/duckdb"

if [[ -n "$DUCK" ]]; then
  DB="$ENGINE_DIR/ssb.duckdb"
  echo "===== DuckDB  (db: $DB) ====="
  rm -f "$DB"
  echo "--- loading ---"
  sed "s|__DATA__|$DATA|g" "$SQL/duckdb_load.sql" | "$DUCK" "$DB"
  echo "--- verification ---"
  "$DUCK" "$DB" < "$SQL/duckdb_verify.sql"
  # All iterations inside a single invocation: a fresh process would start with
  # cold engine caches on every run, which would penalise the engine relative to
  # the Spark configurations, where the process stays warm across iterations.
  REP=$(mktemp -p "$ENGINE_DIR")
  for r in $(seq 1 "$RUNS"); do
    echo "SELECT '----- duckdb run $r/$RUNS -----' AS marker;" >> "$REP"
    cat "$SQL/duckdb_queries.sql" >> "$REP"
  done
  "$DUCK" "$DB" < "$REP"
  rm -f "$REP"
else
  echo "SKIP DuckDB: binary not found."
  echo "  install:  curl https://install.duckdb.org | sh"
fi

# ---- ClickHouse -------------------------------------------------------------
# ClickHouse runs through `clickhouse local`, which executes SQL without a
# server process. It uses the same engine and supports MergeTree tables in a
# working directory, so the measurement is equivalent to a server deployment
# while avoiding a port conflict with anything already bound to 9000.
CHBIN="${CLICKHOUSE:-}"
[[ -z "$CHBIN" ]] && command -v clickhouse >/dev/null 2>&1 && CHBIN="clickhouse"
[[ -z "$CHBIN" && -x "$HOME/.local/bin/clickhouse" ]] && CHBIN="$HOME/.local/bin/clickhouse"
[[ -z "$CHBIN" && -x ./clickhouse ]] && CHBIN="./clickhouse"

CH=""
[[ -n "$CHBIN" ]] && CH="$CHBIN local --path $ENGINE_DIR/ch"

if [[ -z "$CH" ]]; then
  echo "SKIP ClickHouse: binary not found."
  echo "  install:  curl https://clickhouse.com/ | sh     (single binary, no root)"
elif [[ -z "${S3_SECRET:-}" ]]; then
  echo "SKIP ClickHouse: set S3_SECRET (tables are loaded from the object store)."
else
  echo "===== ClickHouse ====="
  mkdir -p "$ENGINE_DIR/ch"
  TMP=$(mktemp -p "$ENGINE_DIR")
  sed -e "s|__ENDPOINT__|$S3_ENDPOINT|g" -e "s|__KEY__|$S3_KEY|g" \
      -e "s|__SECRET__|$S3_SECRET|g" "$SQL/clickhouse_load.sql" > "$TMP"
  echo "--- loading ---"
  $CH --multiquery < "$TMP"
  echo "--- verification ---"
  $CH --multiquery < "$SQL/clickhouse_verify.sql"
  # Same reasoning as above: one invocation, iterations repeated inside it.
  REP=$(mktemp -p "$ENGINE_DIR")
  for r in $(seq 1 "$RUNS"); do
    echo "SELECT '----- clickhouse run $r/$RUNS -----';" >> "$REP"
    cat "$SQL/clickhouse_queries.sql" >> "$REP"
  done
  $CH --time --multiquery < "$REP"
  rm -f "$TMP" "$REP"
fi
