#!/usr/bin/env bash
# Prints "start_us measure_start_us measure_end_us finished_us" of a run (used by prom_export.py).
source "$(dirname "$0")/common.sh"
psql_exec "SELECT start_us, measure_start_us, measure_end_us, coalesce(finished_us, measure_end_us) FROM runs WHERE run_id = '$1'" | tr '|' ' '
