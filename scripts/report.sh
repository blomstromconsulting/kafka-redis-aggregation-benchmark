#!/usr/bin/env bash
# Generates results/report.html, results/summary.csv and docs/RESULTS.md from all saved runs.
source "$(dirname "$0")/lib/common.sh"
python3 "$ROOT/scripts/report.py"
