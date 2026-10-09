#!/usr/bin/env bash
# Starts the Streamlit dashboard (add --sample to view the synthetic smoke-test results)
set -e
cd "$(dirname "$0")/.."
[ -f .venv/bin/activate ] && . .venv/bin/activate
[ "$1" = "--sample" ] && export CA3_SAMPLE=1
streamlit run dashboard/app.py
