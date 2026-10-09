#!/usr/bin/env bash
# Runs the whole Spark pipeline. Extra args go to src.run_pipeline
#   scripts/run_project.sh            full dataset, all stages
#   scripts/run_project.sh --sample   synthetic smoke test
#   scripts/run_project.sh --stage preprocess
set -e
cd "$(dirname "$0")/.."
[ -f .venv/bin/activate ] && . .venv/bin/activate
python -m src.run_pipeline "$@"
