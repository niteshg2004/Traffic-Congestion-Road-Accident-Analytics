#!/usr/bin/env bash
# Creates a virtual environment (.venv) and installs requirements.
set -e
cd "$(dirname "$0")/.."
command -v java >/dev/null || { echo "Java not found. Install Java 11 or 17 first."; exit 1; }
PY=$(command -v python3.11 || command -v python3.10 || command -v python3)
echo "Using $($PY --version)"
$PY -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
echo "Setup complete. Next: scripts/run_project.sh --sample   (smoke test)  or place the dataset in data/raw/"
