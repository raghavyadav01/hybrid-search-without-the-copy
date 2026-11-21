#!/bin/bash
# Runs ON the ES or OS box. Arg: es|os. Same mapping/settings as the laptop run; reads the same S3 files.
set -euxo pipefail
E=$1
cd ~/eval; sudo sysctl -w vm.max_map_count=262144; export ESCI_BIND=0.0.0.0
export ESCI_LAKE_FILES=$HOME/lake_files.txt AWS_REGION=us-west-2
python3 search_baselines.py --engine $E up --heap 16g --mem 56g
python3 search_baselines.py --engine $E index --workers 4 2>&1 | tail -5
python3 search_baselines.py --engine $E timings || true
echo INDEX-DONE
