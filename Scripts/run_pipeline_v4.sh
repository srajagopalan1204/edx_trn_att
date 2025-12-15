#!/usr/bin/env bash
set -euo pipefail

UAP="Inputs/SR04-Trn_Att_20251029_0905.csv"
EMP="emp_mstr/All_Employees_New_Hires_Terms_10_29_25.xlsx"
FIND="Transi/FindName.csv"
DNR="Outputs/DNR_110325_1914.csv"
CAT_LATEST="Transi/Module_Catalog_LATEST.csv"
OUTDIR="Outputs"

TS=$(python - <<'PY'
from lib.timeutil_v4 import ts_ny; print(ts_ny())
PY
)

# Optional: rebuild/merge catalog if a fresh one is available
# Example use:
# python Scripts/merge_module_catalog_v4.py --new Outputs/ModMstr/module_catalog_fresh.csv

# Enrich UAP
python Scripts/enrich_uap_with_catalog_v4.py \
  --uap "$UAP" \
  --catalog "$CAT_LATEST" \
  --out "$OUTDIR/UAP_enriched_${TS}.csv" \
  --print-summary \
  --debug-dir "$OUTDIR/QA"

# Full report
python Scripts/build_full_report_v4.py \
  --uap "$UAP" \
  --findname "$FIND" \
  --emp-mstr "$EMP" \
  --dnr "$DNR" \
  --out-xlsx "$OUTDIR/Training_Attempts_Report_${TS}.xlsx"

echo "[DONE] Pipeline v4 complete."
