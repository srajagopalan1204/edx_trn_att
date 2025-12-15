# Training Attainment Pipeline — v4 Summary

## Goals (manager-facing)
- **Readiness at a glance**: pivots by Manager/Group/Location/Role (Attempts, First/Last, MaxScore, Pass).
- **Activity & aging**: Last Active + Days-since buckets to spot quiet users.
- **Module drag**: Module Risk (overall + by Entity/SOP/SubSOP) with conversion, attempts-to-pass, time-to-pass, and success-only average time.

## Key guarantees
- **Catalog grandfathering**: new catalogs only fill blanks from previous authoritative; manual edits are preserved.
- **LATEST pointer**: `Transi/Module_Catalog_LATEST.csv` always points at the newest merged catalog.
- **DNR reactivation**: only rows with `Exclude = 'Y'` are excluded; flipping to `N` re-includes next run.
- **FindName backups**: `Transi/FindName.csv` is auto-archived to `Transi/Prev/FindName_YYYYMMDD_HHMM.csv` every run.
- **Unified timestamps**: America/New_York, `YYYYMMDD_HHMM`.

## Inputs
- UAP attempts CSV (e.g., `Inputs/SR04-Trn_Att_20251029_0905.csv`)
- Employee master (optional)
- `Transi/FindName.csv`
- DNR CSV (with `Exclude` recommended)
- Module catalog (`Transi/Module_Catalog_LATEST.csv`)

## Outputs
- `Outputs/Training_Attempts_Report_YYYYMMDD_HHMM.xlsx`
- QA artifacts under `Outputs/QA/`
- Risk CSVs (when called separately): `Module_Risk_*`

## Run order
1. (Optional) `merge_module_catalog_v4.py` to merge fresh catalog and update LATEST.
2. `enrich_uap_with_catalog_v4.py` to attach Entity/SOP/SubSOP to each UAP row.
3. `build_full_report_v4.py` to emit the Excel workbook.
4. In Excel, run your macros: RC_All, LA_All, MR_All.

## Columns (Module Risk)
- ConversionPct, AttemptsToPass_Min/Avg/Max, TimeToPassHrs_Min/Avg/Max,
- **TimeToPassHrs_Avg_SuccessOnly**, PassScore_Min/Avg/Max,
- UsersOpened, UsersSucceeded, UsersStillNotPassed.
