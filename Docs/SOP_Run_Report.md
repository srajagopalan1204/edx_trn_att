# SOP: Training Attainment Report Run

You are the operator. You run this. You hand off results.

## 1. Prepare inputs
- Export latest training attempts CSV from the system.
- Save it under `Inputs/` as `SR04-Trn_Att_YYYYMMDD_HHMM.csv`.

## 2. Update requirement map (optional right now)
- Open `Config/role_module_map.xlsx`.
- For each role or department, mark each Module_Name with:
  - R = required
  - N = nice to have
  - Z = not in role

## 3. Generate report
In Codespaces terminal:
```bash
python Scripts/train_report.py
```

## 4. Output
The script will create a timestamped Excel file in `Outputs/`, e.g.:
`Training_Attempts_Report_20251029_1530.xlsx`

Sheets in that workbook:
- `Report_Card`      Per-user per-module attempts, scores, pass status.
- `Exceptions`       Who needs coaching / not passed / low scores.
- `Last_Active`      Last training touch by each user.
- `Module_Risk`      Module health: conversion, attempts-to-pass, time-to-pass.
- `Requirement_Matrix` (placeholder) Role-based R/N/Z grid.
- `Source_Columns`   Traceability of source columns.

## 5. Session checklist
- The script also writes a session run checklist HTML under `Session_Checklists/`.
- You'll see `runcheck_latest.html` plus a timestamped archive.
- Download that HTML or just open it in Codespaces Preview to resume work next session.

## 6. Hand off and stop
- Send the Excel (Outputs/*.xlsx) to management / supervisors.
- Save the session checklist if needed.
- Close for the day. You are allowed to have a life.
