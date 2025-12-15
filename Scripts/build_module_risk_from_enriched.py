#!/usr/bin/env python3
"""
Build Module Risk (overall and by Entity/SOP/SubSOP) from the *enriched* UAP CSV.

Inputs:
  --enriched : CSV from enrich_uap_with_catalog.py (must have Version Name, Start Time, Passed_Flag or Result, Score, and Entity/SOP/SubSOP)
  --out-dir  : output folder (default: Outputs)
  --excel    : also emit a single XLSX with both tabs (default: off)

Outputs:
  Module_Risk_From_Enriched_<TS>.csv
  Module_Risk_By_Policy_From_Enriched_<TS>.csv
  (optional) QA/unmatched_from_enriched_<TS>.csv if Match_Status exists and has NO_MATCH

Usage:
  python Scripts/build_module_risk_from_enriched.py \
    --enriched Outputs/UAP_enriched_20251105_1417.csv \
    --out-dir Outputs --excel
"""

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

REQ_COLS = ["Version Name", "Start Time"]
POLICY_COLS = ["Entity", "SOP", "SubSOP"]

def _to_dt(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    try:
        return dt.dt.tz_localize(None)
    except Exception:
        return dt

def _ensure_pass_score(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Passed_Flag" not in out.columns:
        if "Result" in out.columns:
            out["Passed_Flag"] = out["Result"].astype(str).str.lower().str.strip().eq("passed")
        else:
            out["Passed_Flag"] = False
    if "Score_valid" not in out.columns:
        if "Score" in out.columns:
            out["Score_valid"] = pd.to_numeric(out["Score"], errors="coerce")
        else:
            out["Score_valid"] = np.nan
    return out

def _core_metrics(g: pd.DataFrame) -> dict:
    user_col = "User.Full Name" if "User.Full Name" in g.columns else "User"
    users_opened = set(g[user_col].dropna().unique().tolist())
    succeeded = set(g.loc[g["Passed_Flag"] == True, user_col].dropna().unique().tolist())
    passing_scores = g.loc[g["Passed_Flag"] == True, "Score_valid"].dropna().tolist()

    att_to_pass, hrs_to_pass = [], []
    for u, gu in g.groupby(user_col, dropna=True):
        gu = gu.sort_values("Start Time").reset_index(drop=True)
        if gu.empty:
            continue
        first_t = gu["Start Time"].min()
        idxs = gu.index[gu["Passed_Flag"] == True].tolist()
        if idxs:
            i = idxs[0]
            pass_t = gu.loc[i, "Start Time"]
            att_to_pass.append(i + 1)
            hrs_to_pass.append((pass_t - first_t).total_seconds() / 3600.0)

    def stats(lst):
        if not lst:
            return (np.nan, np.nan, np.nan)
        return (min(lst), float(np.mean(lst)), max(lst))

    att_min, att_avg, att_max = stats(att_to_pass)
    tmin, tavg, tmax = stats(hrs_to_pass)
    smin, savg, smax = stats(passing_scores)

    opened = len(users_opened)
    succ = len(succeeded)
    conv = 100.0 * succ / opened if opened else np.nan

    return {
        "UsersOpened": opened,
        "UsersSucceeded": succ,
        "UsersStillNotPassed": (opened - succ) if opened else np.nan,
        "ConversionPct": conv,
        "AttemptsToPass_Min": att_min,
        "AttemptsToPass_Avg": att_avg,
        "AttemptsToPass_Max": att_max,
        "TimeToPassHrs_Min": tmin,
        "TimeToPassHrs_Avg": tavg,
        "TimeToPassHrs_Max": tmax,
        "PassScore_Min": smin,
        "PassScore_Avg": savg,
        "PassScore_Max": smax,
    }

def build_module_risk(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for mod, g in df.groupby("Version Name"):
        core = _core_metrics(g)
        rows.append({"Module": mod, **core})
    return pd.DataFrame(rows).sort_values("Module").reset_index(drop=True)

def build_module_risk_by_policy(df: pd.DataFrame) -> pd.DataFrame:
    # only if Entity/SOP/SubSOP present
    if not set(POLICY_COLS).issubset(df.columns):
        return pd.DataFrame(columns=["Module"] + POLICY_COLS)
    rows = []
    keys = ["Version Name"] + POLICY_COLS
    tmp = df.copy()
    tmp["Module"] = tmp["Version Name"]
    for _keys, g in tmp.groupby(["Module"] + POLICY_COLS, dropna=False):
        core = _core_metrics(g)
        rec = {"Module": _keys[0], "Entity": _keys[1], "SOP": _keys[2], "SubSOP": _keys[3], **core}
        rows.append(rec)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["Entity", "SOP", "SubSOP", "Module"], na_position="last").reset_index(drop=True)
    return out

def main():
    ap = argparse.ArgumentParser(description="Build Module Risk from enriched UAP CSV.")
    ap.add_argument("--enriched", required=True, help="Path to Outputs/UAP_enriched_*.csv")
    ap.add_argument("--out-dir", default="Outputs", help="Directory to write outputs")
    ap.add_argument("--excel", action="store_true", help="Also write a single XLSX with both tabs")
    args = ap.parse_args()

    enr_path = Path(args.enriched)
    if not enr_path.exists():
        raise FileNotFoundError(enr_path)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(enr_path)
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise KeyError(f"Enriched file missing required columns: {missing}")

    # Parse and normalize
    df["Start Time"] = _to_dt(df["Start Time"])
    df = _ensure_pass_score(df)

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    qa_dir = out_dir / "QA"
    qa_dir.mkdir(parents=True, exist_ok=True)

    # Optional: dump unmatched from enrichment if present
    if "Match_Status" in df.columns:
        un = df.loc[df["Match_Status"].astype(str).str.upper().eq("NO_MATCH")].copy()
        if not un.empty:
            un_out = qa_dir / f"unmatched_from_enriched_{ts}.csv"
            un.to_csv(un_out, index=False)
            print(f"[INFO] Wrote unmatched snapshot: {un_out}")

    # Build risk tables
    risk = build_module_risk(df)
    risk_path = out_dir / f"Module_Risk_From_Enriched_{ts}.csv"
    risk.to_csv(risk_path, index=False)
    print(f"[INFO] Wrote: {risk_path}")

    risk_policy = build_module_risk_by_policy(df)
    rp_path = out_dir / f"Module_Risk_By_Policy_From_Enriched_{ts}.csv"
    risk_policy.to_csv(rp_path, index=False)
    print(f"[INFO] Wrote: {rp_path}")

    # Optional Excel bundle
    if args.excel:
        xlsx_path = out_dir / f"Module_Risk_Package_{ts}.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as w:
            risk.to_excel(w, sheet_name="Module_Risk", index=False)
            risk_policy.to_excel(w, sheet_name="Module_Risk_By_Policy", index=False)
        print(f"[INFO] Wrote: {xlsx_path}")

    # Summary
    print(f"[SUMMARY] Rows: {len(df):,} | Modules (overall): {risk.shape[0]} | By-Policy rows: {risk_policy.shape[0]}")

if __name__ == "__main__":
    main()
