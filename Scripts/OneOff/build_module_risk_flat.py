#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_module_risk_flat.py

Create a single flat CSV summarizing module risk metrics, enriched with Entity/SOP/SubSOP
from a module catalog. Ideal as the one tab you can pivot freely.

Inputs:
  --uap       Path to UAP attempts CSV (e.g., Inputs/SR04-Trn_Att_v2_*.csv)
              Must contain at least: "User.Full Name", "Version Name", "Start Time"
              Optional: "Result" (for pass flag), "Score"
  --catalog   Path to module catalog CSV with: "Module","Entity","SOP","SubSOP"
              (typically Outputs/ModMstr/module_catalog_*.csv)
  --out       Path for the flat CSV to write
Options:
  --dnr                  Path to DNR CSV (if present, names found here are excluded)
  --pass-string          Text used in UAP "Result" to mark a pass (default: "passed")
  --unmapped-out         Optional CSV to list modules seen in UAP that have no catalog row
  --print-summary        If set, prints totals to stdout

Example:
  python Scripts/build_module_risk_flat.py \
    --uap Inputs/SR04-Trn_Att_v2_20251104_1330.csv \
    --catalog Outputs/ModMstr/module_catalog_20251104_2117.csv \
    --out Outputs/Module_Risk_Flat_$(date +%Y%m%d_%H%M).csv \
    --dnr Outputs/DNR_110325_1914.csv \
    --unmapped-out Outputs/QA/unmapped_modules_$(date +%Y%m%d_%H%M).csv \
    --print-summary
"""
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime
from typing import Tuple, List

import numpy as np
import pandas as pd


# ---------------------------
# Helpers
# ---------------------------
def _norm_str(s: str) -> str:
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    return " ".join(str(s).strip().lower().split())

def _norm_mod(s: str) -> str:
    """Normalize module names for joining (lower + trim + collapse spaces)."""
    return _norm_str(s)

def _ensure_cols(df: pd.DataFrame, cols: List[str], msg: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{msg} missing columns: {missing}")


# ---------------------------
# Data loaders
# ---------------------------
def load_uap_attempts(path: Path, pass_string: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"UAP file not found: {path}")
    df = pd.read_csv(path)

    _ensure_cols(df, ["User.Full Name", "Version Name", "Start Time"],
                 "UAP attempts")

    # timestamps, scores, pass flag
    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce")
    if "Score" in df.columns:
        df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
        df["Score_valid"] = df["Score"].where(df["Score"] >= 0, np.nan)
    else:
        df["Score_valid"] = np.nan

    if "Result" in df.columns:
        df["Result_norm"] = df["Result"].astype(str).str.lower().str.strip()
        df["Passed_Flag"] = df["Result_norm"].eq(pass_string.lower())
    else:
        df["Passed_Flag"] = False

    # normalized join keys
    df["NormModule"] = df["Version Name"].apply(_norm_mod)
    df["NormUser"] = df["User.Full Name"].astype(str).map(_norm_str)
    return df


def load_catalog(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Catalog file not found: {path}")
    cat = pd.read_csv(path)
    _ensure_cols(cat, ["Module", "Entity", "SOP", "SubSOP"], "Catalog")

    cat = cat.copy()
    cat["NormModule"] = cat["Module"].map(_norm_mod)
    # de-dupe on normalized module; keep first mapping
    cat = cat.drop_duplicates(subset=["NormModule"], keep="first")
    return cat[["NormModule", "Module", "Entity", "SOP", "SubSOP"]]


def load_dnr(path: Path) -> set[str]:
    if not path:
        return set()
    p = Path(path)
    if not p.exists():
        return set()
    dnr = pd.read_csv(p)
    if "User.Full Name" not in dnr.columns:
        # accept a few alternatives if user pasted a different header
        candidates = [c for c in dnr.columns if c.lower().replace(".", "").replace(" ", "") in
                      ("userfullname", "name", "employee", "emp_name")]
        if not candidates:
            return set()
        col = candidates[0]
    else:
        col = "User.Full Name"
    return set(dnr[col].astype(str).map(_norm_str).tolist())


# ---------------------------
# Metrics
# ---------------------------
def module_metrics(g: pd.DataFrame) -> dict:
    """Compute risk metrics for a module (group slice of UAP rows)."""
    user_col = "User.Full Name"

    users_opened = set(g[user_col].dropna().unique().tolist())
    succeeded_users = set(g.loc[g["Passed_Flag"], user_col].dropna().unique().tolist())
    passing_scores = g.loc[g["Passed_Flag"], "Score_valid"].dropna().tolist()

    att_to_pass, hrs_to_pass = [], []
    # per-user path to first pass
    for _, gu in g.groupby(user_col):
        gu = gu.sort_values("Start Time").reset_index(drop=True)
        if gu.empty:
            continue
        first_t = gu["Start Time"].min()
        pass_idx = gu.index[gu["Passed_Flag"] == True].tolist()
        if pass_idx:
            i = pass_idx[0]
            pass_t = gu.loc[i, "Start Time"]
            att_to_pass.append(i + 1)
            if pd.notna(pass_t) and pd.notna(first_t):
                hrs_to_pass.append((pass_t - first_t).total_seconds() / 3600.0)

    def stats(lst):
        if not lst:
            return (np.nan, np.nan, np.nan)
        return (min(lst), float(np.mean(lst)), max(lst))

    att_min, att_avg, att_max = stats(att_to_pass)
    tmin, tavg, tmax = stats(hrs_to_pass)
    smin, savg, smax = stats(passing_scores)

    opened = len(users_opened)
    succ = len(succeeded_users)
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


# ---------------------------
# Main builder
# ---------------------------
def build_flat(uap_df: pd.DataFrame, catalog_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      flat_df: one row per Module (with Entity/SOP/SubSOP) + risk metrics
      unmapped: NormModule values that were present in UAP but not in catalog
    """
    # Attach catalog (left join: all UAP modules retained)
    merged = (
        uap_df.merge(catalog_df, how="left", on="NormModule", suffixes=("", "_cat"))
    )

    # Compute metrics per normalized module (but keep the original Module label if available)
    rows = []
    for nmod, g in merged.groupby("NormModule", dropna=False):
        # choose label: catalog Module if present else UAP's first "Version Name"
        label = g["Module"].dropna().iloc[0] if g["Module"].notna().any() else g["Version Name"].dropna().iloc[0] if g["Version Name"].notna().any() else ""
        ent  = g["Entity"].dropna().iloc[0] if "Entity" in g.columns and g["Entity"].notna().any() else ""
        sop  = g["SOP"].dropna().iloc[0] if "SOP" in g.columns and g["SOP"].notna().any() else ""
        subs = g["SubSOP"].dropna().iloc[0] if "SubSOP" in g.columns and g["SubSOP"].notna().any() else ""

        core = module_metrics(g)
        rows.append({
            "Module": label,
            "Entity": ent,
            "SOP": sop,
            "SubSOP": subs,
            **core
        })
    flat = pd.DataFrame(rows).sort_values(["Entity", "SOP", "SubSOP", "Module"], na_position="last").reset_index(drop=True)

    # Identify unmapped modules (no Entity/SOP/SubSOP from catalog)
    unmapped = flat[(flat["Entity"].isna()) | (flat["Entity"] == "")]
    return flat, unmapped


def main():
    ap = argparse.ArgumentParser(description="Build a single flat Module Risk CSV enriched with Entity/SOP/SubSOP from a catalog.")
    ap.add_argument("--uap", required=True, help="Path to UAP attempts CSV")
    ap.add_argument("--catalog", required=True, help="Path to module catalog CSV (Module,Entity,SOP,SubSOP)")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--dnr", help="Optional DNR CSV path to exclude names found there")
    ap.add_argument("--pass-string", default="passed", help='UAP "Result" value that means pass (default: "passed")')
    ap.add_argument("--unmapped-out", help="Optional path to write unmapped module list")
    ap.add_argument("--print-summary", action="store_true", help="Print a short summary at the end")
    args = ap.parse_args()

    uap_df = load_uap_attempts(Path(args.uap), pass_string=args.pass_string)

    # DNR exclusion (if provided)
    dnr_names = load_dnr(args.dnr) if args.dnr else set()
    if dnr_names:
        before = len(uap_df)
        uap_df = uap_df[~uap_df["User.Full Name"].astype(str).map(_norm_str).isin(dnr_names)].copy()
        after = len(uap_df)
        print(f"[INFO] DNR applied: removed {before - after} attempt rows for {len(dnr_names)} names.")

    catalog_df = load_catalog(Path(args.catalog))

    flat, unmapped = build_flat(uap_df, catalog_df)

    # Stamp & write
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Add generator metadata columns at the end
    flat = flat.copy()
    flat["Generated_Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    flat["Source_UAP"] = str(Path(args.uap).resolve())
    flat["Source_Catalog"] = str(Path(args.catalog).resolve())

    flat.to_csv(out_path, index=False)
    print(f"[INFO] Wrote flat risk CSV: {out_path}")

    if args.unmapped_out:
        unmapped_out = Path(args.unmapped_out)
        unmapped_out.parent.mkdir(parents=True, exist_ok=True)
        # keep a slim unmapped list (Module label + metrics are less relevant)
        slim = unmapped[["Module"]].copy()
        slim = slim[slim["Module"].astype(str).str.strip() != ""].drop_duplicates()
        slim.to_csv(unmapped_out, index=False)
        print(f"[INFO] Wrote unmapped modules: {unmapped_out}")

    if args.print_summary:
        total_mods = flat.shape[0]
        mapped = total_mods - unmapped.shape[0]
        print(f"[SUMMARY] Modules: {total_mods} | mapped: {mapped} | unmapped: {unmapped.shape[0]}")


if __name__ == "__main__":
    main()
