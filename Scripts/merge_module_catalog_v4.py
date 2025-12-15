# Scripts/merge_module_catalog_v4.py
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd
from lib.timeutil_v4 import ts_ny

REQ_COLS = ["Module","Entity","SOP","SubSOP","SourceFile"]

def read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df

def validate_cols(df: pd.DataFrame, label: str):
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"[ERR] {label} missing columns: {missing}")

def merge_fill(prev: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    # Left-join prev onto new, fill blanks in new from prev
    merged = new.merge(prev[["Module","Entity","SOP","SubSOP"]], on="Module", how="left", suffixes=("", "_prev"))
    for col in ["Entity","SOP","SubSOP"]:
        merged[col] = merged[col].mask(merged[col].astype(str).str.strip().ne(""), merged[col]) \
                                 .fillna(merged[f"{col}_prev"])
    merged.drop(columns=[c for c in merged.columns if c.endswith("_prev")], inplace=True, errors="ignore")
    return merged

def main():
    ap = argparse.ArgumentParser(description="Merge new module catalog with previous (grandfather manual edits) and write LATEST.")
    ap.add_argument("--new", required=True, help="Path to a freshly generated module catalog CSV")
    ap.add_argument("--prev-glob", default="Outputs/ModMstr/module_catalog_*.csv", help="Glob to find previous authoritative")
    ap.add_argument("--out-dir", default="Outputs/ModMstr", help="Where to write merged catalog")
    ap.add_argument("--latest", default="Transi/Module_Catalog_LATEST.csv", help="Copy of newest merged")
    args = ap.parse_args()

    new = read_csv(Path(args.new))
    validate_cols(new, "NEW")

    prev_path = Path()
    prev_found = sorted(Path().glob(args.prev_glob))
    prev_df = pd.DataFrame(columns=REQ_COLS)
    if prev_found:
        prev_path = prev_found[-1]
        prev_df = read_csv(prev_path)
        validate_cols(prev_df, "PREV")
        print(f"[INFO] Previous authoritative: {prev_path} ({len(prev_df)} rows)")
    else:
        print("[INFO] No previous catalog found; the new file becomes authoritative.")

    merged = merge_fill(prev_df, new) if len(prev_df) else new.copy()
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"module_catalog_{ts_ny()}.csv"
    merged.to_csv(out_path, index=False)
    print(f"[INFO] Wrote merged authoritative: {out_path} ({len(merged)} rows)")

    latest = Path(args.latest)
    latest.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(latest, index=False)
    print(f"[INFO] Updated LATEST pointer: {latest}")

if __name__ == "__main__":
    main()
