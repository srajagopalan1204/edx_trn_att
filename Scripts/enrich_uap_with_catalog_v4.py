# Scripts/enrich_uap_with_catalog_v4.py
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd
from lib.timeutil_v4 import ts_ny

def norm_mod(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace("-", " ", regex=False)
         .str.replace("_", " ", regex=False)
         .str.replace(r"\s+", " ", regex=True)
         .str.strip()
         .str.lower()
    )

def load_uap(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "Version Name" not in df.columns and "Document.Name" in df.columns:
        df["Version Name"] = df["Document.Name"]
    if "Version Name" not in df.columns:
        raise SystemExit("[ERR] UAP CSV must have 'Version Name' (or Document.Name to alias).")
    df["__key"] = norm_mod(df["Version Name"])
    return df

def load_catalog(path: Path) -> pd.DataFrame:
    cat = pd.read_csv(path)
    for c in ["Module","Entity","SOP","SubSOP"]:
        if c not in cat.columns:
            raise SystemExit(f"[ERR] Catalog missing '{c}'")
    cat["__key"] = norm_mod(cat["Module"])
    cat = cat.drop_duplicates(subset=["__key"])
    return cat

def main():
    ap = argparse.ArgumentParser(description="Attach Entity/SOP/SubSOP to UAP rows using module catalog (normalized match).")
    ap.add_argument("--uap", required=True)
    ap.add_argument("--catalog", default="Transi/Module_Catalog_LATEST.csv")
    ap.add_argument("--out", required=True)
    ap.add_argument("--debug-dir", default="Outputs/QA")
    ap.add_argument("--print-summary", action="store_true")
    args = ap.parse_args()

    uap = load_uap(Path(args.uap))
    cat = load_catalog(Path(args.catalog))

    merged = uap.merge(cat[["__key","Entity","SOP","SubSOP"]], on="__key", how="left")
    matched = merged["Entity"].notna().sum()
    total = len(merged)
    unmatched = total - matched

    Path(args.debug_dir).mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.out, index=False)
    if args.print_summary:
        print(f"[SUMMARY] Rows: {total} | matched: {matched} | unmatched: {unmatched}")
        print("[INFO] Match tried (in order): Version Name -> Catalog.Module (normalized)")

    # write a small unmatched sample
    um = merged.loc[merged["Entity"].isna(), ["Version Name","__key"]].drop_duplicates().head(50)
    um.to_csv(Path(args.debug_dir)/f"enrich_unmatched_{ts_ny()}.csv", index=False)

if __name__ == "__main__":
    main()
