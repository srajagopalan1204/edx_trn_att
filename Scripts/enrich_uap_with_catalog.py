#!/usr/bin/env python3
# Scripts/enrich_uap_with_catalog.py
import argparse
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

def load_csv_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    return pd.read_csv(path)

# --- Normalization helpers ----------------------------------------------------

_ws_re = re.compile(r"\s+")
_leading_num_re = re.compile(r"^\s*\d+\s*[-_\.]?\s*")
_punct_to_space_re = re.compile(r"[-_]+")

def norm_core(s: str) -> str:
    """
    Normalize a module name for matching:
    - lowercase
    - replace '-' and '_' with spaces
    - remove leading numbering like '01-', '02_', '7.' etc
    - collapse spaces
    """
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = _punct_to_space_re.sub(" ", s)
    s = _leading_num_re.sub("", s)
    s = _ws_re.sub(" ", s).strip()
    return s

def norm_nospace(s: str) -> str:
    return norm_core(s).replace(" ", "")

def build_match_keys(series: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({"raw": series.fillna("")})
    df["key_core"] = df["raw"].map(norm_core)
    df["key_nospace"] = df["raw"].map(norm_nospace)
    return df

# --- Matching -----------------------------------------------------------------

def left_join_on_keys(uap_df: pd.DataFrame, cat_df: pd.DataFrame, left_cols, cat_key_col="Module"):
    """
    Try multiple left columns (in order) against catalog key, using both
    core and nospace variants. Returns (merged, match_source_col, match_mode_col).
    """
    # Catalog keys
    cat_keys = build_match_keys(cat_df[cat_key_col])
    cat = cat_df.copy()
    cat["_cat_key_core"] = cat_keys["key_core"]
    cat["_cat_key_nospace"] = cat_keys["key_nospace"]

    merged = uap_df.copy()
    merged["_match_source"] = ""
    merged["_match_mode"] = ""
    merged["_match_key"] = ""

    for src_col in left_cols:
        if src_col not in merged.columns:
            continue
        keys = build_match_keys(merged[src_col])
        # Try core
        tmp = merged.merge(
            cat[["_cat_key_core", cat_key_col, "Entity", "SOP", "SubSOP"]],
            left_on=keys["key_core"], right_on=cat["_cat_key_core"], how="left", suffixes=("", "_cat")
        )
        hit = tmp[cat_key_col].notna() & (merged["_match_source"] == "")
        merged.loc[hit, ["Entity", "SOP", "SubSOP"]] = tmp.loc[hit, ["Entity", "SOP", "SubSOP"]].values
        merged.loc[hit, "_match_source"] = src_col
        merged.loc[hit, "_match_mode"] = "core"
        merged.loc[hit, "_match_key"] = keys["key_core"][hit]

        # Try nospace (only for those still not matched)
        still = (merged["_match_source"] == "")
        if still.any():
            tmp2 = merged.merge(
                cat[["_cat_key_nospace", cat_key_col, "Entity", "SOP", "SubSOP"]],
                left_on=keys["key_nospace"], right_on=cat["_cat_key_nospace"], how="left", suffixes=("", "_cat2")
            )
            hit2 = tmp2[cat_key_col].notna() & (merged["_match_source"] == "")
            merged.loc[hit2, ["Entity", "SOP", "SubSOP"]] = tmp2.loc[hit2, ["Entity", "SOP", "SubSOP"]].values
            merged.loc[hit2, "_match_source"] = src_col
            merged.loc[hit2, "_match_mode"] = "nospace"
            merged.loc[hit2, "_match_key"] = keys["key_nospace"][hit2]

    merged["_match_flag"] = (merged["_match_source"] != "")
    return merged

# --- Main ---------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Enrich UAP attempts with Entity/SOP/SubSOP from the module catalog.")
    ap.add_argument("--uap", required=True, help="Path to UAP attempts CSV/XLSX")
    ap.add_argument("--catalog", required=True, help="Path to module_catalog_*.csv/xlsx (must have Module,Entity,SOP,SubSOP)")
    ap.add_argument("--out", required=True, help="Output enriched CSV")
    ap.add_argument("--debug-dir", help="Optional dir to write debug key dumps")
    ap.add_argument("--use-file-stem", action="store_true",
                    help="Also try matching on Document.File Name (stem without extension)")
    ap.add_argument("--print-summary", action="store_true")
    args = ap.parse_args()

    uap_path = Path(args.uap)
    cat_path = Path(args.catalog)
    out_path = Path(args.out)
    dbg_dir = Path(args.debug_dir) if args.debug_dir else None
    if dbg_dir:
        dbg_dir.mkdir(parents=True, exist_ok=True)

    uap = load_csv_any(uap_path)
    cat = load_csv_any(cat_path)

    # Validate catalog columns
    need_cat = {"Module", "Entity", "SOP", "SubSOP"}
    miss_cat = sorted(list(need_cat - set(cat.columns)))
    if miss_cat:
        raise KeyError(f"Catalog file missing columns: {miss_cat}")

    # Define source columns to try (in order)
    try_cols = []
    if "Version Name" in uap.columns:
        try_cols.append("Version Name")
    if "Document.Name" in uap.columns:
        try_cols.append("Document.Name")
    if args.use_file_stem and "Document.File Name" in uap.columns:
        # create a stem column without extension
        stem = uap["Document.File Name"].astype(str).str.rsplit(".", n=1, expand=True)
        uap["_DocFileStem"] = stem[0]
        try_cols.append("_DocFileStem")

    # Perform matching
    merged = left_join_on_keys(uap, cat, try_cols, cat_key_col="Module")

    # Append stamp line (write as separate “_meta” sheet? For CSV: add footer line)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    merged["_enrich_note"] = f"Enriched with {cat_path.name} at {ts}"

    # Optional debug dumps
    if dbg_dir:
        # Distinct keys
        if "Version Name" in uap.columns:
            build_match_keys(uap["Version Name"]).drop_duplicates().to_csv(dbg_dir / "uap_vn_norm_keys.csv", index=False)
        if "Document.Name" in uap.columns:
            build_match_keys(uap["Document.Name"]).drop_duplicates().to_csv(dbg_dir / "uap_dn_norm_keys.csv", index=False)
        build_match_keys(cat["Module"]).drop_duplicates().to_csv(dbg_dir / "catalog_norm_keys.csv", index=False)

        # Unmatched sample
        um = merged.loc[~merged["_match_flag"]].copy()
        um_cols = [c for c in ["Version Name", "Document.Name", "Document.File Name"] if c in um.columns]
        um[um_cols + ["_match_key", "_match_source", "_match_mode"]].head(200).to_csv(
            dbg_dir / "enrich_unmatched_sample.csv", index=False
        )

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False, encoding="utf-8")
    if args.print_summary:
        total = len(merged)
        matched = int(merged["_match_flag"].sum())
        print(f"[SUMMARY] Rows: {total} | matched: {matched} | unmatched: {total - matched}")
        if try_cols:
            print(f"[INFO] Match tried (in order): {', '.join(try_cols)}")
        by_source = merged.loc[merged["_match_flag"], "_match_source"].value_counts()
        if not by_source.empty:
            print("[INFO] Match by source column:")
            for k, v in by_source.items():
                print(f"  - {k}: {v}")

    print(f"[INFO] Wrote enriched UAP CSV: {out_path}")

if __name__ == "__main__":
    main()
