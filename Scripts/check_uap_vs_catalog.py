#!/usr/bin/env python3
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

def norm_key(s):
    if pd.isna(s):
        return ""
    # collapse whitespace and lowercase
    t = " ".join(str(s).replace("\u00A0", " ").split()).strip().lower()
    return t

def pick_col(df, candidates, required=True, label=""):
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(
            f"Required column not found for {label}. "
            f"Tried: {candidates}. Found: {list(df.columns)}"
        )
    return None

def main():
    ap = argparse.ArgumentParser(
        description="Check UAP Document.Name vs Catalog Module (lower+trim), output match/no-match CSVs."
    )
    ap.add_argument("--uap", required=True, help="Path to UAP attempts CSV (v2) that has Document.Name.")
    ap.add_argument("--catalog", required=True, help="Path to module_catalog_*.csv (has Module, Entity, SOP, SubSOP).")
    ap.add_argument("--outdir", default="Outputs/QA", help="Directory for results (default: Outputs/QA).")
    ap.add_argument("--outfile-stem", default="uap_vs_catalog_check", help="Base name for output files.")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # --- Load UAP ---
    uap_path = Path(args.uap)
    if not uap_path.exists():
        raise FileNotFoundError(f"UAP file not found: {uap_path}")
    uap = pd.read_csv(uap_path)

    # Try to find the UAP document/module column
    uap_doc_col = pick_col(
        uap,
        candidates=["Document.Name", "Document Name", "Name", "Version Name"],
        required=True,
        label="UAP document/module"
    )

    # --- Load Catalog ---
    cat_path = Path(args.catalog)
    if not cat_path.exists():
        raise FileNotFoundError(f"Catalog file not found: {cat_path}")
    cat = pd.read_csv(cat_path)

    # Find columns in catalog
    cat_module_col = pick_col(cat, ["Module", "Name"], required=True, label="Catalog Module")
    cat_entity_col = pick_col(cat, ["Entity"], required=False)
    cat_sop_col    = pick_col(cat, ["SOP"], required=False)
    cat_subsop_col = pick_col(cat, ["SubSOP", "Sub_SOP", "Sub SOP"], required=False)

    # --- Build normalized keys ---
    uap["_UAP_Key"] = uap[uap_doc_col].map(norm_key)
    cat["_CAT_Key"] = cat[cat_module_col].map(norm_key)

    # Keep only necessary catalog columns for merge
    keep_cols = [cat_module_col]
    for c in [cat_entity_col, cat_sop_col, cat_subsop_col]:
        if c:
            keep_cols.append(c)
    keep_cols.append("_CAT_Key")
    cat_small = cat[keep_cols].drop_duplicates("_CAT_Key")

    # --- Merge (left join: UAP -> Catalog) ---
    merged = uap.merge(cat_small, left_on="_UAP_Key", right_on="_CAT_Key", how="left", suffixes=("", "_cat"))

    # Add match flag
    merged["MatchFlag"] = merged["_CAT_Key"].notna().map({True: "MATCH", False: "NO_MATCH"})

    # Add source columns (so every row remembers inputs)
    merged["Source_UAP_File"] = str(uap_path)
    merged["Source_Catalog_File"] = str(cat_path)

    # Friendly output column order
    out_cols_front = [
        "MatchFlag",
        uap_doc_col,
        cat_module_col if cat_module_col in merged.columns else None,
        cat_entity_col if cat_entity_col and cat_entity_col in merged.columns else None,
        cat_sop_col if cat_sop_col and cat_sop_col in merged.columns else None,
        cat_subsop_col if cat_subsop_col and cat_subsop_col in merged.columns else None,
        "Source_UAP_File",
        "Source_Catalog_File",
    ]
    out_cols_front = [c for c in out_cols_front if c]

    # Put the rest afterwards
    rest = [c for c in merged.columns if c not in out_cols_front and not c.startswith("_CAT_") and c != "_UAP_Key"]
    final = merged[out_cols_front + rest]

    # --- Write outputs ---
    out_all = outdir / f"{args.outfile_stem}_full_{ts}.csv"
    final.to_csv(out_all, index=False, encoding="utf-8")
    print(f"[INFO] Wrote full match file: {out_all}")

    unmatched = final.loc[final["MatchFlag"] == "NO_MATCH"].copy()
    out_unmatched = outdir / f"{args.outfile_stem}_unmatched_{ts}.csv"
    unmatched.to_csv(out_unmatched, index=False, encoding="utf-8")
    print(f"[INFO] Wrote unmatched-only file: {out_unmatched}")

    # --- Quick summary ---
    total = len(final)
    matched_n = int((final["MatchFlag"] == "MATCH").sum())
    unmatched_n = total - matched_n
    print(f"[SUMMARY] Total UAP rows: {total} | MATCH: {matched_n} | NO_MATCH: {unmatched_n}")

if __name__ == "__main__":
    main()
