#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import sys
import pandas as pd
import numpy as np
from datetime import datetime

REQ_COLS = ["Entity", "SOP", "SubSOP"]

def norm_module(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
         .str.lower()
    )

def derive_out_path(inp: Path, suffix="_upd") -> Path:
    if inp.suffix.lower() in [".xlsx", ".xls"]:
        return inp.with_name(inp.stem + suffix + inp.suffix)
    else:
        # csv / txt
        return inp.with_name(inp.stem + suffix + inp.suffix)

def main():
    ap = argparse.ArgumentParser(
        description="Fill Entity/SOP/SubSOP in a CSV/XLSX using a module catalog CSV."
    )
    ap.add_argument("--catalog", required=True,
                    help="Path to module catalog CSV (must have Module,Entity,SOP,SubSOP).")
    ap.add_argument("--in", dest="inp", required=True,
                    help="Path to file to update (CSV or XLSX).")
    ap.add_argument("--sheet", default="Module_Risk_By_Policy",
                    help="For Excel input: sheet name to patch (default: Module_Risk_By_Policy). Ignored for CSV.")
    ap.add_argument("--out", default=None,
                    help="Optional explicit output path. Default: add _upd before extension next to input.")
    ap.add_argument("--module-col", default=None,
                    help="Optional module column name in the input (auto-detects 'Module' then 'Version Name').")
    ap.add_argument("--force", action="store_true",
                    help="Overwrite existing non-blank Entity/SOP/SubSOP values (default: only fill blanks).")
    args = ap.parse_args()

    catalog_path = Path(args.catalog)
    inp_path = Path(args.inp)
    if not catalog_path.exists():
        sys.exit(f"[ERROR] Catalog not found: {catalog_path}")
    if not inp_path.exists():
        sys.exit(f"[ERROR] Input not found: {inp_path}")

    # --- Load catalog
    cat = pd.read_csv(catalog_path)
    needed = {"Module", "Entity", "SOP", "SubSOP"}
    if not needed.issubset(cat.columns):
        sys.exit(f"[ERROR] Catalog missing columns {sorted(needed - set(cat.columns))}. Found: {list(cat.columns)}")

    cat["_key"] = norm_module(cat["Module"])
    cat_use = cat[["_key", "Entity", "SOP", "SubSOP"]].drop_duplicates()

    # --- Figure output paths
    out_path = Path(args.out) if args.out else derive_out_path(inp_path)
    unmapped_path = out_path.with_name(out_path.stem + "_unmapped.csv")

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    src_note = str(inp_path)

    # --- Helper: patch a dataframe in-place
    def patch_df(df: pd.DataFrame, module_col: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        if module_col not in df.columns:
            raise KeyError(f"Module column '{module_col}' not found in input.")

        # Ensure required cols exist
        for c in REQ_COLS:
            if c not in df.columns:
                df[c] = np.nan

        df["_key"] = norm_module(df[module_col])
        merged = df.merge(cat_use, on="_key", how="left", suffixes=("", "_cat"))

        updates = {}
        for c in REQ_COLS:
            src_col = f"{c}_cat"
            if src_col not in merged.columns:
                continue
            if args.force:
                # overwrite always
                before_nonnull = merged[c].notna().sum()
                merged[c] = merged[src_col].where(merged[src_col].notna(), merged[c])
                after_nonnull = merged[c].notna().sum()
                updates[c] = int(after_nonnull - before_nonnull) if after_nonnull >= before_nonnull else 0
            else:
                # only fill blanks
                mask = merged[c].isna() | (merged[c].astype(str).str.strip() == "")
                merged.loc[mask, c] = merged.loc[mask, src_col]
                updates[c] = int(mask.sum())

        # Build unmapped rows
        unmapped = merged.loc[merged["Entity"].isna() & merged["SOP"].isna() & merged["SubSOP"].isna(), :]
        unmapped = unmapped[[module_col]].drop_duplicates().rename(columns={module_col: "Module_Unmapped"})

        # Add provenance note if it's a CSV write target later—here we just add a column we’ll keep
        merged["Catalog_Source_File"] = str(catalog_path)
        merged["Patched_Timestamp"] = ts

        # Drop helper cols
        keep_cols = [c for c in merged.columns if not c.endswith("_cat")]
        merged = merged[keep_cols].drop(columns=["_key"], errors="ignore")

        return merged, unmapped, updates

    # --- CSV input
    if inp_path.suffix.lower() not in [".xlsx", ".xls"]:
        df = pd.read_csv(inp_path)

        # detect module column if not provided
        module_col = args.module_col
        if not module_col:
            if "Module" in df.columns:
                module_col = "Module"
            elif "Version Name" in df.columns:
                module_col = "Version Name"
            else:
                sys.exit("[ERROR] Could not find a module column. Use --module-col to specify it "
                         "(e.g., 'Module' or 'Version Name').")

        patched, unmapped, updates = patch_df(df.copy(), module_col)
        # Append source note row for CSV
        # (keep data rectangular by adding an info row with NaNs except a small note column)
        if "Patched_Source_File" not in patched.columns:
            patched["Patched_Source_File"] = ""
        patched.loc[len(patched)] = [np.nan]*len(patched.columns)
        patched.loc[patched.index[-1], "Patched_Source_File"] = src_note

        out_path.parent.mkdir(parents=True, exist_ok=True)
        patched.to_csv(out_path, index=False)
        unmapped.to_csv(unmapped_path, index=False)

        print(f"[INFO] Wrote: {out_path}")
        print(f"[INFO] Unmapped modules: {len(unmapped)} -> {unmapped_path}")
        print(f"[INFO] Updates filled (approx.): {updates}")
        return

    # --- Excel input
    xls = pd.ExcelFile(inp_path)
    sheets = {sn: xls.parse(sn) for sn in xls.sheet_names}

    target_sheet = args.sheet
    if target_sheet not in sheets:
        sys.exit(f"[ERROR] Sheet '{target_sheet}' not found in {inp_path}. Sheets: {xls.sheet_names}")

    df = sheets[target_sheet]

    module_col = args.module_col
    if not module_col:
        if "Module" in df.columns:
            module_col = "Module"
        elif "Version Name" in df.columns:
            module_col = "Version Name"
        else:
            sys.exit(f"[ERROR] Could not find a module column in sheet '{target_sheet}'. "
                     f"Use --module-col to specify it.")

    patched, unmapped, updates = patch_df(df.copy(), module_col)

    # write a new workbook with patched target sheet; copy others unchanged
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as w:
        for sn, sdf in sheets.items():
            if sn == target_sheet:
                patched.to_excel(w, sheet_name=sn, index=False)
            else:
                sdf.to_excel(w, sheet_name=sn, index=False)
        # lightweight metadata sheet
        meta = pd.DataFrame({
            "Patched_File": [str(inp_path)],
            "Catalog_File": [str(catalog_path)],
            "Patched_Timestamp": [ts],
            "Patched_Sheet": [target_sheet],
            "Updates_Summary": [str(updates)]
        })
        meta.to_excel(w, sheet_name="Patch_Metadata", index=False)

    unmapped.to_csv(unmapped_path, index=False)
    print(f"[INFO] Wrote: {out_path}")
    print(f"[INFO] Unmapped modules: {len(unmapped)} -> {unmapped_path}")
    print(f"[INFO] Updates filled (approx.): {updates}")

if __name__ == "__main__":
    main()
