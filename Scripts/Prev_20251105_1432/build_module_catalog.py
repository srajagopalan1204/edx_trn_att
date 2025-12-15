#!/usr/bin/env python3
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

REQ_COL_NAME = "Name"
REQ_COL_CTX  = "Parent.Context"  # we’ll try a few alternates if this exact name isn't present

ALT_CTX_NAMES = [
    "Parent.Context",
    "Parent Context",
    "Parent Context Name",
    "ParentContext",
    "Parent_Context",
]

def find_ctx_column(cols):
    """Return the best-matching Parent.Context column name or None."""
    norm = {c.strip().lower(): c for c in cols}
    for cand in ALT_CTX_NAMES:
        key = cand.strip().lower()
        if key in norm:
            return norm[key]
    # fuzzy fallback: any column that contains 'parent' and 'context'
    for c in cols:
        cl = c.strip().lower()
        if "parent" in cl and "context" in cl:
            return c
    return None

def parse_entity_sop_subsop(ctx_val: str) -> tuple[str, str, str]:
    """
    Rules:
      - If ctx startswith 'CSD > Document Library > PALCO POWER SYSTEMS' -> Entity=PALCO,
        then SOP = next part, SubSOP = next part (if present)
      - Else if ctx startswith 'CSD > Document Library >' -> Entity=SE,
        then SOP = next part, SubSOP = next part (if present)
      - Else -> ('', '', '')
    """
    if not isinstance(ctx_val, str) or not ctx_val.strip():
        return ("", "", "")

    # Split by '>' and trim each token
    parts_raw = ctx_val.split(">")
    parts = [p.strip() for p in parts_raw if str(p).strip() != ""]
    if not parts:
        return ("", "", "")

    # Build case-insensitive view for matching the prefix
    upper_parts = [p.upper() for p in parts]

    # Match: CSD > Document Library > PALCO POWER SYSTEMS
    if len(upper_parts) >= 3 and upper_parts[0] == "CSD" and upper_parts[1] == "DOCUMENT LIBRARY" and upper_parts[2] == "PALCO POWER SYSTEMS":
        entity = "PALCO"
        # SOP after the third element, SubSOP after SOP (if any)
        sop = parts[3] if len(parts) >= 4 else ""
        subsop = parts[4] if len(parts) >= 5 else ""
        return (entity, sop, subsop)

    # Match: CSD > Document Library > ...
    if len(upper_parts) >= 2 and upper_parts[0] == "CSD" and upper_parts[1] == "DOCUMENT LIBRARY":
        entity = "SE"
        sop = parts[2] if len(parts) >= 3 else ""
        subsop = parts[3] if len(parts) >= 4 else ""
        return (entity, sop, subsop)

    # No known pattern
    return ("", "", "")

def build_catalog(in_path: Path) -> pd.DataFrame:
    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")

    # Load robustly; SR04 often has embedded commas
    df = pd.read_csv(in_path, dtype=str, keep_default_na=False, na_values=[""], engine="python")

    cols = list(df.columns)
    if REQ_COL_NAME not in cols:
        raise KeyError(f"Input is missing required column '{REQ_COL_NAME}'. Found cols: {cols}")

    ctx_col = find_ctx_column(cols)
    if ctx_col is None:
        raise KeyError(f"Could not find a 'Parent.Context' column. Searched among: {cols}")

    # Normalize to strings
    df[REQ_COL_NAME] = df[REQ_COL_NAME].astype(str).str.strip()
    df[ctx_col]      = df[ctx_col].astype(str).str.strip()

    # Map rows -> (Entity, SOP, SubSOP)
    parsed = df[ctx_col].apply(parse_entity_sop_subsop)
    df["_Entity"]  = parsed.apply(lambda t: t[0])
    df["_SOP"]     = parsed.apply(lambda t: t[1])
    df["_SubSOP"]  = parsed.apply(lambda t: t[2])

    # Build catalog frame
    cat = pd.DataFrame({
        "Module":  df[REQ_COL_NAME].fillna("").astype(str).str.strip(),
        "Entity":  df["_Entity"],
        "SOP":     df["_SOP"],
        "SubSOP":  df["_SubSOP"],
    })

    # De-duplicate by Module (keep the first non-blank mapping)
    cat = (
        cat.replace({"Entity": {"nan": ""}, "SOP": {"nan": ""}, "SubSOP": {"nan": ""}})
           .drop_duplicates(subset=["Module"], keep="first")
           .reset_index(drop=True)
    )

    # Add provenance
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    cat["Source_File"] = str(in_path)
    cat["Generated_Timestamp"] = ts

    # Sort nice: Entity/SOP/SubSOP/Module
    cat = cat.sort_values(["Entity", "SOP", "SubSOP", "Module"], na_position="last").reset_index(drop=True)
    return cat

def main():
    ap = argparse.ArgumentParser(description="Build Module → (Entity, SOP, SubSOP) catalog from SR04 Document Tracking Report.")
    ap.add_argument("--in", dest="inp", required=True, help="Path to SR04 Document Tracking Report CSV")
    ap.add_argument("--outdir", default="Outputs/ModMstr", help="Directory to write the catalog CSV (default: Outputs/ModMstr)")
    args = ap.parse_args()

    in_path = Path(args.inp)
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cat = build_catalog(in_path)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = out_dir / f"module_catalog_{ts}.csv"
    cat.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[INFO] Wrote module catalog: {out_path}")
    print(f"[INFO] Rows: {len(cat)} | Sample columns: {list(cat.columns)}")

if __name__ == "__main__":
    main()
