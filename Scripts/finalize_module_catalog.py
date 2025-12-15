#!/usr/bin/env python3
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

def split_context(ctx: str):
    """
    Expect a '>'-separated path like:
      CSD>Document Library>PALCO POWER SYSTEMS>ISM>01_Service Order Process
      CSD>Document Library>SALES>02_Quote to Cash
    Rules:
      - If segment[2] == "PALCO POWER SYSTEMS" => Entity="PALCO", SOP=seg[3], SubSOP=seg[4] (if exists)
      - Else Entity="SE", SOP=seg[2], SubSOP=seg[3] (if exists)
    Returns (Entity, SOP, SubSOP)
    """
    if not isinstance(ctx, str):
        return (None, None, None)

    seg = [s.strip() for s in ctx.split(">") if str(s).strip() != ""]
    if len(seg) < 2:
        return (None, None, None)

    # Expect starts with "CSD", "Document Library"
    # We’ll be tolerant: find indices in a robust way
    # Try to find the "Document Library" anchor, entity comes after that.
    try:
        anchor = next(i for i, s in enumerate(seg) if s.lower() == "document library")
    except StopIteration:
        # no anchor found; give best-effort guess
        if len(seg) >= 3 and seg[2].upper() == "PALCO POWER SYSTEMS":
            entity = "PALCO"
            sop = seg[3] if len(seg) > 3 else None
            subsop = seg[4] if len(seg) > 4 else None
            return (entity, sop, subsop)
        elif len(seg) >= 3:
            entity = "SE"
            sop = seg[2]
            subsop = seg[3] if len(seg) > 3 else None
            return (entity, sop, subsop)
        else:
            return (None, None, None)

    # position after the anchor is where entity (or PALCO POWER SYSTEMS) appears
    after = anchor + 1
    if after >= len(seg):
        return (None, None, None)

    if seg[after].upper() == "PALCO POWER SYSTEMS":
        entity = "PALCO"
        sop = seg[after + 1] if (after + 1) < len(seg) else None
        subsop = seg[after + 2] if (after + 2) < len(seg) else None
    else:
        entity = "SE"
        sop = seg[after] if after < len(seg) else None
        subsop = seg[after + 1] if (after + 1) < len(seg) else None

    return (entity, sop, subsop)

def choose_module(row):
    # Prefer UAP-side normalized module if present
    for c in ["UAP_Module", "UAP_Mod", "Module", "Doc_Name", "Name"]:
        if c in row and pd.notna(row[c]) and str(row[c]).strip():
            return str(row[c]).strip()
    return None

def main():
    ap = argparse.ArgumentParser(
        description="Finalize module catalog (Module, Entity, SOP, SubSOP) from a curated worklist."
    )
    ap.add_argument("--worklist", required=True,
                    help="Path to FindModuleContext_Worklist_Resp_*.csv (with Final_Context).")
    ap.add_argument("--outdir", default="Outputs/ModMstr",
                    help="Output directory for catalog CSVs (default: Outputs/ModMstr).")
    args = ap.parse_args()

    worklist_path = Path(args.worklist)
    if not worklist_path.exists():
        raise FileNotFoundError(f"Worklist not found: {worklist_path}")

    df = pd.read_csv(worklist_path)

    # We’ll use Final_Context when present else ParentContextRaw
    ctx_col = None
    if "Final_Context" in df.columns:
        ctx_col = "Final_Context"
    elif "ParentContextRaw" in df.columns:
        ctx_col = "ParentContextRaw"

    if ctx_col is None:
        raise KeyError("Expected a 'Final_Context' or 'ParentContextRaw' column in the worklist.")

    # Ensure we have something to call Module
    # Try to map common column names
    # Keep original doc columns for traceability
    possible_module_cols = ["UAP_Module", "UAP_Mod", "Module", "Doc_Name", "Name"]
    for c in possible_module_cols:
        if c not in df.columns:
            df[c] = None  # create if missing so choose_module won't KeyError

    # Build base catalog rows
    rows = []
    for _, r in df.iterrows():
        ctx = r.get(ctx_col, None)
        entity, sop, subsop = split_context(ctx)
        module = choose_module(r)
        rows.append({
            "SourceFile": worklist_path.name,
            "Module": module,
            "Entity": entity,
            "SOP": sop,
            "SubSOP": subsop,
            "FinalContext": ctx,
            "UAP_Module": r.get("UAP_Module", None),
            "Doc_Name": r.get("Doc_Name", None),
            "Doc_File_Name": r.get("Doc_File_Name", None),
            "Doc_Parent_Name": r.get("Doc_Parent_Name", None),
        })

    cat = pd.DataFrame(rows)

    # Basic cleanup
    for c in ["Module", "Entity", "SOP", "SubSOP"]:
        if c in cat.columns:
            cat[c] = cat[c].astype(str).str.strip().replace({"None": None, "nan": None})

    # Deduplicate on Module keeping first non-null mapping
    cat = (
        cat.sort_values(["Module"])
           .drop_duplicates(subset=["Module"], keep="first")
           .reset_index(drop=True)
    )

    # Emit files
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    out_catalog = outdir / f"module_catalog_{ts}.csv"
    cat.to_csv(out_catalog, index=False, encoding="utf-8")

    # Unmapped for review
    unmapped = cat[(cat["Entity"].isna()) | (cat["SOP"].isna())]
    out_unmapped = outdir / f"module_catalog_unmapped_{ts}.csv"
    unmapped.to_csv(out_unmapped, index=False, encoding="utf-8")

    print(f"[INFO] Wrote catalog:   {out_catalog}")
    print(f"[INFO] Wrote unmapped:  {out_unmapped}")
    print(f"[SUMMARY] Total modules: {len(cat)} | unmapped: {len(unmapped)}")

if __name__ == "__main__":
    main()
