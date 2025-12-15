# Scripts/apply_module_worklist.py
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

REQ_DECISIONS = {"ACCEPT", "EDIT"}

def pick(val_edit, val_base):
    v = str(val_edit).strip() if pd.notna(val_edit) else ""
    if v:
        return v
    v2 = str(val_base).strip() if pd.notna(val_base) else ""
    return v2 or ""

def detect_module_col(df: pd.DataFrame) -> str:
    for c in ["UAP_Module", "UAP_Mod", "Module", "Version Name", "Module_Name", "Name"]:
        if c in df.columns:
            return c
    raise KeyError("Could not find a Module column (looked for UAP_Module/UAP_Mod/Module/Version Name/Module_Name/Name).")

def main():
    ap = argparse.ArgumentParser(description="Apply RESP worklist → final module catalog (Module,Entity,SOP,SubSOP).")
    ap.add_argument("--worklist", required=True, help="Path to FindModuleContext_Worklist_*_RESP.csv")
    ap.add_argument("--outdir", default="Outputs/ModMstr", help="Directory to write final catalog")
    args = ap.parse_args()

    in_path = Path(args.worklist)
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    df = pd.read_csv(in_path)
    df.columns = [c.strip() for c in df.columns]

    mod_col = detect_module_col(df)

    # Best-guess base context columns (parsed from Parent.Context earlier)
    base_entity  = "Entity" if "Entity" in df.columns else None
    base_sop     = "SOP" if "SOP" in df.columns else None
    base_subsop  = "SubSOP" if "SubSOP" in df.columns else ("Sub_SOP" if "Sub_SOP" in df.columns else None)

    # Final override columns
    final_entity = "Final_Entity" if "Final_Entity" in df.columns else None
    final_sop    = "Final_SOP" if "Final_SOP" in df.columns else None
    final_subsop = "Final_SubSOP" if "Final_SubSOP" in df.columns else ("Final_Sub_SOP" if "Final_Sub_SOP" in df.columns else None)

    if "Decision" not in df.columns:
        raise KeyError("RESP worklist must have a 'Decision' column (ACCEPT/EDIT).")

    # Keep only rows you decided on
    work = df[df["Decision"].astype(str).str.upper().isin(REQ_DECISIONS)].copy()
    if work.empty:
        raise ValueError("No rows with Decision in {ACCEPT, EDIT}. Fill Decision and try again.")

    # Build final mapping row-by-row
    out_rows = []
    for _, r in work.iterrows():
        module = str(r[mod_col]).strip()
        if not module:
            continue

        ent = pick(r.get(final_entity, ""), r.get(base_entity, ""))
        sop = pick(r.get(final_sop, ""), r.get(base_sop, ""))
        sub = pick(r.get(final_subsop, ""), r.get(base_subsop, ""))

        out_rows.append({
            "Module": module,
            "Entity": ent,
            "SOP": sop,
            "SubSOP": sub,
            "Source": "RESP_worklist",
            "Decision": str(r["Decision"]).strip().upper()
        })

    out = pd.DataFrame(out_rows)

    # Deduplicate by Module: prefer EDIT over ACCEPT; otherwise keep first nonblank
    if not out.empty:
        # Rank: EDIT(1) > ACCEPT(0)
        out["_rank"] = out["Decision"].map({"EDIT": 1, "ACCEPT": 0}).fillna(0)
        out.sort_values(["Module", "_rank"], ascending=[True, False], inplace=True)
        out = out.drop_duplicates(subset=["Module"], keep="first")
        out.drop(columns=["_rank"], inplace=True, errors="ignore")

    # Basic hygiene
    for c in ["Module", "Entity", "SOP", "SubSOP"]:
        if c in out.columns:
            out[c] = out[c].fillna("").astype(str).str.strip()

    # Save
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = out_dir / f"module_catalog_final_{ts}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8")

    # Console summary
    nonblank = (out["Entity"].ne("") | out["SOP"].ne("") | out["SubSOP"].ne("")).sum()
    print(f"[INFO] Wrote final catalog: {out_path}")
    print(f"[SUMMARY] Modules total: {len(out)} | with any context: {nonblank}")

if __name__ == "__main__":
    main()
