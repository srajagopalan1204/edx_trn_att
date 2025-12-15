# Scripts/check_uap_vs_catalog_v4.py
import argparse, pandas as pd
from pathlib import Path

def norm(s: pd.Series) -> pd.Series:
    return (s.astype(str)
              .str.replace("-", " ", regex=False)
              .str.replace("_", " ", regex=False)
              .str.replace(r"\s+"," ", regex=True)
              .str.strip()
              .str.lower())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uap", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--out-dir", default="Outputs/QA")
    args = ap.parse_args()
    u = pd.read_csv(args.uap)
    c = pd.read_csv(args.catalog)
    if "Version Name" not in u.columns: raise SystemExit("[ERR] UAP missing Version Name")
    if "Module" not in c.columns: raise SystemExit("[ERR] Catalog missing Module")
    u["__key"]=norm(u["Version Name"]); c["__key"]=norm(c["Module"])
    m = u.merge(c[["__key"]], on="__key", how="left", indicator=True)
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    m.to_csv(out/"uap_vs_catalog_full.csv", index=False)
    m.loc[m["_merge"]=="left_only", ["Version Name","__key"]].drop_duplicates()\
        .to_csv(out/"uap_vs_catalog_unmatched.csv", index=False)
    print(f"[INFO] Rows: {len(u)} | Unmatched: { (m['_merge']=='left_only').sum() }")

if __name__ == "__main__":
    main()
