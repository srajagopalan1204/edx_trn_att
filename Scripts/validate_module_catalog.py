# Scripts/validate_module_catalog.py
import argparse, pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    args = ap.parse_args()
    p = Path(args.catalog)
    df = pd.read_excel(p) if p.suffix.lower() in (".xlsx",".xls") else pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]

    need = ["Module","Entity","SOP","SubSOP"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        print(f"[ERROR] Missing columns: {miss}. Found: {list(df.columns)}")
        return

    for c in need:
        blanks = df[c].isna() | (df[c].astype(str).str.strip()=="")
        print(f"{c}: total={len(df)} | blank={int(blanks.sum())}")

    print("\n[Sample blanks]")
    print(df.loc[(df["Entity"].isna()) | (df['Entity'].astype(str).str.strip()==''), :].head(10))

if __name__ == "__main__":
    main()
