#!/usr/bin/env python3
# Emits a worklist of unmatched UAP names and applies responses to Transi/FindName.csv
# v1 103125_1653
import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime

NOW = datetime.now()

def read_csv_any(p):
    try:
        return pd.read_csv(p, dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(p, dtype=str, encoding="latin-1")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uap", default="Inputs/uap/SR04-Trn_Att.csv")
    ap.add_argument("--emp-mstr", default="emp_mstr/*.xlsx")
    ap.add_argument("--worklist", default="")
    ap.add_argument("--update", default="no", choices=["yes","no"])
    ap.add_argument("--outdir", default="Transi")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    if args.update == "yes":
        # Apply response worklist updates into Transi/FindName.csv
        resp = read_csv_any(args.worklist).fillna("")
        # Expect columns: User.Full Name (UAP), HR_Name, Flag
        # Normalize columns
        ucol = None; hcol = None; fcol = None
        for c in resp.columns:
            lc = c.lower().strip()
            if "user.full" in lc and "name" in lc: ucol = c
            if lc in ("hr_name","hr name","name"): hcol = c
            if lc == "flag": fcol = c
        if not (ucol and hcol and fcol):
            raise SystemExit("Response worklist must contain: User.Full Name, HR_Name (or Name), Flag")
        resp = resp[resp[fcol].str.upper()=="U"].copy()
        resp = resp.rename(columns={ucol:"User.Full Name", hcol:"HR_Name"})[["User.Full Name","HR_Name"]]
        fmap = outdir/"FindName.csv"
        if fmap.exists():
            cur = read_csv_any(fmap).fillna("")
            cur = pd.concat([cur, resp]).drop_duplicates(subset=["User.Full Name"], keep="last")
        else:
            cur = resp
        cur.to_csv(fmap, index=False)
        print(str(fmap))
        return

    # Generate worklist from current UAP vs HR
    uap = read_csv_any(args.uap).fillna("")
    # load HR (first sheet only, simple path) — for worklist we only need the Name column
    import glob
    hr_candidates = sorted(glob.glob(args.emp_mstr))
    if not hr_candidates:
        raise SystemExit("No HR file found for worklist generation")
    hr = pd.read_excel(hr_candidates[0], sheet_name=0)
    # Find HR Name column
    name_col = None
    for c in hr.columns:
        if str(c).strip().lower() in ("name","employee name","user.full name"):
            name_col = c; break
    hr_names = set(str(x).strip() for x in hr[name_col].dropna().tolist()) if name_col else set()
    # Existing map
    fmap = outdir/"FindName.csv"
    mapped = set()
    if fmap.exists():
        tmp = read_csv_any(fmap)
        if "User.Full Name" in tmp.columns: mapped = set(tmp["User.Full Name"].dropna().astype(str))
    uap_names = set(uap["User.Full Name"].dropna().astype(str))
    need = sorted(uap_names - hr_names - mapped)
    wl = pd.DataFrame({"User.Full Name": need, "HR_Name":"", "Flag":""})
    wl_path = outdir / f"FindName_Worklist_{NOW.strftime('%m%d%y_%H%M')}.csv"
    wl.to_csv(wl_path, index=False)
    print(str(wl_path))

if __name__ == "__main__":
    main()
