#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
name_translate_fuzzy.py — UAP↔HR name matching helper with fuzzy suggestions.

Modes
-----
1) Generate a worklist of unmatched UAP names with fuzzy HR suggestions:
   python Scripts/name_translate_fuzzy.py \
     --uap Inputs/uap/SR04-Trn_Att.csv \
     --emp-mstr emp_mstr/All_Employees_New_Hires_Terms_10_29_25.xlsx \
     --outdir Transi --suggest yes --topn 5 --threshold 0.72

2) Apply your responses (Flag=U rows) to update the authoritative map:
   python Scripts/name_translate_fuzzy.py \
     --update yes \
     --worklist Transi/FindName_Resp_Worklist_<ts>.csv \
     --outdir Transi

Outputs
-------
- Transi/FindName_Worklist_<ts>.csv   (to review & edit)
- Transi/FindName.csv                  (authoritative map; created/updated)
"""

import argparse
import glob
import difflib
from pathlib import Path
from datetime import datetime
import pandas as pd

NOW = datetime.now()

# -------------------------- helpers --------------------------

def read_csv_any(path: str | Path) -> pd.DataFrame:
    """Read CSV with fallback encoding."""
    p = str(path)
    try:
        return pd.read_csv(p, dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(p, dtype=str, encoding="latin-1")

def normalize_name(s: str) -> str:
    """Lowercase, trim, collapse spaces, remove commas."""
    if s is None:
        return ""
    s = str(s).replace(",", " ").strip()
    return " ".join(s.split()).lower()

def best_suggestions(query: str, candidates: list[str], topn: int = 5, threshold: float = 0.72):
    """Return top-N candidates with similarity >= threshold."""
    nq = normalize_name(query)
    scored: list[tuple[str, float]] = []
    for c in candidates:
        score = difflib.SequenceMatcher(None, nq, normalize_name(c)).ratio()
        if score >= threshold:
            scored.append((c, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:topn]

def detect_header_and_standardize(hr_df: pd.DataFrame) -> pd.DataFrame:
    """
    HR workbook sometimes has title rows before the header.
    Find a header within first ~10 rows and set columns accordingly.
    """
    raw = hr_df.copy()
    # Search first 10 rows for any row that contains a "Name" variant
    header_row = None
    for i in range(min(10, len(raw))):
        row_vals = [str(v).strip().lower() for v in raw.iloc[i].tolist()]
        if any(v in ("name", "employee name", "user.full name") for v in row_vals):
            header_row = i
            break
    if header_row is None:
        # Fallback: assume row 0 already is header
        df = raw.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # Use that row as header
    df = raw.copy()
    df.columns = [str(v).strip() for v in df.iloc[header_row].tolist()]
    df = df.iloc[header_row + 1 :].reset_index(drop=True)
    # Normalize common columns (case-insensitive)
    ren = {}
    for c in df.columns:
        lc = c.lower().strip()
        if lc in {"name", "employee name", "user.full name"}:
            ren[c] = "Name"
    df = df.rename(columns=ren)
    return df

def resolve_hr_names(hr_path_glob: str) -> list[str]:
    """Load HR workbook (first matching file), detect header, return list of names."""
    matches = sorted(glob.glob(hr_path_glob))
    if not matches:
        raise SystemExit("No HR file found (check --emp-mstr path).")
    # Read first sheet of first matching file
    hr_raw = pd.read_excel(matches[0], sheet_name=0, header=None)
    hr = detect_header_and_standardize(hr_raw)
    # Choose the standard 'Name' column (after standardization)
    name_col = None
    candidates = [c for c in hr.columns if str(c).strip().lower() in ("name", "employee name", "user.full name")]
    if candidates:
        name_col = candidates[0]
    if name_col is None:
        raise SystemExit("Could not locate an HR 'Name' column in the first sheet.")
    return [str(x).strip() for x in hr[name_col].dropna().tolist()]

# -------------------------- main logic --------------------------

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uap", default="Inputs/uap/SR04-Trn_Att.csv",
                    help="Path to UAP SR04-Trn_Att CSV")
    ap.add_argument("--emp-mstr", default="emp_mstr/*.xlsx",
                    help="Glob to HR master workbook")
    ap.add_argument("--outdir", default="Transi",
                    help="Output directory for worklists/maps")
    ap.add_argument("--suggest", choices=["yes", "no"], default="yes",
                    help="Include fuzzy suggestions in the worklist")
    ap.add_argument("--topn", type=int, default=5,
                    help="Max suggestions per name")
    ap.add_argument("--threshold", type=float, default=0.72,
                    help="Min similarity [0..1] to include a suggestion")
    ap.add_argument("--update", choices=["yes", "no"], default="no",
                    help="Apply responses to update Transi/FindName.csv")
    ap.add_argument("--worklist", default="",
                    help="Path to response worklist CSV when --update yes")
    return ap.parse_args()

def main():
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # ---------------- UPDATE MODE ----------------
    if args.update == "yes":
        if not args.worklist:
            raise SystemExit("--worklist is required with --update yes")
        resp = read_csv_any(args.worklist).fillna("")

        # Normalize expected columns
        ucol = None; hcol = None; fcol = None
        for c in resp.columns:
            lc = c.lower().strip()
            if "user.full" in lc and "name" in lc:
                ucol = c
            if lc in ("hr_name", "hr name", "name"):
                hcol = c
            if lc == "flag":
                fcol = c
        if not (ucol and hcol and fcol):
            raise SystemExit("Response worklist must contain: "
                             "'User.Full Name', 'HR_Name' (or 'Name'), and 'Flag'.")

        apply_rows = resp[resp[fcol].astype(str).str.upper() == "U"].copy()
        apply_rows = apply_rows.rename(columns={ucol: "User.Full Name", hcol: "HR_Name"})[
            ["User.Full Name", "HR_Name"]
        ]

        fmap = outdir / "FindName.csv"
        if fmap.exists():
            cur = read_csv_any(fmap).fillna("")
            cur = pd.concat([cur, apply_rows], ignore_index=True)
            cur = cur.drop_duplicates(subset=["User.Full Name"], keep="last")
        else:
            cur = apply_rows

        cur.to_csv(fmap, index=False)
        print(str(fmap))
        return

    # ---------------- WORKLIST MODE ----------------
    # Load UAP attempts and collect UAP names
    uap = read_csv_any(args.uap).fillna("")
    if "User.Full Name" not in uap.columns:
        alt = [c for c in uap.columns if c.strip().lower() in ("user.full name", "name", "employee name")]
        if not alt:
            raise SystemExit("UAP file must have 'User.Full Name' (or 'Name'/'Employee Name').")
        uap = uap.rename(columns={alt[0]: "User.Full Name"})
    uap_names = set(uap["User.Full Name"].dropna().astype(str))

    # Load HR names (robust header detection)
    hr_names_list = resolve_hr_names(args.emp_mstr)
    hr_names_set = set(hr_names_list)

    # Existing authoritative map (UAP->HR)
    fmap = outdir / "FindName.csv"
    mapped = set()
    if fmap.exists():
        tmp = read_csv_any(fmap).fillna("")
        if "User.Full Name" in tmp.columns:
            mapped = set(tmp["User.Full Name"].dropna().astype(str))

    # Compute names needing mapping
    need = sorted(uap_names - hr_names_set - mapped)

    # Build worklist rows with suggestions
    rows = []
    do_suggest = (args.suggest == "yes")
    for u in need:
        if do_suggest:
            best = best_suggestions(u, hr_names_list, topn=args.topn, threshold=args.threshold)
            if best:
                primary, score = best[0]
                alternates = [f"{n} ({s:.2f})" for n, s in best[1:]]
                rows.append({
                    "User.Full Name": u,
                    "Suggested_HR_Name": primary,
                    "Score": f"{score:.2f}",
                    "Alt_Suggestions": " | ".join(alternates),
                    "HR_Name": "",
                    "Flag": "",
                })
                continue
        rows.append({
            "User.Full Name": u,
            "Suggested_HR_Name": "",
            "Score": "",
            "Alt_Suggestions": "",
            "HR_Name": "",
            "Flag": "",
        })

    worklist = pd.DataFrame(rows, columns=[
        "User.Full Name", "Suggested_HR_Name", "Score",
        "Alt_Suggestions", "HR_Name", "Flag"
    ])

    wl_path = outdir / f"FindName_Worklist_{NOW.strftime('%m%d%y_%H%M')}.csv"
    worklist.to_csv(wl_path, index=False)
    print(str(wl_path))

if __name__ == "__main__":
    main()
