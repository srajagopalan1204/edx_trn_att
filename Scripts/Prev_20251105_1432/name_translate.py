import argparse
import glob
import os
from datetime import datetime
from pathlib import Path
import difflib

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent  # repo root
INPUT_DIR = BASE_DIR / "Inputs"
TRANSI_DIR = BASE_DIR / "Transi"
EMP_DIR = BASE_DIR / "emp_mstr"

TRANSI_DIR.mkdir(exist_ok=True)

# ---------- helpers ----------

def latest_attempt_csv():
    csvs = sorted(glob.glob(str(INPUT_DIR / "*.csv")), key=os.path.getmtime, reverse=True)
    if not csvs:
        raise FileNotFoundError("No attempt CSVs found in Inputs/.")
    return csvs[0]

def detect_header_row_xlsx(xlsx_path: Path):
    """
    The HR export has the true header within the first ~10 rows.
    We search for the row containing all key labels.
    """
    probe = pd.read_excel(xlsx_path, header=None, nrows=10)
    required = {
        "Name","Location","Location Number","Postion/Title","Role Name",
        "Role Description","Infor ID","Ad Ons","Manager","Email","Primary Functional Group"
    }
    for r in range(min(10, probe.shape[0])):
        vals = set(str(v) for v in probe.iloc[r].tolist())
        if required.issubset(vals):
            return r
    return 0

def load_emp_master(emp_path: Path):
    hdr = detect_header_row_xlsx(emp_path)
    emp = pd.read_excel(emp_path, header=hdr)
    emp = emp[emp["Name"].notna()]
    emp = emp[emp["Name"].astype(str).str.strip().str.upper() != "NAME"]
    emp["Name_norm"] = emp["Name"].astype(str).str.strip().str.upper()
    return emp[["Name","Name_norm"]].drop_duplicates().reset_index(drop=True)

def load_attempt_names():
    att_path = latest_attempt_csv()
    df = pd.read_csv(att_path)
    if "User.Full Name" not in df.columns:
        raise KeyError("Expected 'User.Full Name' in attempts CSV.")
    att = (
        df["User.Full Name"].astype(str).str.strip()
        .replace("", pd.NA).dropna().drop_duplicates()
        .to_frame(name="User_Full_Name")
    )
    att["User_norm"] = att["User_Full_Name"].str.upper()
    return att, att_path

def load_existing_map():
    map_path = TRANSI_DIR / "FindName.csv"
    if not map_path.exists():
        return pd.DataFrame(columns=["User_Full_Name","Emp_Name","Added_On"]), map_path
    m = pd.read_csv(map_path)
    for c in ["User_Full_Name","Emp_Name"]:
        if c not in m.columns:
            raise KeyError("FindName.csv must have columns: User_Full_Name, Emp_Name")
    return m, map_path

# ---------- worklist generation ----------

def emit_worklist(emp_path: Path):
    emp = load_emp_master(emp_path)
    att, att_src = load_attempt_names()

    # apply existing mapping so we don't ask again
    existing_map, _ = load_existing_map()
    if not existing_map.empty:
        already_mapped = set(existing_map["User_Full_Name"].astype(str).str.upper())
        att = att[~att["User_norm"].isin(already_mapped)]

    # remove any that already match HR exactly
    emp_names = set(emp["Name_norm"])
    unmatched = att[~att["User_norm"].isin(emp_names)].copy()

    # suggest closest HR name
    emp_name_list = emp["Name"].tolist()
    emp_name_norm_list = emp["Name_norm"].tolist()
    suggestions = []
    for _, row in unmatched.iterrows():
        cand = difflib.get_close_matches(row["User_norm"], emp_name_norm_list, n=1, cutoff=0.6)
        if cand:
            idx = emp_name_norm_list.index(cand[0])
            suggestions.append(emp_name_list[idx])
        else:
            suggestions.append("")
    unmatched["Suggested_Emp_Name"] = suggestions
    unmatched["Chosen_Emp_Name"] = ""   # you fill this
    unmatched["Flag"] = ""              # set to "U" to apply
    unmatched["Notes"] = ""

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = TRANSI_DIR / f"FindName_Worklist_{ts}.csv"
    unmatched[["User_Full_Name","Suggested_Emp_Name","Chosen_Emp_Name","Flag","Notes"]].to_csv(out_path, index=False)

    print("[INFO] Attempts source:", att_src)
    print("[INFO] HR source     :", emp_path)
    print("[INFO] Wrote worklist:", out_path)
    print("[INFO] Open in Excel, fill Chosen_Emp_Name, set Flag=U, save, then run with --update yes")

# ---------- update mapping ----------

def update_mapping_from_latest_worklist():
    # find latest worklist
    worklists = sorted(glob.glob(str(TRANSI_DIR / "FindName_Worklist_*.csv")), key=os.path.getmtime, reverse=True)
    if not worklists:
        raise FileNotFoundError("No FindName_Worklist_*.csv found in Transi/. Run worklist mode first.")
    wl_path = Path(worklists[0])
    wl = pd.read_csv(wl_path)
    need_cols = {"User_Full_Name","Chosen_Emp_Name","Flag"}
    if not need_cols.issubset(wl.columns):
        raise KeyError(f"{wl_path.name} must contain columns: {sorted(need_cols)}")

    to_apply = wl[(wl["Flag"].astype(str).str.upper() == "U") & (wl["Chosen_Emp_Name"].astype(str).str.strip() != "")]
    if to_apply.empty:
        print("[INFO] No rows flagged with U and a Chosen_Emp_Name. Nothing to update.")
        return

    # normalize keys
    to_apply = to_apply.copy()
    to_apply["User_norm"] = to_apply["User_Full_Name"].astype(str).str.upper().str.strip()

    existing_map, map_path = load_existing_map()
    if existing_map.empty:
        existing_map["User_Full_Name"] = []
        existing_map["Emp_Name"] = []
        existing_map["Added_On"] = []

    # build dict for quick replace / upsert
    existing_map["User_norm"] = existing_map["User_Full_Name"].astype(str).str.upper().str.strip()

    # upsert rows
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    for _, r in to_apply.iterrows():
        user_full = str(r["User_Full_Name"]).strip()
        emp_name  = str(r["Chosen_Emp_Name"]).strip()
        user_norm = str(r["User_norm"]).strip()

        # if user already in map, update; else append
        hit = existing_map["User_norm"] == user_norm
        if hit.any():
            existing_map.loc[hit, "Emp_Name"] = emp_name
            existing_map.loc[hit, "Added_On"] = now
        else:
            existing_map = pd.concat([
                existing_map,
                pd.DataFrame([{
                    "User_Full_Name": user_full,
                    "Emp_Name": emp_name,
                    "Added_On": now,
                    "User_norm": user_norm
                }])
            ], ignore_index=True)

    # save
    # keep stable columns, drop helper
    existing_map = existing_map[["User_Full_Name","Emp_Name","Added_On","User_norm"]]
    existing_map.sort_values("User_Full_Name", inplace=True)
    existing_map.to_csv(map_path, index=False)

    print("[INFO] Updated mapping:", map_path)
    print(f"[INFO] Applied rows: {to_apply.shape[0]}")
    print("[INFO] You can re-run reports now. build_full_report.py will use this map.")

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Maintain manual name mapping between Attempts and HR.")
    ap.add_argument("--emp-mstr", help="Path to employee master xlsx (required for worklist mode).")
    ap.add_argument("--update", choices=["yes","no"], default="no", help="Apply edited worklist rows (Flag=U) into Transi/FindName.csv")
    args = ap.parse_args()

    if args.update == "yes":
        update_mapping_from_latest_worklist()
    else:
        if not args.emp_mstr:
            ap.error("--emp-mstr is required in worklist mode")
        emit_worklist(Path(args.emp_mstr))

if __name__ == "__main__":
    main()

