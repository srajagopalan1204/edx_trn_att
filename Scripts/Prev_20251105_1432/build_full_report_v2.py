# -*- coding: utf-8 -*-
"""
Builds the Training Attempts Excel in one shot, with Module Catalog join so
Module_Risk_By_Policy has Entity/SOP/SubSOP populated.

Inputs (CLI):
  --uap                UAP attempts CSV
  --findname           Transi/FindName.csv
  --out-xlsx           Output Excel path
  --emp-mstr           Employee master XLSX (optional)
  --dnr                DNR CSV (authoritative; optional)
  --hr-terms           HR Terms CSV/XLSX (optional; upserts 'TERMINATED' into DNR)
  --role-area-policy   Policy CSV/XLSX (optional; kept for compatibility)
  --req-matrix         Requirements CSV/XLSX (optional; matrix tab)
  --module-catalog     Module catalog CSV (optional; else auto-pick latest Outputs/ModMstr/module_catalog_*.csv)

Outputs:
  - Excel workbook with Report_Card, Exceptions, Last_Active, Module_Risk,
    Module_Risk_By_Policy, Requirement_Matrix, Run_Metadata, Source_Columns, Summary.
  - Unmatched names CSV in Outputs/.
  - Optional policy_unmapped_modules_*.csv if policy expansion is used and has gaps.
"""

import argparse
import os
import json
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd

# ----------------------------- PATHS -----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "Inputs"
OUTPUT_DIR = BASE_DIR / "Outputs"
CONFIG_DIR = BASE_DIR / "Config"
CHECKLIST_DIR = BASE_DIR / "Session_Checklists"
TRANSI_DIR = BASE_DIR / "Transi"

SETINGS_FILE = CONFIG_DIR / "settings.json5"
ROLE_MAP_FILE = CONFIG_DIR / "role_module_map.xlsx"  # fallback matrix
DNR_FILE_DEFAULT = TRANSI_DIR / "DNR.csv"

# ----------------------------- UTIL ------------------------------
def load_settings() -> dict:
    p = SETINGS_FILE
    if not p.exists():
        return {}
    txt = p.read_text(encoding="utf-8")
    cleaned = "\n".join(line for line in txt.splitlines() if not line.strip().startswith("//"))
    try:
        return json.loads(cleaned or "{}")
    except Exception:
        return {}

def _norm_name(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).upper().strip()
    s = s.replace(".", " ")
    s = " ".join(s.split())
    return s

def _norm_module_key(s: str) -> str:
    """Loose normalizer to match Version Name to catalog Module."""
    if pd.isna(s):
        return ""
    t = str(s).strip()
    # unify separators & casing
    t = t.replace("_", " ")
    t = t.replace("-", " ")
    t = " ".join(t.split())
    return t.lower()

def add_date_only_from_ts(df: pd.DataFrame, src_ts_col: str, dest_date_col: str) -> None:
    if src_ts_col not in df.columns:
        df[dest_date_col] = pd.NaT
        return
    ts = pd.to_datetime(df[src_ts_col], errors="coerce", utc=False)
    try:
        ts = ts.dt.tz_localize(None)
    except Exception:
        pass
    df[dest_date_col] = ts.dt.date

# ---------------------- NAME OVERRIDES --------------------------
def load_name_overrides(path: Path) -> dict:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str)

    maps = {}
    def add_pair(src, dst):
        if src in df.columns and dst in df.columns:
            sub = df[[src, dst]].dropna(how="any")
            for _, r in sub.iterrows():
                u = _norm_name(r[src]); e = _norm_name(r[dst])
                if u and e:
                    maps[u] = e
    add_pair("User_Full_Name", "Emp_Name")
    add_pair("User.Full Name", "HR_Name")
    return maps

# ---------------------- LOADERS ---------------------------------
def load_attempts_df(uap_csv: Path, pass_string="passed") -> pd.DataFrame:
    if not uap_csv.exists():
        raise FileNotFoundError(f"UAP attempts file not found: {uap_csv}")
    df = pd.read_csv(uap_csv)

    need = ["User.Full Name", "Version Name", "Start Time"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise KeyError(f"Attempts CSV missing required columns: {missing}")

    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce")
    if "Score" in df.columns:
        df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
        df["Score_valid"] = df["Score"].where(df["Score"] >= 0, np.nan)
    else:
        df["Score_valid"] = np.nan

    if "Result" in df.columns:
        df["Result_norm"] = df["Result"].astype(str).str.lower().str.strip()
        df["Passed_Flag"] = df["Result_norm"].eq(str(pass_string).lower())
    else:
        df["Passed_Flag"] = False

    df["match_name"] = df["User.Full Name"].apply(_norm_name)
    # key used to join to module catalog
    df["Module_key"] = df["Version Name"].apply(_norm_module_key)
    return df

def detect_header_row(df: pd.DataFrame, required_cols: List[str]) -> int:
    scan_rows = min(10, df.shape[0])
    for r in range(scan_rows):
        vals = df.iloc[r].astype(str).tolist()
        if all(req in vals for req in required_cols):
            return r
    return 0

def load_emp_master(emp_path: Path) -> pd.DataFrame:
    if not emp_path.exists():
        raise FileNotFoundError(f"Employee master file not found: {emp_path}")
    raw = pd.read_excel(emp_path, header=None)
    required = [
        "Name", "Location", "Location Number", "Postion/Title", "Role Name",
        "Role Description", "Infor ID", "Ad Ons", "Manager", "Email",
        "Primary Functional Group",
    ]
    hdr = detect_header_row(raw, required)
    emp = pd.read_excel(emp_path, header=hdr)

    emp = emp[emp["Name"].notna()]
    emp = emp[emp["Name"].astype(str).str.strip().str.upper() != "NAME"]
    emp["match_name"] = emp["Name"].apply(_norm_name)

    rename = {
        "Name": "Emp_Name",
        "Location": "Emp_Location",
        "Location Number": "Emp_LocationNumber",
        "Postion/Title": "Emp_Title",
        "Role Name": "Emp_RoleName",
        "Role Description": "Emp_RoleDescription",
        "Infor ID": "Emp_InforID",
        "Ad Ons": "Emp_AdOns",
        "Manager": "Emp_Manager",
        "Email": "Emp_Email",
        "Primary Functional Group": "Emp_PrimaryFunctionalGroup",
    }
    for k in list(rename.keys()):
        if k not in emp.columns:
            rename.pop(k)
    emp = emp.rename(columns=rename)

    keep = [
        "match_name",
        "Emp_Name", "Emp_InforID", "Emp_Manager", "Emp_Title",
        "Emp_RoleName", "Emp_RoleDescription", "Emp_PrimaryFunctionalGroup",
        "Emp_Location", "Emp_LocationNumber", "Emp_Email", "Emp_AdOns",
    ]
    keep = [c for c in keep if c in emp.columns]
    return emp[keep].drop_duplicates(subset=["match_name"], keep="first")

# ---------------------- DNR / TERMS ------------------------------
def _ensure_dnr_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "User.Full Name",
        "Emp_Location", "Emp_LocationNumber", "Postion/Title",
        "RoleName", "Role Description", "Emp_InforID", "Ad Ons",
        "Emp_Manager", "Email", "Emp_PrimaryFunctionalGroup",
        "HR_Status", "Added_On", "Reason",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def load_dnr_names_authoritative(path: Path) -> Tuple[set, pd.DataFrame]:
    if not path.exists():
        return set(), pd.DataFrame(columns=["User.Full Name"])
    df = pd.read_csv(path)
    df = _ensure_dnr_columns(df)
    if "User.Full Name" not in df.columns:
        raise KeyError("DNR file must have 'User.Full Name' column.")
    names = set(df["User.Full Name"].astype(str).map(_norm_name).tolist())
    names.discard("")
    return names, df

def _extract_terms_names(terms_df: pd.DataFrame) -> set:
    cand = ["User.Full Name", "Name", "Employee Name", "FullName", "Emp_Name"]
    for c in cand:
        if c in terms_df.columns:
            return set(terms_df[c].astype(str).map(_norm_name).tolist())
    first = terms_df.columns[0]
    return set(terms_df[first].astype(str).map(_norm_name).tolist())

def upsert_dnr_with_terms(dnr_df: pd.DataFrame, terms_path: Path) -> pd.DataFrame:
    if not terms_path or not terms_path.exists():
        return dnr_df
    if terms_path.suffix.lower() in (".xlsx", ".xls"):
        tdf = pd.read_excel(terms_path)
    else:
        tdf = pd.read_csv(terms_path)
    tdf.columns = [c.strip() for c in tdf.columns]
    term_names = _extract_terms_names(tdf)
    if not term_names:
        return dnr_df

    dnr_df = _ensure_dnr_columns(dnr_df)
    if "User.Full Name" not in dnr_df.columns:
        dnr_df["User.Full Name"] = np.nan
    dnr_df["_match_name"] = dnr_df["User.Full Name"].map(_norm_name)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    for n in sorted(term_names):
        if not n:
            continue
        exists = (dnr_df["_match_name"] == n).any()
        if not exists:
            dnr_df = pd.concat([dnr_df, pd.DataFrame([{
                "User.Full Name": n,
                "HR_Status": "TERMINATED",
                "Added_On": now_str,
                "Reason": "HR Terms import",
            }])], ignore_index=True)
        else:
            idx = dnr_df.index[dnr_df["_match_name"] == n]
            if dnr_df.loc[idx, "HR_Status"].isna().any() or (dnr_df.loc[idx, "HR_Status"].astype(str).str.strip() == "").any():
                dnr_df.loc[idx, "HR_Status"] = "TERMINATED"
                dnr_df.loc[idx, "Reason"] = dnr_df.loc[idx, "Reason"].fillna("HR Terms import")
                dnr_df.loc[idx, "Added_On"] = dnr_df.loc[idx, "Added_On"].fillna(now_str)
    return dnr_df.drop(columns=["_match_name"], errors="ignore")

def save_dnr_with_backup(dnr_df: pd.DataFrame, dnr_path: Path):
    dnr_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    if dnr_path.exists():
        backup = dnr_path.parent / f"DNR_backup_{ts}.csv"
        dnr_path.replace(backup)
        print(f"[INFO] DNR backed up to: {backup}")
    dnr_df.to_csv(dnr_path, index=False, encoding="utf-8")
    print(f"[INFO] DNR updated: {dnr_path}")

# ---------------------- MODULE CATALOG ---------------------------
def pick_latest_catalog() -> Optional[Path]:
    mod_dir = OUTPUT_DIR / "ModMstr"
    if not mod_dir.exists():
        return None
    files = sorted(mod_dir.glob("module_catalog_*.csv"))
    return files[-1] if files else None

def load_module_catalog(path: Optional[Path]) -> pd.DataFrame:
    """
    Expected cols (case-sensitive): Module, Entity, SOP, SubSOP
    We also build Module_key for joining.
    """
    if path is None:
        return pd.DataFrame()
    if not path.exists():
        print(f"[WARN] Module catalog not found: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path)
    need = ["Module", "Entity", "SOP", "SubSOP"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        print(f"[WARN] Module catalog missing columns: {miss}.")
        return pd.DataFrame()
    df["Module_key"] = df["Module"].apply(_norm_module_key)
    # keep only one row per Module_key; if duplicates, prefer the first encountered
    df = df.dropna(subset=["Module_key"]).drop_duplicates(subset=["Module_key"], keep="first")
    return df[["Module_key", "Module", "Entity", "SOP", "SubSOP"]]

# ---------------------- REQUIREMENT MATRIX -----------------------
def load_requirement_matrix_from(path_str: str) -> pd.DataFrame:
    p = Path(path_str)
    if not p.exists():
        raise FileNotFoundError(f"Requirements file not found: {p}")
    if p.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(p)
    else:
        df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    if {"Role_or_Dept", "Module_Name", "ReqFlag"}.issubset(df.columns):
        pass
    elif {"RoleName", "Module_Name", "Flag"}.issubset(df.columns):
        df = df.rename(columns={"RoleName": "Role_or_Dept", "Flag": "ReqFlag"})
    else:
        raise KeyError("Requirements needs [Role_or_Dept, Module_Name, ReqFlag] or [RoleName, Module_Name, Flag].")
    for c in ["Role_or_Dept", "Module_Name", "ReqFlag"]:
        df[c] = df[c].astype(str).str.strip()
    matrix = df.pivot_table(index="Role_or_Dept", columns="Module_Name", values="ReqFlag", aggfunc="first").reset_index()
    return matrix

def build_requirement_matrix_fallback() -> pd.DataFrame:
    if not ROLE_MAP_FILE.exists():
        return pd.DataFrame()
    df = pd.read_excel(ROLE_MAP_FILE)
    df.columns = [c.strip() for c in df.columns]
    if {"Role_or_Dept", "Module_Name", "ReqFlag"}.issubset(df.columns):
        pass
    elif {"RoleName", "Module_Name", "Flag"}.issubset(df.columns):
        df = df.rename(columns={"RoleName": "Role_or_Dept", "Flag": "ReqFlag"})
    else:
        return pd.DataFrame()
    for c in ["Role_or_Dept", "Module_Name", "ReqFlag"]:
        df[c] = df[c].astype(str).str.strip()
    return df.pivot_table(index="Role_or_Dept", columns="Module_Name", values="ReqFlag", aggfunc="first").reset_index()

# ---------------------- BUILD TABS -------------------------------
def attach_emp_info(attempts_df: pd.DataFrame, emp_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if emp_df is None or emp_df.empty:
        enriched = attempts_df.copy()
        enriched["Emp_Name"] = np.nan
        return enriched
    return attempts_df.merge(emp_df, how="left", on="match_name", suffixes=("", "_emp"))

def attach_catalog(enriched_df: pd.DataFrame, catalog_df: pd.DataFrame) -> pd.DataFrame:
    """
    Left join on Module_key (from Version Name and catalog Module).
    Adds: Catalog_Module, Entity, SOP, SubSOP
    """
    if catalog_df is None or catalog_df.empty:
        out = enriched_df.copy()
        out["Catalog_Module"] = np.nan
        out["Entity"] = np.nan
        out["SOP"] = np.nan
        out["SubSOP"] = np.nan
        return out
    cat = catalog_df.rename(columns={"Module": "Catalog_Module"})
    return enriched_df.merge(cat, how="left", on="Module_key")

def build_report_card(enriched_df: pd.DataFrame) -> pd.DataFrame:
    ucol = "User.Full Name"
    mcol = "Version Name"
    g = (
        enriched_df.groupby([ucol, mcol]).agg(
            Attempts=("Start Time","count"),
            FirstAttempt=("Start Time","min"),
            LastAttempt=("Start Time","max"),
            MinScore=("Score_valid","min"),
            MaxScore=("Score_valid","max"),
            EverPassed=("Passed_Flag","any"),
        ).reset_index()
    )
    g["FirstAttempt_date"] = g["FirstAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    g["LastAttempt_date"]  = g["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    add_date_only_from_ts(g, "FirstAttempt", "FirstAttempt_D")
    add_date_only_from_ts(g, "LastAttempt",  "LastAttempt_D")

    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lut = enriched_df[[ucol] + emp_cols].drop_duplicates(subset=[ucol])
    rc = g.merge(emp_lut, how="left", on=ucol)

    cols = [
        ucol,
        "Emp_InforID", "Emp_Manager", "Emp_PrimaryFunctionalGroup",
        "Emp_Title", "Emp_RoleName", "Emp_RoleDescription",
        "Emp_Location", "Emp_LocationNumber",
        mcol, "Attempts", "MinScore", "MaxScore",
        "FirstAttempt_date", "FirstAttempt_D",
        "LastAttempt_date", "LastAttempt_D",
        "EverPassed",
    ]
    cols = [c for c in cols if c in rc.columns]
    return rc[cols].sort_values([ucol, mcol]).reset_index(drop=True)

def build_exceptions(enriched_df: pd.DataFrame, flag_score_min=70) -> pd.DataFrame:
    ucol = "User.Full Name"; mcol = "Version Name"
    g = (
        enriched_df.groupby([ucol, mcol]).agg(
            Attempts=("Start Time", "count"),
            FirstAttempt=("Start Time","min"),
            LastAttempt=("Start Time","max"),
            MaxScore=("Score_valid","max"),
            EverPassed=("Passed_Flag","any"),
        ).reset_index()
    )
    def reason(r):
        rs = []
        ms = r["MaxScore"]
        if pd.isna(ms): rs.append("no score")
        elif ms < flag_score_min: rs.append(f"score <{flag_score_min}")
        if not r["EverPassed"]: rs.append("no pass")
        return ", ".join(rs)
    g["Reason"] = g.apply(reason, axis=1)
    g["LastAttempt_date"] = g["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")

    mask = (g["EverPassed"] == False) | (g["MaxScore"].isna()) | (g["MaxScore"].fillna(-1) < flag_score_min)
    flagged = g.loc[mask].copy()

    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lut = enriched_df[[ucol] + emp_cols].drop_duplicates(subset=[ucol])
    flagged = flagged.merge(emp_lut, how="left", on=ucol)

    keep = [
        ucol,
        "Emp_Manager","Emp_PrimaryFunctionalGroup","Emp_Title",
        "Emp_RoleName","Emp_Location","Emp_LocationNumber",
        mcol,"Attempts","MaxScore","EverPassed",
        "LastAttempt_date","Reason",
    ]
    keep = [c for c in keep if c in flagged.columns]
    return flagged[keep].sort_values([ucol,"EverPassed","MaxScore"]).reset_index(drop=True)

def build_last_active(enriched_df: pd.DataFrame) -> pd.DataFrame:
    ucol = "User.Full Name"
    last = (
        enriched_df.groupby(ucol)["Start Time"].max().reset_index().rename(columns={"Start Time":"LastActivity"})
    )
    last["LastActivity_date"] = last["LastActivity"].dt.strftime("%Y-%m-%d %H:%M")
    add_date_only_from_ts(last, "LastActivity", "LastActivity_D")
    today_floor = pd.Timestamp(date.today())
    last["DaysSinceActive"] = (today_floor - last["LastActivity"].dt.floor("D")).dt.days

    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lut = enriched_df[[ucol] + emp_cols].drop_duplicates(subset=[ucol])
    out = last.merge(emp_lut, how="left", on=ucol)
    keep = [
        ucol,
        "Emp_InforID","Emp_Manager","Emp_PrimaryFunctionalGroup",
        "Emp_Title","Emp_RoleName","Emp_RoleDescription",
        "Emp_Location","Emp_LocationNumber",
        "LastActivity_date","LastActivity_D","DaysSinceActive",
    ]
    keep = [c for c in keep if c in out.columns]
    return out[keep].sort_values(ucol).reset_index(drop=True)

def _risk_core(g: pd.DataFrame) -> dict:
    ucol = "User.Full Name"
    users_opened = set(g[ucol].dropna().unique().tolist())
    succ_users = set(g.loc[g["Passed_Flag"], ucol].dropna().unique().tolist())
    pass_scores = g.loc[g["Passed_Flag"], "Score_valid"].dropna().tolist()

    att_to_pass, hrs_to_pass = [], []
    for u, gu in g.groupby(ucol):
        gu = gu.sort_values("Start Time").reset_index(drop=True)
        if gu.empty:
            continue
        first_t = gu["Start Time"].min()
        pass_idx = gu.index[gu["Passed_Flag"] == True].tolist()
        if pass_idx:
            i = pass_idx[0]
            pass_t = gu.loc[i, "Start Time"]
            att_to_pass.append(i + 1)
            hrs_to_pass.append((pass_t - first_t).total_seconds() / 3600.0)

    def stats(lst):
        if not lst: return (np.nan, np.nan, np.nan)
        return (min(lst), float(np.mean(lst)), max(lst))

    a_min, a_avg, a_max = stats(att_to_pass)
    t_min, t_avg, t_max = stats(hrs_to_pass)
    s_min, s_avg, s_max = stats(pass_scores)

    opened = len(users_opened)
    succ = len(succ_users)
    conv = 100.0 * succ / opened if opened else np.nan

    return {
        "UsersOpened": opened,
        "UsersSucceeded": succ,
        "UsersStillNotPassed": (opened - succ) if opened else np.nan,
        "ConversionPct": conv,
        "AttemptsToPass_Min": a_min,
        "AttemptsToPass_Avg": a_avg,
        "AttemptsToPass_Max": a_max,
        "TimeToPassHrs_Min": t_min,
        "TimeToPassHrs_Avg": t_avg,
        "TimeToPassHrs_Max": t_max,
        "PassScore_Min": s_min,
        "PassScore_Avg": s_avg,
        "PassScore_Max": s_max,
    }

def build_module_risk(enriched_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for mod, g in enriched_df.groupby("Version Name"):
        rows.append({"Module": mod, **_risk_core(g)})
    return pd.DataFrame(rows).sort_values("Module").reset_index(drop=True)

def build_module_risk_by_policy_from_catalog(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses Entity/SOP/SubSOP that came from catalog join (not the legacy policy join).
    """
    cols_needed = ["Version Name", "Entity", "SOP", "SubSOP"]
    for c in cols_needed:
        if c not in enriched_df.columns:
            enriched_df[c] = np.nan

    tmp = enriched_df.rename(columns={"Version Name": "Module"})
    groups = ["Module", "Entity", "SOP", "SubSOP"]
    rows = []
    for keys, g in tmp.groupby(groups, dropna=False):
        rows.append({
            "Module": keys[0],
            "Entity": keys[1],
            "SOP": keys[2],
            "SubSOP": keys[3],
            **_risk_core(g)
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["Entity","SOP","SubSOP","Module"], na_position="last").reset_index(drop=True)

# ---------------------- WRITE OUTPUT -----------------------------
def write_output_excel(
    out_path: Path,
    report_card: pd.DataFrame,
    exceptions_view: pd.DataFrame,
    last_active_view: pd.DataFrame,
    module_risk_df: pd.DataFrame,
    req_matrix_df: pd.DataFrame,
    enriched_df: pd.DataFrame,
    attempts_path_used: Path,
    emp_path_used: Optional[Path],
    module_risk_by_policy_df: Optional[pd.DataFrame] = None,
):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as w:
        report_card.to_excel(w, sheet_name="Report_Card", index=False)
        exceptions_view.to_excel(w, sheet_name="Exceptions", index=False)
        last_active_view.to_excel(w, sheet_name="Last_Active", index=False)
        module_risk_df.to_excel(w, sheet_name="Module_Risk", index=False)
        if module_risk_by_policy_df is not None and not module_risk_by_policy_df.empty:
            module_risk_by_policy_df.to_excel(w, sheet_name="Module_Risk_By_Policy", index=False)
        req_matrix_df.to_excel(w, sheet_name="Requirement_Matrix", index=False)

        meta = pd.DataFrame({
            "Source_AttemptCSV": [str(attempts_path_used)],
            "Source_EmployeeMaster": [str(emp_path_used) if emp_path_used else ""],
            "Generated_Timestamp": [datetime.now().strftime("%Y%m%d_%H%M")],
        })
        meta.to_excel(w, sheet_name="Run_Metadata", index=False)

        pd.DataFrame({"ColumnName": enriched_df.columns.tolist()}).to_excel(
            w, sheet_name="Source_Columns", index=False
        )
        pd.DataFrame({"Note":[f"Training Attempts Report • built {datetime.now().strftime('%Y-%m-%d %H:%M')}"]}).to_excel(
            w, sheet_name="Summary", index=False
        )
    return out_path

def update_session_checklist(out_xlsx_path: Path, project_start_ny: str):
    latest = CHECKLIST_DIR / "runcheck_latest.html"
    if not latest.exists():
        return None
    base_part = project_start_ny[:19]
    try:
        start_dt = datetime.strptime(base_part, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        start_dt = datetime.now()
    now_local = datetime.now()
    cycle_hours = (now_local - start_dt).total_seconds() / 3600.0
    tsstamp = now_local.strftime("%Y-%m-%d %H:%M")
    rel = os.path.relpath(out_xlsx_path, BASE_DIR)
    html = latest.read_text(encoding="utf-8")
    injected = (
        f"<p><strong>Most recent run:</strong> {tsstamp} local time</p>\n"
        f"<p><strong>Latest output workbook:</strong> {rel}</p>\n"
        f"<p><strong>Cycle time since 2025-10-29 07:36 AM ET:</strong> {cycle_hours:.2f} hours</p>\n"
    )
    marker = "(will be updated by train_report.py)"
    if marker in html:
        html = html.replace(marker, injected)
    else:
        html = html.replace("</body>", injected + "</body>")
    tsname = now_local.strftime("%Y%m%d_%H%M")
    arch = CHECKLIST_DIR / f"runcheck_{tsname}.html"
    arch.write_text(html, encoding="utf-8")
    latest.write_text(html, encoding="utf-8")
    return arch

# ---------------------- MAIN ------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Build full training report with Module Catalog join for Entity/SOP/SubSOP.")
    ap.add_argument("--uap", required=True)
    ap.add_argument("--findname", required=True)
    ap.add_argument("--out-xlsx", required=True)
    ap.add_argument("--emp-mstr")
    ap.add_argument("--role-area-policy")  # kept for compatibility (not required)
    ap.add_argument("--req-matrix")
    ap.add_argument("--dnr")
    ap.add_argument("--hr-terms")
    ap.add_argument("--module-catalog", help="Path to module_catalog_*.csv (optional; else auto-pick latest).")
    args = ap.parse_args()

    settings = load_settings()

    # 1) UAP attempts
    uap_path = Path(args.uap)
    attempts = load_attempts_df(uap_path, pass_string=settings.get("pass_string", "passed"))

    # 1a) name overrides
    overrides = load_name_overrides(Path(args.findname))
    if overrides:
        attempts["match_name"] = attempts["match_name"].apply(lambda x: overrides.get(x, x))

    # 1b) DNR
    dnr_path = Path(args.dnr) if args.dnr else DNR_FILE_DEFAULT
    dnr_names, dnr_df = load_dnr_names_authoritative(dnr_path)
    if args.hr_terms:
        try:
            updated = upsert_dnr_with_terms(dnr_df, Path(args.hr_terms))
            if not updated.equals(dnr_df):
                save_dnr_with_backup(updated, dnr_path)
                dnr_names, _ = load_dnr_names_authoritative(dnr_path)
        except Exception as e:
            print("[WARN] HR Terms upsert skipped:", e)
    if dnr_names:
        before = len(attempts)
        attempts = attempts[~attempts["match_name"].isin(dnr_names)].copy()
        after = len(attempts)
        print(f"[INFO] DNR applied: removed {before - after} attempt rows for {len(dnr_names)} names.")

    # 2) Emp master
    emp_df = None; emp_path_used = None
    if args.emp_mstr:
        emp_path_used = Path(args.emp_mstr)
        emp_df = load_emp_master(emp_path_used)

    # 3) Merge emp
    enriched = attach_emp_info(attempts, emp_df)

    # 4) Module catalog (Entity/SOP/SubSOP)
    cat_path = Path(args.module_catalog) if args.module_catalog else pick_latest_catalog()
    cat_df = load_module_catalog(cat_path)
    if cat_df.empty:
        print('[WARN] Module catalog missing/empty; Entity/SOP/SubSOP will be blank.')
    else:
        print(f"[INFO] Using module catalog: {cat_path}")
    enriched = attach_catalog(enriched, cat_df)

    # 5) Requirement Matrix (sheet)
    if args.req_matrix:
        req_matrix_df = load_requirement_matrix_from(args.req_matrix)
    else:
        req_matrix_df = build_requirement_matrix_fallback()

    # 6) Tabs
    report_card = build_report_card(enriched)
    exceptions_view = build_exceptions(enriched, flag_score_min=settings.get("flag_score_min", 70))
    last_active_view = build_last_active(enriched)
    module_risk_df = build_module_risk(enriched)
    module_risk_by_policy_df = build_module_risk_by_policy_from_catalog(enriched)

    # 7) Write Excel
    out_path = Path(args.out_xlsx)
    out_xlsx = write_output_excel(
        out_path=out_path,
        report_card=report_card,
        exceptions_view=exceptions_view,
        last_active_view=last_active_view,
        module_risk_df=module_risk_df,
        req_matrix_df=req_matrix_df,
        enriched_df=enriched,
        attempts_path_used=uap_path,
        emp_path_used=emp_path_used,
        module_risk_by_policy_df=module_risk_by_policy_df,
    )
    print("[INFO] Wrote Excel report:", out_xlsx)

    # 8) Unmatched names (emp join)
    if emp_df is not None:
        unmatched = (
            enriched.loc[enriched["Emp_Name"].isna()]
            .groupby("User.Full Name", dropna=True).size()
            .reset_index(name="RowCount").sort_values("RowCount", ascending=False)
        )
        if dnr_names:
            unmatched["_match_name"] = unmatched["User.Full Name"].map(_norm_name)
            unmatched = unmatched[~unmatched["_match_name"].isin(dnr_names)].drop(columns=["_match_name"], errors="ignore")
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        unmatched_path = OUTPUT_DIR / f"unmatched_names_{ts}.csv"
        unmatched.to_csv(unmatched_path, index=False)
        print("[INFO] Wrote unmatched list:", unmatched_path)

    # 9) Checklist stamp (optional)
    st = load_settings()
    if "project_start_ny" in st:
        update_session_checklist(out_xlsx_path=out_xlsx, project_start_ny=st["project_start_ny"])

    print("[INFO] Done.")

if __name__ == "__main__":
    main()
