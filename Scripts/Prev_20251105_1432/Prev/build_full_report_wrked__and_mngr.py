# Scripts/build_full_report.py
import argparse
import os
import json
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


# -------------------------------------------------
# PATHS
# -------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root
INPUT_DIR = BASE_DIR / "Inputs"
OUTPUT_DIR = BASE_DIR / "Outputs"
CONFIG_DIR = BASE_DIR / "Config"
CHECKLIST_DIR = BASE_DIR / "Session_Checklists"
TRANSI_DIR = BASE_DIR / "Transi"

SETTINGS_FILE = CONFIG_DIR / "settings.json5"
ROLE_MAP_FILE = CONFIG_DIR / "role_module_map.xlsx"  # fallback if --req-matrix not passed
DNR_FILE_DEFAULT = TRANSI_DIR / "DNR.csv"


# -------------------------------------------------
# UTIL
# -------------------------------------------------
def load_settings() -> dict:
    """json5-lite: allow // full-line comments."""
    if not SETTINGS_FILE.exists():
        return {}
    txt = SETTINGS_FILE.read_text(encoding="utf-8")
    cleaned = "\n".join(line for line in txt.splitlines() if not line.strip().startswith("//"))
    try:
        return json.loads(cleaned or "{}")
    except Exception:
        return {}


def _norm_name(x: str) -> str:
    """Robust normalizer: uppercase, strip, collapse spaces, drop periods."""
    if pd.isna(x):
        return ""
    s = str(x).upper().strip()
    s = s.replace(".", " ")
    s = " ".join(s.split())  # collapse multiple spaces
    return s


def add_date_only_from_ts(df: pd.DataFrame, src_ts_col: str, dest_date_col: str) -> None:
    """
    Ensures a true date-only column exists from a timestamp column (in-place).
    If src is missing/blank, result is NaT.
    """
    if src_ts_col not in df.columns:
        df[dest_date_col] = pd.NaT
        return
    ts = pd.to_datetime(df[src_ts_col], errors="coerce", utc=False)
    try:
        ts = ts.dt.tz_localize(None)
    except Exception:
        pass
    df[dest_date_col] = ts.dt.date


# -------------------------------------------------
# NAME OVERRIDES (Transi/FindName.csv)
# -------------------------------------------------
def load_name_overrides(path: Path) -> dict:
    """
    Build overrides mapping from FindName.csv.
    Accepts either pair:
      A) User_Full_Name -> Emp_Name
      B) User.Full Name -> HR_Name
    Returns dict: normalized(UserName) -> normalized(EmployeeName)
    """
    if not path.exists():
        return {}
    df = pd.read_csv(path)

    # Normalize dtype
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str)

    mappings = {}

    def add_pairs(src_col: str, dst_col: str):
        if src_col in df.columns and dst_col in df.columns:
            sub = df[[src_col, dst_col]].dropna(how="any")
            for _, row in sub.iterrows():
                u = _norm_name(row[src_col])
                e = _norm_name(row[dst_col])
                if u and e:
                    mappings[u] = e

    add_pairs("User_Full_Name", "Emp_Name")
    add_pairs("User.Full Name", "HR_Name")
    return mappings


# -------------------------------------------------
# TRAINING ATTEMPTS LOADER (UAP CSV)
# -------------------------------------------------
def load_attempts_df(uap_csv: Path, pass_string="passed") -> pd.DataFrame:
    if not uap_csv.exists():
        raise FileNotFoundError(f"UAP attempts file not found: {uap_csv}")
    df = pd.read_csv(uap_csv)

    # sanity
    expect = ["User.Full Name", "Version Name", "Start Time"]
    missing = [c for c in expect if c not in df.columns]
    if missing:
        raise KeyError(f"Attempts CSV missing required columns: {missing}")

    # parse timestamp & score
    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce")
    if "Score" in df.columns:
        df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
        df["Score_valid"] = df["Score"].where(df["Score"] >= 0, np.nan)
    else:
        df["Score_valid"] = np.nan

    # pass flag
    if "Result" in df.columns:
        df["Result_norm"] = df["Result"].astype(str).str.lower().str.strip()
        df["Passed_Flag"] = df["Result_norm"].eq(str(pass_string).lower())
    else:
        df["Passed_Flag"] = False

    # join key (override later if mapping present)
    df["match_name"] = df["User.Full Name"].apply(_norm_name)
    return df


# -------------------------------------------------
# EMPLOYEE MASTER LOADER (optional enrichment)
# -------------------------------------------------
def detect_header_row(df: pd.DataFrame, required_cols: list[str]) -> int:
    scan_rows = min(10, df.shape[0])
    for r in range(scan_rows):
        row_vals = df.iloc[r].astype(str).tolist()
        if all(req in row_vals for req in required_cols):
            return r
    return 0


def load_emp_master(emp_path: Path) -> pd.DataFrame:
    """Header may be within first ~10 rows; also repeats later. Normalize to Emp_* fields."""
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

    # Remove header repeats and blanks
    emp = emp[emp["Name"].notna()]
    emp = emp[emp["Name"].astype(str).str.strip().str.upper() != "NAME"]

    emp["match_name"] = emp["Name"].apply(_norm_name)

    rename_map = {
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
    for k in list(rename_map.keys()):
        if k not in emp.columns:
            rename_map.pop(k)
    emp = emp.rename(columns=rename_map)

    keep = [
        "match_name",
        "Emp_Name", "Emp_InforID", "Emp_Manager", "Emp_Title",
        "Emp_RoleName", "Emp_RoleDescription", "Emp_PrimaryFunctionalGroup",
        "Emp_Location", "Emp_LocationNumber", "Emp_Email", "Emp_AdOns",
    ]
    keep = [c for c in keep if c in emp.columns]
    emp = emp[keep].copy().drop_duplicates(subset=["match_name"], keep="first")
    return emp


# -------------------------------------------------
# REQUIREMENTS (explicit path or fallback)
# -------------------------------------------------
def load_requirement_matrix_from(path_str: str) -> pd.DataFrame:
    """
    Load role→module requirements and pivot to a matrix.
    Accepts either schema:
      A) Role_or_Dept, Module_Name, ReqFlag
      B) RoleName, Module_Name, Flag
    Returns a pivoted matrix: index=Role_or_Dept, columns=Module_Name, values=ReqFlag.
    """
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Requirements file not found: {path}")
    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    # Trim column names and values
    df.columns = [c.strip() for c in df.columns]

    # Map schema B -> A if needed
    if {"Role_or_Dept", "Module_Name", "ReqFlag"}.issubset(df.columns):
        pass  # already good
    elif {"RoleName", "Module_Name", "Flag"}.issubset(df.columns):
        df = df.rename(columns={"RoleName": "Role_or_Dept", "Flag": "ReqFlag"})
    else:
        raise KeyError(
            "Requirements file missing expected columns. "
            "Provide either [Role_or_Dept, Module_Name, ReqFlag] or [RoleName, Module_Name, Flag]. "
            f"Found: {sorted(df.columns)}"
        )

    # Clean value whitespace
    for c in ["Role_or_Dept", "Module_Name", "ReqFlag"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Build matrix
    matrix = (
        df.pivot_table(
            index="Role_or_Dept",
            columns="Module_Name",
            values="ReqFlag",
            aggfunc="first"
        )
        .reset_index()
    )
    return matrix


def build_requirement_matrix_fallback() -> pd.DataFrame:
    """
    Fallback: read Config/role_module_map.xlsx if present.
    Accepts same schemas as load_requirement_matrix_from:
      A) Role_or_Dept, Module_Name, ReqFlag
      B) RoleName, Module_Name, Flag
    """
    if not ROLE_MAP_FILE.exists():
        return pd.DataFrame()
    role_map = pd.read_excel(ROLE_MAP_FILE)
    role_map.columns = [c.strip() for c in role_map.columns]

    if {"Role_or_Dept", "Module_Name", "ReqFlag"}.issubset(role_map.columns):
        df = role_map.copy()
    elif {"RoleName", "Module_Name", "Flag"}.issubset(role_map.columns):
        df = role_map.rename(columns={"RoleName": "Role_or_Dept", "Flag": "ReqFlag"}).copy()
    else:
        return pd.DataFrame()  # unknown schema; skip silently

    for c in ["Role_or_Dept", "Module_Name", "ReqFlag"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    matrix = (
        df.pivot_table(
            index="Role_or_Dept",
            columns="Module_Name",
            values="ReqFlag",
            aggfunc="first"
        )
        .reset_index()
    )
    return matrix


def load_requirements_raw(path_str: str) -> pd.DataFrame:
    """
    Load the raw requirements listing (CSV/XLSX) with columns like:
      Entity, SOP, SubSOP, Module_Name, RoleName, Source, Flag
    Returns the dataframe as-is (with basic column normalization).
    """
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Requirements file not found: {path}")
    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    needed = ["Entity", "SOP", "SubSOP", "Module_Name"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Requirements file missing columns needed for policy join: {missing}")

    for col in ["Entity", "SOP", "SubSOP", "Module_Name", "RoleName"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


# -------------------------------------------------
# ROLE/AREA/POLICY MAP (optional) with UNMAPPED reporting
# -------------------------------------------------
def load_role_area_policy_map(
    path_str: str,
    req_raw: Optional[pd.DataFrame] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load CSV/XLSX linking Module -> Entity / Role_Area / Policy_Normalized.

    Accepts either:
      A) columns: Module OR Module_Name, Entity, Role_Area, Policy_Normalized
         (direct mapping). Returns (map_df, empty_unmapped_df)

      OR
      B) columns: Entity, SOP, SubSOP[, RoleName], Coverage
         (JOIN with req_raw to derive Module_Name). Returns (map_df, unmapped_df)
         where unmapped_df captures policy rows that failed to map to any Module.
    """
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Role/Area/Policy file not found: {path}")

    if path.suffix.lower() in (".xlsx", ".xls"):
        pol = pd.read_excel(path)
    else:
        pol = pd.read_csv(path)

    pol.columns = [c.strip() for c in pol.columns]

    # Case A: already Module-based
    if ("Module" in pol.columns) or ("Module_Name" in pol.columns):
        module_col = "Module" if "Module" in pol.columns else "Module_Name"
        need = [module_col, "Entity", "Role_Area", "Policy_Normalized"]
        missing = [c for c in need if c not in pol.columns]
        if missing:
            raise KeyError(f"Policy file missing columns: {missing}")
        out = pol.rename(columns={module_col: "Module"})[
            ["Module", "Entity", "Role_Area", "Policy_Normalized"]
        ].drop_duplicates()
        for col in ["Module", "Entity", "Role_Area", "Policy_Normalized"]:
            out[col] = out[col].astype(str).str.strip()
        return out.reset_index(drop=True), pd.DataFrame()

    # Case B: SOP/SubSOP-based; need to expand via requirements
    needed = ["Entity", "SOP", "SubSOP"]
    missing = [c for c in needed if c not in pol.columns]
    if missing:
        raise KeyError(f"Policy file missing columns for SOP/SubSOP join: {missing} and no Module/Module_Name present.")

    if req_raw is None or req_raw.empty:
        raise KeyError("Policy file has no Module field; please pass --req-matrix so we can derive Module from requirements.")

    for col in ["Entity", "SOP", "SubSOP", "RoleName", "Coverage"]:
        if col in pol.columns:
            pol[col] = pol[col].astype(str).str.strip()

    join_keys = ["Entity", "SOP", "SubSOP"]
    if "RoleName" in pol.columns and "RoleName" in req_raw.columns:
        join_keys.append("RoleName")

    # Keep only necessary columns for join from requirements
    req_cols = join_keys + (["Module_Name"] if "Module_Name" in req_raw.columns else [])
    req_sub = req_raw[req_cols].drop_duplicates()

    pol_expanded = pol.merge(req_sub, on=join_keys, how="left")

    # Unmapped rows: where Module_Name is NaN after join
    unmapped_mask = pol_expanded["Module_Name"].isna()
    unmapped_cols = [c for c in ["Entity", "SOP", "SubSOP", "RoleName", "Coverage"] if c in pol_expanded.columns]
    unmapped = pol_expanded.loc[unmapped_mask, unmapped_cols].drop_duplicates().reset_index(drop=True)

    # Build final map
    pol_expanded["Module"] = pol_expanded["Module_Name"].astype(str).str.strip()
    pol_expanded["Entity"] = pol_expanded["Entity"].astype(str).str.strip()
    if "RoleName" in pol_expanded.columns:
        pol_expanded["Role_Area"] = pol_expanded["RoleName"].astype(str).str.strip()
    else:
        pol_expanded["Role_Area"] = pol_expanded["SOP"].astype(str).str.strip()
    pol_expanded["Policy_Normalized"] = pol_expanded["Coverage"].astype(str).str.strip() if "Coverage" in pol_expanded.columns else "ALL"

    out = pol_expanded[["Module", "Entity", "Role_Area", "Policy_Normalized"]].dropna(subset=["Module"]).drop_duplicates()
    return out.reset_index(drop=True), unmapped


# -------------------------------------------------
# DNR (Do-Not-Report) — TRUE EXCEPTION LIST
# -------------------------------------------------
def _ensure_dnr_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure DNR df has at least the canonical columns."""
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


def load_dnr_names_authoritative(path: Path) -> Tuple[set[str], pd.DataFrame]:
    """
    Read DNR CSV (any row present means EXCLUDE, regardless of HR_Status).
    Returns:
      - set of normalized names to exclude
      - the DNR DataFrame (with ensured columns)
    """
    if not path.exists():
        return set(), pd.DataFrame(columns=["User.Full Name"])
    df = pd.read_csv(path)
    df = _ensure_dnr_columns(df)
    if "User.Full Name" not in df.columns:
        raise KeyError("DNR file must have 'User.Full Name' column.")
    names = set(df["User.Full Name"].astype(str).map(_norm_name).tolist())
    names.discard("")  # remove blanks
    return names, df


def _extract_terms_names(terms_df: pd.DataFrame) -> set[str]:
    """
    Try several likely columns to find employee names in a Terms file.
    """
    cand_cols = ["User.Full Name", "Name", "Employee Name", "FullName", "Emp_Name"]
    for c in cand_cols:
        if c in terms_df.columns:
            return set(terms_df[c].astype(str).map(_norm_name).tolist())
    # Fallback: if the very first column is name-like
    first = terms_df.columns[0]
    return set(terms_df[first].astype(str).map(_norm_name).tolist())


def upsert_dnr_with_terms(dnr_df: pd.DataFrame, terms_path: Path) -> pd.DataFrame:
    """
    Upsert 'Terminated' names from an HR Terms file into DNR.
    - Adds rows for new names with HR_Status='Terminated', Added_On=now.
    - Leaves existing rows untouched (except fill HR_Status if blank).
    """
    if not terms_path or not terms_path.exists():
        return dnr_df

    if terms_path.suffix.lower() in (".xlsx", ".xls"):
        tdf = pd.read_excel(terms_path)
    else:
        tdf = pd.read_csv(terms_path)

    tdf_columns = [c.strip() for c in tdf.columns]
    tdf.columns = tdf_columns

    term_names = _extract_terms_names(tdf)
    if not term_names:
        return dnr_df

    dnr_df = _ensure_dnr_columns(dnr_df)
    if "User.Full Name" not in dnr_df.columns:
        dnr_df["User.Full Name"] = np.nan

    # Build a normalized index on DNR
    dnr_df["_match_name"] = dnr_df["User.Full Name"].map(_norm_name)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    for n in sorted(term_names):
        if not n:
            continue
        exists = (dnr_df["_match_name"] == n).any()
        if not exists:
            # Insert new row
            new_row = {
                "User.Full Name": n,  # we store normalized form for minimal typing
                "HR_Status": "TERMINATED",
                "Added_On": now_str,
                "Reason": "HR Terms import",
            }
            dnr_df = pd.concat([dnr_df, pd.DataFrame([new_row])], ignore_index=True)
        else:
            # If exists but missing HR_Status, set to Terminated
            idx = dnr_df.index[dnr_df["_match_name"] == n]
            if dnr_df.loc[idx, "HR_Status"].isna().any() or (dnr_df.loc[idx, "HR_Status"].astype(str).str.strip() == "").any():
                dnr_df.loc[idx, "HR_Status"] = "TERMINATED"
                dnr_df.loc[idx, "Reason"] = dnr_df.loc[idx, "Reason"].fillna("HR Terms import")
                dnr_df.loc[idx, "Added_On"] = dnr_df.loc[idx, "Added_On"].fillna(now_str)

    dnr_df = dnr_df.drop(columns=["_match_name"], errors="ignore")
    return dnr_df


def save_dnr_with_backup(dnr_df: pd.DataFrame, dnr_path: Path):
    dnr_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    if dnr_path.exists():
        backup = dnr_path.parent / f"DNR_backup_{ts}.csv"
        dnr_path.replace(backup)
        print(f"[INFO] DNR backed up to: {backup}")
    dnr_df.to_csv(dnr_path, index=False, encoding="utf-8")
    print(f"[INFO] DNR updated: {dnr_path}")


# -------------------------------------------------
# MERGE
# -------------------------------------------------
def attach_emp_info(attempts_df: pd.DataFrame, emp_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if emp_df is None or emp_df.empty:
        enriched = attempts_df.copy()
        enriched["Emp_Name"] = np.nan
        return enriched
    return attempts_df.merge(emp_df, how="left", on="match_name", suffixes=("", "_emp"))


# -------------------------------------------------
# TABS
# -------------------------------------------------
def build_report_card(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces:
      - FirstAttempt_date / LastAttempt_date (YYYY-MM-DD HH:MM)
      - FirstAttempt_D / LastAttempt_D (date-only)
      - Emp_* HR columns including Emp_Manager
    """
    user_col = "User.Full Name"
    module_col = "Version Name"

    summary = (
        enriched_df.groupby([user_col, module_col]).agg(
            Attempts=("Start Time", "count"),
            FirstAttempt=("Start Time", "min"),
            LastAttempt=("Start Time", "max"),
            MinScore=("Score_valid", "min"),
            MaxScore=("Score_valid", "max"),
            EverPassed=("Passed_Flag", "any"),
        ).reset_index()
    )

    # Text timestamps for readability
    summary["FirstAttempt_date"] = summary["FirstAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    summary["LastAttempt_date"]  = summary["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")

    # True date-only columns for pivots/slicers
    add_date_only_from_ts(summary, "FirstAttempt", "FirstAttempt_D")
    add_date_only_from_ts(summary, "LastAttempt",  "LastAttempt_D")

    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lookup = enriched_df[[user_col] + emp_cols].drop_duplicates(subset=[user_col])
    report_card = summary.merge(emp_lookup, how="left", on=user_col)

    front_cols = [
        user_col,
        "Emp_InforID", "Emp_Manager", "Emp_PrimaryFunctionalGroup",
        "Emp_Title", "Emp_RoleName", "Emp_RoleDescription",
        "Emp_Location", "Emp_LocationNumber",
        module_col, "Attempts", "MinScore", "MaxScore",
        "FirstAttempt_date", "FirstAttempt_D",
        "LastAttempt_date", "LastAttempt_D",
        "EverPassed",
    ]
    front_cols = [c for c in front_cols if c in report_card.columns]
    return report_card[front_cols].sort_values([user_col, module_col]).reset_index(drop=True)


def build_exceptions(enriched_df: pd.DataFrame, flag_score_min=70) -> pd.DataFrame:
    user_col = "User.Full Name"
    module_col = "Version Name"

    summary = (
        enriched_df.groupby([user_col, module_col]).agg(
            Attempts=("Start Time", "count"),
            FirstAttempt=("Start Time", "min"),
            LastAttempt=("Start Time", "max"),
            MaxScore=("Score_valid", "max"),
            EverPassed=("Passed_Flag", "any"),
        ).reset_index()
    )

    def reason_row(r):
        reasons = []
        maxscore = r["MaxScore"]
        if pd.isna(maxscore):
            reasons.append("no score")
        elif maxscore < flag_score_min:
            reasons.append(f"score <{flag_score_min}")
        if not r["EverPassed"]:
            reasons.append("no pass")
        return ", ".join(reasons)

    summary["Reason"] = summary.apply(reason_row, axis=1)
    summary["LastAttempt_date"] = summary["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")

    mask = (
        (summary["EverPassed"] == False)
        | (summary["MaxScore"].isna())
        | (summary["MaxScore"].fillna(-1) < flag_score_min)
    )
    flagged = summary.loc[mask].copy()

    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lookup = enriched_df[[user_col] + emp_cols].drop_duplicates(subset=[user_col])
    flagged = flagged.merge(emp_lookup, how="left", on=user_col)

    keep = [
        user_col,
        "Emp_Manager", "Emp_PrimaryFunctionalGroup", "Emp_Title",
        "Emp_RoleName", "Emp_Location", "Emp_LocationNumber",
        module_col, "Attempts", "MaxScore", "EverPassed",
        "LastAttempt_date", "Reason",
    ]
    keep = [c for c in keep if c in flagged.columns]
    return flagged[keep].sort_values([user_col, "EverPassed", "MaxScore"]).reset_index(drop=True)


def build_last_active(enriched_df: pd.DataFrame) -> pd.DataFrame:
    user_col = "User.Full Name"
    last_touch = (
        enriched_df.groupby(user_col)["Start Time"]
        .max().reset_index().rename(columns={"Start Time": "LastActivity"})
    )
    last_touch["LastActivity_date"] = last_touch["LastActivity"].dt.strftime("%Y-%m-%d %H:%M")

    # Optional: also provide a date-only for aging pivots if you want it later
    add_date_only_from_ts(last_touch, "LastActivity", "LastActivity_D")

    today_floor = pd.Timestamp(date.today())
    last_touch["DaysSinceActive"] = (today_floor - last_touch["LastActivity"].dt.floor("D")).dt.days

    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lookup = enriched_df[[user_col] + emp_cols].drop_duplicates(subset=[user_col])
    last_active = last_touch.merge(emp_lookup, how="left", on=user_col)

    keep = [
        user_col,
        "Emp_InforID", "Emp_Manager", "Emp_PrimaryFunctionalGroup",
        "Emp_Title", "Emp_RoleName", "Emp_RoleDescription",
        "Emp_Location", "Emp_LocationNumber",
        "LastActivity_date", "LastActivity_D", "DaysSinceActive",
    ]
    keep = [c for c in keep if c in last_active.columns]
    return last_active[keep].sort_values(user_col).reset_index(drop=True)


def _module_risk_core(grouped: pd.DataFrame) -> dict:
    """Helper to compute risk metrics for a grouped slice of attempts."""
    user_col = "User.Full Name"
    g = grouped

    users_opened = set(g[user_col].dropna().unique().tolist())
    succeeded_users = set(g.loc[g["Passed_Flag"], user_col].dropna().unique().tolist())
    passing_scores = g.loc[g["Passed_Flag"], "Score_valid"].dropna().tolist()

    att_to_pass, hrs_to_pass = [], []
    for u, gu in g.groupby(user_col):
        gu = gu.sort_values("Start Time").reset_index(drop=True)
        if gu.empty:
            continue
        first_t = gu["Start Time"].min()
        pass_idx = gu.index[gu["Passed_Flag"] == True].tolist()
        if pass_idx:
            i = pass_idx[0]
            pass_t = gu.loc[i, "Start Time"]
            att_to_pass.append(i + 1)  # position within sorted rows
            hrs_to_pass.append((pass_t - first_t).total_seconds() / 3600.0)

    def stats(lst):
        if not lst:
            return (np.nan, np.nan, np.nan)
        return (min(lst), float(np.mean(lst)), max(lst))

    att_min, att_avg, att_max = stats(att_to_pass)
    tmin, tavg, tmax = stats(hrs_to_pass)
    smin, savg, smax = stats(passing_scores)

    opened = len(users_opened)
    succ = len(succeeded_users)
    conv = 100.0 * succ / opened if opened else np.nan

    return {
        "UsersOpened": opened,
        "UsersSucceeded": succ,
        "UsersStillNotPassed": (opened - succ) if opened else np.nan,
        "ConversionPct": conv,
        "AttemptsToPass_Min": att_min,
        "AttemptsToPass_Avg": att_avg,
        "AttemptsToPass_Max": att_max,
        "TimeToPassHrs_Min": tmin,
        "TimeToPassHrs_Avg": tavg,
        "TimeToPassHrs_Max": tmax,
        "PassScore_Min": smin,
        "PassScore_Avg": savg,
        "PassScore_Max": smax,
    }


def build_module_risk(enriched_df: pd.DataFrame) -> pd.DataFrame:
    module_col = "Version Name"
    rows = []
    for mod, g in enriched_df.groupby(module_col):
        core = _module_risk_core(g)
        rows.append({"Module": mod, **core})
    return pd.DataFrame(rows).sort_values("Module").reset_index(drop=True)


def build_module_risk_by_policy(enriched_df: pd.DataFrame, policy_map: pd.DataFrame) -> pd.DataFrame:
    if policy_map is None or policy_map.empty:
        return pd.DataFrame()
    tmp = enriched_df.rename(columns={"Version Name": "Module"}).merge(policy_map, on="Module", how="left")
    rows = []
    for keys, g in tmp.groupby(["Module", "Entity", "Role_Area", "Policy_Normalized"], dropna=False):
        core = _module_risk_core(g)
        rows.append({
            "Module": keys[0],
            "Entity": keys[1],
            "Role_Area": keys[2],
            "Policy_Normalized": keys[3],
            **core
        })
    return (
        pd.DataFrame(rows)
        .sort_values(["Entity", "Role_Area", "Policy_Normalized", "Module"], na_position="last")
        .reset_index(drop=True)
    )


# -------------------------------------------------
# WRITE OUTPUT
# -------------------------------------------------
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
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        # Data tabs write with headers at row 1
        report_card.to_excel(writer, sheet_name="Report_Card", index=False)
        exceptions_view.to_excel(writer, sheet_name="Exceptions", index=False)
        last_active_view.to_excel(writer, sheet_name="Last_Active", index=False)
        module_risk_df.to_excel(writer, sheet_name="Module_Risk", index=False)
        if module_risk_by_policy_df is not None and not module_risk_by_policy_df.empty:
            module_risk_by_policy_df.to_excel(writer, sheet_name="Module_Risk_By_Policy", index=False)
        req_matrix_df.to_excel(writer, sheet_name="Requirement_Matrix", index=False)

        meta = pd.DataFrame({
            "Source_AttemptCSV": [str(attempts_path_used)],
            "Source_EmployeeMaster": [str(emp_path_used) if emp_path_used else ""],
            "Generated_Timestamp": [datetime.now().strftime("%Y%m%d_%H%M")],
        })
        meta.to_excel(writer, sheet_name="Run_Metadata", index=False)

        pd.DataFrame({"ColumnName": enriched_df.columns.tolist()}).to_excel(
            writer, sheet_name="Source_Columns", index=False
        )
        # Optional summary instead of title rows on data tabs
        pd.DataFrame({"Note":[f"Training Attempts Report • built {datetime.now().strftime('%Y-%m-%d %H:%M')}"]}).to_excel(
            writer, sheet_name="Summary", index=False
        )
    return out_path


def update_session_checklist(out_xlsx_path: Path, project_start_ny: str):
    latest_file = CHECKLIST_DIR / "runcheck_latest.html"
    if not latest_file.exists():
        return None
    base_part = project_start_ny[:19]
    try:
        start_dt = datetime.strptime(base_part, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        start_dt = datetime.now()
    now_local = datetime.now()
    cycle_hours = (now_local - start_dt).total_seconds() / 3600.0

    tsstamp = now_local.strftime("%Y-%m-%d %H:%M")
    rel_xlsx = os.path.relpath(out_xlsx_path, BASE_DIR)
    html_latest = latest_file.read_text(encoding="utf-8")
    injected = (
        f"<p><strong>Most recent run:</strong> {tsstamp} local time</p>\n"
        f"<p><strong>Latest output workbook:</strong> {rel_xlsx}</p>\n"
        f"<p><strong>Cycle time since 2025-10-29 07:36 AM ET:</strong> {cycle_hours:.2f} hours</p>\n"
    )
    marker = "(will be updated by train_report.py)"
    if marker in html_latest:
        html_latest = html_latest.replace(marker, injected)
    else:
        html_latest = html_latest.replace("</body>", injected + "</body>")
    tsname = now_local.strftime("%Y%m%d_%H%M")
    archive = CHECKLIST_DIR / f"runcheck_{tsname}.html"
    archive.write_text(html_latest, encoding="utf-8")
    latest_file.write_text(html_latest, encoding="utf-8")
    return archive


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Build full training attainment report with HR enrichment, name overrides, policy, requirements, and DNR."
    )
    ap.add_argument("--uap", required=True, help="Path to training attempts CSV (e.g., Inputs/SR04-Trn_Att_*.csv)")
    ap.add_argument("--findname", required=True, help="Path to Transi/FindName.csv (manual overrides).")
    ap.add_argument("--out-xlsx", required=True, help="Output path for Excel (e.g., Outputs/Training_Attempts_Report_YYYYMMDD_HHMM.xlsx)")
    ap.add_argument("--emp-mstr", help="Path to employee master Excel (optional enrichment).")
    ap.add_argument("--role-area-policy", help="Path to entity_role_area_policy_normalized_202*.csv/xlsx (optional).")
    ap.add_argument("--req-matrix", help="Path to role→module requirements (CSV or XLSX). Optional.")

    # DNR — true exception list
    ap.add_argument("--dnr", help="Path to Transi/DNR.csv (authoritative Do-Not-Report list).")
    ap.add_argument("--hr-terms", help="Path to HR Terms CSV/XLSX (optional, to upsert Terminated rows into DNR).")
    args = ap.parse_args()

    settings = load_settings()

    # 1) attempts
    uap_path = Path(args.uap)
    attempts_df = load_attempts_df(uap_path, pass_string=settings.get("pass_string", "passed"))

    # 1a) apply manual name overrides to join key BEFORE everything else
    overrides = load_name_overrides(Path(args.findname))
    if overrides:
        attempts_df["match_name"] = attempts_df["match_name"].apply(lambda x: overrides.get(x, x))

    # 1b) DNR (authoritative) — optional
    dnr_path = Path(args.dnr) if args.dnr else DNR_FILE_DEFAULT
    dnr_names, dnr_df = load_dnr_names_authoritative(dnr_path)  # names set + df (maybe empty)

    # 1c) Optional HR Terms -> update DNR on disk (backup + write)
    if args.hr_terms:
        try:
            updated = upsert_dnr_with_terms(dnr_df, Path(args.hr_terms))
            # If any change, persist and refresh names set
            if not updated.equals(dnr_df):
                save_dnr_with_backup(updated, dnr_path)
                dnr_names, _ = load_dnr_names_authoritative(dnr_path)
        except Exception as e:
            print("[WARN] HR Terms upsert skipped:", e)

    # 1d) Apply DNR exclusion (presence in DNR always excludes)
    if dnr_names:
        before = len(attempts_df)
        attempts_df = attempts_df[~attempts_df["match_name"].isin(dnr_names)].copy()
        after = len(attempts_df)
        print(f"[INFO] DNR applied: removed {before - after} attempt rows for {len(dnr_names)} names.")

    # 2) employee enrichment (optional)
    emp_df = None
    emp_path_used = None
    if args.emp_mstr:
        emp_path_used = Path(args.emp_mstr)
        emp_df = load_emp_master(emp_path_used)

    # 3) merge
    enriched_df = attach_emp_info(attempts_df, emp_df)

    # 4) requirement matrix (for matrix sheet)
    if args.req_matrix:
        req_matrix_df = load_requirement_matrix_from(args.req_matrix)
    else:
        req_matrix_df = build_requirement_matrix_fallback()

    # 5) baseline tabs
    report_card = build_report_card(enriched_df)
    exceptions_view = build_exceptions(enriched_df, flag_score_min=settings.get("flag_score_min", 70))
    last_active_view = build_last_active(enriched_df)
    module_risk_df = build_module_risk(enriched_df)

    # 6) policy-based risk (and unmapped export)
    module_risk_by_policy_df = pd.DataFrame()
    if args.role_area_policy:
        # If expansion needed, load raw requirements set
        req_raw = None
        if args.req_matrix:
            try:
                req_raw = load_requirements_raw(args.req_matrix)
            except Exception as e:
                print("[WARN] Could not load raw requirements for policy expansion:", e)

        policy_map, unmapped = load_role_area_policy_map(args.role_area_policy, req_raw=req_raw)
        module_risk_by_policy_df = build_module_risk_by_policy(enriched_df, policy_map)

        # Emit unmapped (if any)
        if unmapped is not None and not unmapped.empty:
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            out_unmapped = OUTPUT_DIR / f"policy_unmapped_modules_{ts}.csv"
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            unmapped.to_csv(out_unmapped, index=False)
            print("[WARN] Wrote policy unmapped report:", out_unmapped)

    # 7) write excel
    out_path = Path(args.out_xlsx)
    out_xlsx = write_output_excel(
        out_path=out_path,
        report_card=report_card,
        exceptions_view=exceptions_view,
        last_active_view=last_active_view,
        module_risk_df=module_risk_df,
        req_matrix_df=req_matrix_df,
        enriched_df=enriched_df,
        attempts_path_used=uap_path,
        emp_path_used=emp_path_used,
        module_risk_by_policy_df=module_risk_by_policy_df,
    )
    print("[INFO] Wrote Excel report:", out_xlsx)

    # 8) unmatched employee names (if emp provided)
    if emp_df is not None:
        unmatched = (
            enriched_df.loc[enriched_df["Emp_Name"].isna()]
            .groupby("User.Full Name", dropna=True).size()
            .reset_index(name="RowCount").sort_values("RowCount", ascending=False)
        )
        # Remove any DNR names from the unmatched dump, to reduce noise
        if dnr_names:
            unmatched["_match_name"] = unmatched["User.Full Name"].map(_norm_name)
            unmatched = unmatched[~unmatched["_match_name"].isin(dnr_names)].drop(columns=["_match_name"], errors="ignore")
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        unmatched_path = OUTPUT_DIR / f"unmatched_names_{ts}.csv"
        unmatched.to_csv(unmatched_path, index=False)
        print("[INFO] Wrote unmatched list:", unmatched_path)

    # 9) checklist stamp (if settings has project_start_ny)
    settings = settings or {}
    if "project_start_ny" in settings:
        update_session_checklist(out_xlsx_path=out_xlsx, project_start_ny=settings["project_start_ny"])

    print("[INFO] Done.")


if __name__ == "__main__":
    main()
