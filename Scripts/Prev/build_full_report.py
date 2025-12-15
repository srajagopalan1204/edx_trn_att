#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Training Attempts • one-pass builder
- Ensures headers start on row 1 (no titles on data tabs)
- Adds date-only fields FirstAttempt_D / LastAttempt_D
- Attaches Emp_* HR fields (incl. Emp_Manager)
- Option B: Role/Area policy + req matrix -> Module_Risk + policy_unmapped_modules.csv
"""

from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np


# -------------------------------
# Utilities
# -------------------------------

def now_stamp(fmt="%Y%m%d_%H%M"):
    return datetime.now().strftime(fmt)


def read_csv_or_xlsx(path: Path, header=None, dtype=None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, header=header, dtype=dtype)
    return pd.read_csv(path, header=header, dtype=dtype)


def normalize_spaces_upper(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
         .str.upper()
    )


def add_date_only(df: pd.DataFrame, src: str, dest: str) -> pd.DataFrame:
    """Create date-only column from a text/timestamp column; blanks -> NaT."""
    if src in df.columns:
        ts = pd.to_datetime(df[src], errors="coerce", utc=False)
        try:
            ts = ts.dt.tz_localize(None)
        except Exception:
            pass
        df[dest] = ts.dt.date
    else:
        df[dest] = pd.NaT
    return df


# -------------------------------
# HR Master (robust header finder)
# -------------------------------

HR_EXPECTED = {
    "Name": "HR_Name",
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

def _find_hr_header_row(df_nohdr: pd.DataFrame) -> int:
    df2 = df_nohdr.astype(str)
    for r in range(0, min(12, len(df2))):
        row_vals = set(v.strip() for v in df2.iloc[r].tolist())
        if "Name" in row_vals and (
            "Manager" in row_vals or "Role Name" in row_vals or "Location" in row_vals
        ):
            return r
    return 0


def load_hr_master_robust(path_xlsx: Path) -> pd.DataFrame:
    raw = read_csv_or_xlsx(path_xlsx, header=None, dtype=str)
    hdr_row = _find_hr_header_row(raw)
    df = read_csv_or_xlsx(path_xlsx, header=hdr_row, dtype=str)

    # strip repeated header rows if HR exported blocks
    if "Name" in df.columns:
        df = df[df["Name"].astype(str) != "Name"].copy()

    keep = [c for c in df.columns if c in HR_EXPECTED]
    df = df[keep].copy()
    df.rename(columns=HR_EXPECTED, inplace=True)

    # normalize HR_Name for join
    if "HR_Name" not in df.columns:
        df["HR_Name"] = ""
    df["HR_Name"] = normalize_spaces_upper(df["HR_Name"])

    # de-dup on HR_Name
    df = df.drop_duplicates(subset=["HR_Name"], keep="last").reset_index(drop=True)
    return df


# -------------------------------
# FindName mapping
# -------------------------------

def load_findname(path_csv: Path) -> pd.DataFrame:
    fn = read_csv_or_xlsx(path_csv, dtype=str)
    cols = {c.upper(): c for c in fn.columns}

    # Build User_norm (from User.Full Name when needed)
    if "USER_NORM" not in cols:
        if "USER.FULL NAME" in cols:
            fn["User_norm"] = normalize_spaces_upper(fn[cols["USER.FULL NAME"]])
        else:
            # fallbacks
            for alt in ["User_Full_Name", "User Full Name", "User"]:
                if alt in fn.columns:
                    fn["User_norm"] = normalize_spaces_upper(fn[alt])
                    break
    # Build HR_Name if missing
    if "HR_NAME" not in cols:
        for alt in ["HR_Name", "Emp_Name", "Name"]:
            if alt in fn.columns:
                fn["HR_Name"] = normalize_spaces_upper(fn[alt])
                break
    if "User_norm" not in fn.columns:
        raise KeyError("FindName must include 'User_norm' or 'User.Full Name'")
    if "HR_Name" not in fn.columns:
        fn["HR_Name"] = ""

    # Keep only what we need
    fn = fn[["User_norm", "HR_Name"]].drop_duplicates("User_norm")
    return fn


# -------------------------------
# UAP Attempts loader + aggregation
# -------------------------------

# Flexible column map (source may vary slightly)
UAP_COL_CANDIDATES = {
    "user_name": ["User.Full Name", "User Full Name", "User Name", "User"],
    "module":    ["Version Name", "Module", "Course", "Learning Path"],
    "score":     ["Score", "MaxScore", "Result Score"],
    "result":    ["Result", "Passed", "EverPassed"],
    "started":   ["AttemptStart", "Start Time", "FirstAttempt_date", "First Attempt"],
    "ended":     ["AttemptEnd", "End Time", "LastAttempt_date", "Last Attempt"],
}

def pick(df: pd.DataFrame, keys: list[str]) -> str | None:
    for k in keys:
        if k in df.columns:
            return k
    return None


def load_uap_attempts(path_csv: Path) -> pd.DataFrame:
    df = read_csv_or_xlsx(path_csv, dtype=str)

    # Try to see if it's already an aggregated Report_Card export (has Attempts/MinScore/etc.)
    already_agg = all(c in df.columns for c in
                      ["User.Full Name", "Version Name", "Attempts"])
    if already_agg:
        # normalize date-only columns
        df = add_date_only(df, "FirstAttempt_date", "FirstAttempt_D")
        df = add_date_only(df, "LastAttempt_date",  "LastAttempt_D")
        return df

    # Otherwise aggregate from raw attempts
    col_user   = pick(df, UAP_COL_CANDIDATES["user_name"])
    col_mod    = pick(df, UAP_COL_CANDIDATES["module"])
    col_score  = pick(df, UAP_COL_CANDIDATES["score"])
    col_result = pick(df, UAP_COL_CANDIDATES["result"])
    col_start  = pick(df, UAP_COL_CANDIDATES["started"])
    col_end    = pick(df, UAP_COL_CANDIDATES["ended"])

    need = [col_user, col_mod]
    if any(v is None for v in need):
        raise KeyError("UAP file must include User name and Module columns.")

    tmp = pd.DataFrame({
        "User.Full Name": df[col_user],
        "Version Name":   df[col_mod],
        "Score":          df[col_score] if col_score else np.nan,
        "Result":         df[col_result] if col_result else "",
        "FirstAttempt_date": df[col_start] if col_start else "",
        "LastAttempt_date":  df[col_end] if col_end else "",
    })

    # numeric score
    if "Score" in tmp.columns:
        tmp["Score"] = pd.to_numeric(tmp["Score"], errors="coerce")

    # Aggregations
    grp = tmp.groupby(["User.Full Name", "Version Name"], dropna=False)
    out = grp.agg(
        Attempts=("Version Name", "count"),
        MinScore=("Score", "min"),
        MaxScore=("Score", "max"),
        FirstAttempt_date=("FirstAttempt_date", "min"),
        LastAttempt_date=("LastAttempt_date", "max"),
        EverPassed=("Result", lambda s: np.any(s.astype(str).str.upper().isin(["PASS", "PASSED", "TRUE", "1"])))
    ).reset_index()

    # date-only
    out = add_date_only(out, "FirstAttempt_date", "FirstAttempt_D")
    out = add_date_only(out, "LastAttempt_date",  "LastAttempt_D")
    return out


# -------------------------------
# Attach HR fields to Report_Card / Last_Active
# -------------------------------

def attach_hr_fields(report_card_df: pd.DataFrame,
                     findname_df: pd.DataFrame,
                     hr_df: pd.DataFrame) -> pd.DataFrame:
    rc = report_card_df.copy()
    rc["User_norm"] = normalize_spaces_upper(rc["User.Full Name"])

    fn = findname_df.copy()
    if "User_norm" not in fn.columns:
        raise KeyError("FindName must have 'User_norm' column")
    if "HR_Name" not in fn.columns:
        fn["HR_Name"] = ""

    rc = rc.merge(fn[["User_norm", "HR_Name"]].drop_duplicates("User_norm"),
                  on="User_norm", how="left")

    rc = rc.merge(hr_df, on="HR_Name", how="left")

    # Reorder to keep Emp_* next to the user name
    emp_cols = [c for c in [
        "Emp_Location","Emp_LocationNumber","Emp_Title","Emp_RoleName","Emp_RoleDescription",
        "Emp_InforID","Emp_AdOns","Emp_Manager","Emp_Email","Emp_PrimaryFunctionalGroup"
    ] if c in rc.columns]

    def move_after(cols, anchor, movers):
        out = []
        for c in cols:
            out.append(c)
            if c == anchor:
                for m in movers:
                    if m not in out and m in cols:
                        out.append(m)
        return out

    new_order = move_after(list(rc.columns), "User.Full Name", emp_cols)
    rc = rc[new_order]
    return rc


def build_last_active(report_card_df: pd.DataFrame) -> pd.DataFrame:
    # last attempt by user (overall), plus days since last attempt
    la = (report_card_df
          .groupby("User.Full Name", as_index=False)
          .agg(LastAttempt_date=("LastAttempt_date", "max"),
               LastAttempt_D=("LastAttempt_D", "max")))
    la["Today"] = pd.to_datetime(datetime.now().date())
    la["Days_Since_LastAttempt"] = (la["Today"] - pd.to_datetime(la["LastAttempt_D"])).dt.days
    return la


# -------------------------------
# Option B: Policy + Req Matrix
# -------------------------------

def load_role_area_policy_map(path_csv: Path) -> pd.DataFrame:
    df = read_csv_or_xlsx(path_csv, dtype=str)
    need = {"RoleName","Entity","SOP","SubSOP"}
    missing = need - set(df.columns)
    if missing:
        raise KeyError(f"Role/Area/Policy file missing columns: {sorted(missing)}")
    for c in need:
        df[c] = df[c].astype(str).str.strip()
    return df


def load_requirements_raw(path_csv: Path) -> pd.DataFrame:
    df = read_csv_or_xlsx(path_csv, dtype=str)
    need = {"Entity","SOP","SubSOP","Module_Name","RoleName","Flag"}
    missing = need - set(df.columns)
    if missing:
        raise KeyError(f"Requirements file missing columns: {sorted(missing)}")
    for c in need:
        df[c] = df[c].astype(str).str.strip()
    return df


def build_module_risk(policy_df: pd.DataFrame, req_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Join req -> policy on Entity/SOP/SubSOP/RoleName
    merged = req_df.merge(policy_df, on=["Entity","SOP","SubSOP","RoleName"], how="left", indicator=True)
    unmapped = merged[merged["_merge"] == "left_only"].copy()
    unmapped = unmapped.drop(columns=["_merge"])
    # Keep only useful columns in Module_Risk table
    modrisk = merged.drop(columns=["_merge"])
    # Optional: compute a simple risk tag (R/N/Z) by module
    # Already provided in Flag, but you can extend here later
    return modrisk, unmapped


# -------------------------------
# Writer
# -------------------------------

def write_excel(out_path: Path,
                report_card: pd.DataFrame,
                last_active: pd.DataFrame,
                module_risk: pd.DataFrame | None = None,
                policy_unmapped: pd.DataFrame | None = None) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        report_card.to_excel(xw, sheet_name="Report_Card", index=False)
        last_active.to_excel(xw, sheet_name="Last_Active", index=False)
        if module_risk is not None:
            module_risk.to_excel(xw, sheet_name="Module_Risk", index=False)
        # tiny summary page instead of title rows on data tabs
        pd.DataFrame({"Note":[f"Training Attempts Report • built {now_stamp()}"]}).to_excel(
            xw, sheet_name="Summary", index=False
        )
    # Also emit policy_unmapped CSV beside workbook (if any)
    if policy_unmapped is not None and len(policy_unmapped):
        csv_path = out_path.with_name(f"policy_unmapped_modules_{now_stamp()}.csv")
        policy_unmapped.to_csv(csv_path, index=False)


# -------------------------------
# CLI
# -------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Build Training Attempts Excel (Report_Card, Last_Active, Module_Risk).")
    p.add_argument("--uap", required=True, help="Path to SR04-Trn_Att CSV/XLSX (raw/agg).")
    p.add_argument("--findname", required=True, help="Path to FindName.csv mapping.")
    p.add_argument("--out-xlsx", required=True, help="Output Excel path (.xlsx).")
    p.add_argument("--emp-mstr", help="HR master XLSX for Emp_* fields.")
    p.add_argument("--role-area-policy", help="Role/Area/Policy map CSV/XLSX.")
    p.add_argument("--req-matrix", help="Role→Module requirements CSV/XLSX (Option B).")
    return p.parse_args()


def main():
    args = parse_args()

    uap_path = Path(args.uap)
    findname_path = Path(args.findname)
    out_path = Path(args.out_xlsx)

    # Load core
    uap_df = load_uap_attempts(uap_path)
    fn_df  = load_findname(findname_path)

    # HR enrich (if provided)
    if args.emp_mstr:
        hr_df  = load_hr_master_robust(Path(args.emp_mstr))
        report_card = attach_hr_fields(uap_df, fn_df, hr_df)
    else:
        report_card = uap_df.copy()

    # Ensure date-only columns exist (safety even if already present)
    report_card = add_date_only(report_card, "FirstAttempt_date", "FirstAttempt_D")
    report_card = add_date_only(report_card, "LastAttempt_date",  "LastAttempt_D")

    # Last Active (attach HR if present)
    last_active = build_last_active(report_card)
    if args.emp_mstr:
        # join back the Emp_* on User
        emp_cols = [c for c in report_card.columns if c.startswith("Emp_")]
        last_active = last_active.merge(
            report_card[["User.Full Name"] + emp_cols].drop_duplicates("User.Full Name"),
            on="User.Full Name", how="left"
        )

    # Option B: Module risk via policy + reqs
    module_risk = None
    policy_unmapped = None
    if args.role_area_policy and args.req_matrix:
        policy_df = load_role_area_policy_map(Path(args.role_area_policy))
        req_df    = load_requirements_raw(Path(args.req_matrix))
        module_risk, policy_unmapped = build_module_risk(policy_df, req_df)

    # Write workbook + unmapped CSV (if any)
    write_excel(out_path, report_card, last_active, module_risk, policy_unmapped)

    print(f"[OK] Wrote: {out_path}")
    if policy_unmapped is not None and len(policy_unmapped):
        print("  -> Also wrote policy_unmapped_modules_*.csv next to the workbook.")


if __name__ == "__main__":
    pd.options.display.width = 200
    pd.options.display.max_columns = 200
    main()
