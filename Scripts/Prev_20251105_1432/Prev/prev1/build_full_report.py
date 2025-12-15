import argparse
import glob
import os
import json
from datetime import datetime, date
from pathlib import Path

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

SETTINGS_FILE = CONFIG_DIR / "settings.json5"
ROLE_MAP_FILE = CONFIG_DIR / "role_module_map.xlsx"


# -------------------------------------------------
# CONFIG LOADER (same idea as train_report.py)
# -------------------------------------------------
def load_settings():
    """
    settings.json5 allows // comments. We'll strip full-line //.
    """
    txt = SETTINGS_FILE.read_text(encoding="utf-8")
    cleaned_lines = []
    for line in txt.splitlines():
        s = line.strip()
        if s.startswith("//"):
            continue
        cleaned_lines.append(line)
    cleaned_txt = "\n".join(cleaned_lines)
    return json.loads(cleaned_txt)


# -------------------------------------------------
# TRAINING ATTEMPTS LOADER
# -------------------------------------------------
def get_latest_attempt_csv():
    """
    Grab most recent CSV in Inputs/.
    """
    csvs = glob.glob(str(INPUT_DIR / "*.csv"))
    if not csvs:
        raise FileNotFoundError(
            "No CSV files found in Inputs/. Please drop SR04-Trn_Att_YYYYMMDD_HHMM.csv"
        )
    csvs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return csvs[0]


def load_attempts_df(pass_string="passed"):
    """
    Load training attempts CSV and normalize columns.
    Adds match_name for joining with employee master.
    Calculates Score_valid and Passed_Flag.
    """
    attempts_path = get_latest_attempt_csv()
    df = pd.read_csv(attempts_path)

    # required columns sanity check
    expect_cols = [
        "User.Full Name",
        "Version Name",
        "Start Time",
    ]
    for col in expect_cols:
        if col not in df.columns:
            raise KeyError(f"Expected column '{col}' in attempts CSV, not found.")

    # parse timestamp
    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce")

    # numeric score
    if "Score" in df.columns:
        df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
        df["Score_valid"] = df["Score"].where(df["Score"] >= 0, np.nan)
    else:
        df["Score_valid"] = np.nan

    # passed flag
    if "Result" in df.columns:
        df["Result_norm"] = df["Result"].astype(str).str.lower().str.strip()
        df["Passed_Flag"] = df["Result_norm"].eq(str(pass_string).lower())
    else:
        df["Passed_Flag"] = False

    # match_name for join
    df["match_name"] = (
        df["User.Full Name"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    return df, attempts_path


# -------------------------------------------------
# EMPLOYEE MASTER LOADER
# -------------------------------------------------
def detect_header_row(df, required_cols):
    """
    The HR file has the real header row somewhere near the top (first ~10 rows)
    and may repeat that header again in later sections.

    We'll scan first 10 rows: find a row where ALL required_cols appear
    (exact string match) across that row.
    """
    # df here is read with header=None so rows are raw
    scan_rows = min(10, df.shape[0])
    for r in range(scan_rows):
        row_vals = df.iloc[r].astype(str).tolist()
        # see if each required col is present in this row
        if all(req in row_vals for req in required_cols):
            return r
    # fallback: assume row 0
    return 0


def load_emp_master(emp_path: Path):
    """
    1. Read the Excel with header=None so we see all rows.
    2. Detect which row is the true header (Name, Location, etc.).
    3. Promote that row to columns.
    4. Drop repeated header rows inside the data.
    5. Normalize/rename columns we care about.
    6. Create match_name for joining to attempts.
    """

    # read raw so we can detect the header ourselves
    raw = pd.read_excel(emp_path, header=None)

    # columns we expect to see in the REAL header row
    required_cols = [
        "Name",
        "Location",
        "Location Number",
        "Postion/Title",
        "Role Name",
        "Role Description",
        "Infor ID",
        "Ad Ons",
        "Manager",
        "Email",
        "Primary Functional Group",
    ]

    header_row_idx = detect_header_row(raw, required_cols)

    # now re-read using that row as header
    emp = pd.read_excel(emp_path, header=header_row_idx)

    # drop any rows that are just repeat headers (where 'Name' literally equals "Name")
    emp = emp[emp["Name"].notna()]
    emp = emp[emp["Name"].astype(str).str.strip().str.upper() != "NAME"]

    # build match_name for join
    emp["match_name"] = (
        emp["Name"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # rename HR columns we'll carry forward
    rename_map = {
        "Name": "Emp_Name",
        "Location": "Emp_Location",
        "Location Number": "Emp_LocationNumber",
        "Postion/Title": "Emp_Title",  # keep their spelling but map to Emp_Title
        "Role Name": "Emp_RoleName",
        "Role Description": "Emp_RoleDescription",
        "Infor ID": "Emp_InforID",
        "Ad Ons": "Emp_AdOns",
        "Manager": "Emp_Manager",
        "Email": "Emp_Email",
        "Primary Functional Group": "Emp_PrimaryFunctionalGroup",
    }

    # only rename if column exists (future-proofing)
    for old_col in list(rename_map.keys()):
        if old_col not in emp.columns:
            rename_map.pop(old_col)
    emp = emp.rename(columns=rename_map)

    # keep only the useful fields so we don't explode width
    keep_cols = [
        "match_name",
        "Emp_Name",
        "Emp_InforID",
        "Emp_Manager",
        "Emp_Title",
        "Emp_RoleName",
        "Emp_RoleDescription",
        "Emp_PrimaryFunctionalGroup",
        "Emp_Location",
        "Emp_LocationNumber",
        "Emp_Email",
        "Emp_AdOns",
    ]
    keep_cols = [c for c in keep_cols if c in emp.columns]
    emp = emp[keep_cols].copy()

    # de-dupe on match_name (first occurrence wins)
    emp = emp.drop_duplicates(subset=["match_name"], keep="first")

    return emp


# -------------------------------------------------
# MERGE ATTEMPTS + EMP INFO
# -------------------------------------------------
def attach_emp_info(attempts_df, emp_df):
    """
    Merge employee info onto each attempt row using match_name.
    """
    merged = attempts_df.merge(
        emp_df,
        how="left",
        on="match_name",
        suffixes=("", "_emp"),
    )
    # for traceability: mark whether we matched someone in HR
    merged["Emp_MatchFound"] = np.where(
        merged["Emp_Name"].notna(), True, False
    )
    return merged


# -------------------------------------------------
# BUILD REPORT CARD TAB
# -------------------------------------------------
def build_report_card(enriched_df):
    """
    Per user + module rollup with attempts, scores, pass flag, timestamps.
    We ALSO add the employee context columns for that user.
    """
    user_col = "User.Full Name"
    module_col = "Version Name"

    # prep group summary
    summary = (
        enriched_df.groupby([user_col, module_col]).agg(
            Attempts=("Start Time", "count"),
            FirstAttempt=("Start Time", "min"),
            LastAttempt=("Start Time", "max"),
            MinScore=("Score_valid", "min"),
            MaxScore=("Score_valid", "max"),
            EverPassed=("Passed_Flag", "any"),
        )
        .reset_index()
    )

    # format timestamps readable
    summary["FirstAttempt_date"] = summary["FirstAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    summary["LastAttempt_date"] = summary["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")

    # we want to attach 1 row of employee info per user.
    emp_cols = [
        c
        for c in enriched_df.columns
        if c.startswith("Emp_")
    ]
    emp_lookup = (
        enriched_df[[user_col] + emp_cols]
        .drop_duplicates(subset=[user_col])
    )

    report_card = summary.merge(
        emp_lookup,
        how="left",
        on=user_col,
    )

    # reorder columns for readability
    front_cols = [
        user_col,
        "Emp_InforID",
        "Emp_Manager",
        "Emp_PrimaryFunctionalGroup",
        "Emp_Title",
        "Emp_RoleName",
        "Emp_RoleDescription",
        module_col,
        "Attempts",
        "MinScore",
        "MaxScore",
        "FirstAttempt_date",
        "LastAttempt_date",
        "EverPassed",
    ]

    # keep only what exists
    front_cols = [c for c in front_cols if c in report_card.columns]
    report_card = report_card[front_cols].sort_values(
        [user_col, module_col]
    ).reset_index(drop=True)

    return report_card


# -------------------------------------------------
# BUILD EXCEPTIONS TAB (who needs help)
# -------------------------------------------------
def build_exceptions(enriched_df, flag_score_min=70):
    """
    People/modules who haven't passed or have low scores.
    We also attach manager/role/etc. so you know who owns the fix.
    """
    user_col = "User.Full Name"
    module_col = "Version Name"

    summary = (
        enriched_df.groupby([user_col, module_col]).agg(
            Attempts=("Start Time", "count"),
            FirstAttempt=("Start Time", "min"),
            LastAttempt=("Start Time", "max"),
            MaxScore=("Score_valid", "max"),
            EverPassed=("Passed_Flag", "any"),
        )
        .reset_index()
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

    # keep only the flagged rows
    mask_flag = (
        (summary["EverPassed"] == False)
        | (summary["MaxScore"].isna())
        | (summary["MaxScore"].fillna(-1) < flag_score_min)
    )
    flagged = summary.loc[mask_flag].copy()

    # attach emp info by user
    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lookup = (
        enriched_df[[user_col] + emp_cols]
        .drop_duplicates(subset=[user_col])
    )

    flagged = flagged.merge(emp_lookup, how="left", on=user_col)

    keep_cols = [
        user_col,
        "Emp_Manager",
        "Emp_PrimaryFunctionalGroup",
        "Emp_Title",
        "Emp_RoleName",
        module_col,
        "Attempts",
        "MaxScore",
        "EverPassed",
        "LastAttempt_date",
        "Reason",
    ]
    keep_cols = [c for c in keep_cols if c in flagged.columns]
    flagged = flagged[keep_cols].sort_values(
        [user_col, "EverPassed", "MaxScore"]
    ).reset_index(drop=True)

    return flagged


# -------------------------------------------------
# BUILD LAST_ACTIVE TAB
# -------------------------------------------------
def build_last_active(enriched_df):
    """
    For each user:
    - most recent Start Time
    - plus manager / role / etc.
    - plus DaysSinceActive to prep the AR aging buckets in Excel
    """
    user_col = "User.Full Name"

    last_touch = (
        enriched_df.groupby(user_col)["Start Time"]
        .max()
        .reset_index()
        .rename(columns={"Start Time": "LastActivity"})
    )

    last_touch["LastActivity_date"] = last_touch["LastActivity"].dt.strftime(
        "%Y-%m-%d %H:%M"
    )

    # DaysSinceActive = TODAY() - last active date
    # We'll use local system date() for "today".
    today_floor = pd.Timestamp(date.today())
    last_touch["DaysSinceActive"] = (
        today_floor - last_touch["LastActivity"].dt.floor("D")
    ).dt.days

    emp_cols = [c for c in enriched_df.columns if c.startswith("Emp_")]
    emp_lookup = (
        enriched_df[[user_col] + emp_cols]
        .drop_duplicates(subset=[user_col])
    )

    last_active = last_touch.merge(emp_lookup, how="left", on=user_col)

    # reorder
    keep_cols = [
        user_col,
        "Emp_InforID",
        "Emp_Manager",
        "Emp_PrimaryFunctionalGroup",
        "Emp_Title",
        "Emp_RoleName",
        "Emp_RoleDescription",
        "Emp_Location",
        "Emp_LocationNumber",
        "LastActivity_date",
        "DaysSinceActive",
    ]
    keep_cols = [c for c in keep_cols if c in last_active.columns]
    last_active = last_active[keep_cols].sort_values(user_col).reset_index(drop=True)

    return last_active


# -------------------------------------------------
# MODULE_RISK TAB
# -------------------------------------------------
def build_module_risk(enriched_df):
    """
    One row per module:
    - how many unique users touched it,
    - how many passed,
    - avg attempts to pass,
    - avg time to pass,
    - avg passing score.
    """
    user_col = "User.Full Name"
    module_col = "Version Name"

    stats_rows = []
    for module, g_mod in enriched_df.groupby(module_col):
        users_opened = set(g_mod[user_col].dropna().unique().tolist())
        succeeded_users = set(
            g_mod.loc[g_mod["Passed_Flag"], user_col].dropna().unique().tolist()
        )
        passing_scores = g_mod.loc[g_mod["Passed_Flag"], "Score_valid"].dropna().tolist()

        user_to_attempts_to_pass = []
        user_to_time_to_pass_hours = []

        # per user inside this module
        for u, g_u in g_mod.groupby(user_col):
            g_u_sorted = g_u.sort_values("Start Time").reset_index(drop=True)
            if g_u_sorted.empty:
                continue

            first_attempt_time = g_u_sorted["Start Time"].min()

            pass_rows = g_u_sorted.index[g_u_sorted["Passed_Flag"] == True].tolist()
            if pass_rows:
                first_pass_idx = pass_rows[0]
                first_pass_time = g_u_sorted.loc[first_pass_idx, "Start Time"]

                attempts_before_pass = first_pass_idx + 1  # idx is 0-based
                user_to_attempts_to_pass.append(attempts_before_pass)

                delta_hours = (
                    first_pass_time - first_attempt_time
                ).total_seconds() / 3600.0
                user_to_time_to_pass_hours.append(delta_hours)

        users_opened_ct = len(users_opened)
        users_succeeded_ct = len(succeeded_users)
        users_not_passed_ct = users_opened_ct - users_succeeded_ct

        conversion_pct = (
            100.0 * (users_succeeded_ct / users_opened_ct)
            if users_opened_ct > 0
            else np.nan
        )

        def safe_stats(lst):
            if not lst:
                return (np.nan, np.nan, np.nan)
            return (min(lst), float(np.mean(lst)), max(lst))

        att_min, att_avg, att_max = safe_stats(user_to_attempts_to_pass)
        tmin, tavg, tmax = safe_stats(user_to_time_to_pass_hours)

        def safe_score_stats(lst):
            if not lst:
                return (np.nan, np.nan, np.nan)
            return (min(lst), float(np.mean(lst)), max(lst))

        smin, savg, smax = safe_score_stats(passing_scores)

        stats_rows.append({
            "Module": module,
            "UsersOpened": users_opened_ct,
            "UsersSucceeded": users_succeeded_ct,
            "UsersStillNotPassed": users_not_passed_ct,
            "ConversionPct": conversion_pct,
            "AttemptsToPass_Min": att_min,
            "AttemptsToPass_Avg": att_avg,
            "AttemptsToPass_Max": att_max,
            "TimeToPassHrs_Min": tmin,
            "TimeToPassHrs_Avg": tavg,
            "TimeToPassHrs_Max": tmax,
            "PassScore_Min": smin,
            "PassScore_Avg": savg,
            "PassScore_Max": smax,
        })

    module_risk_df = pd.DataFrame(stats_rows).sort_values("Module").reset_index(drop=True)
    return module_risk_df


# -------------------------------------------------
# REQUIREMENT_MATRIX TAB
# -------------------------------------------------
def build_requirement_matrix():
    """
    Pivot role_module_map.xlsx (manually maintained) into Role_or_Dept vs Module_Name -> ReqFlag (R/N/Z).
    """
    if not ROLE_MAP_FILE.exists():
        return pd.DataFrame()

    role_map = pd.read_excel(ROLE_MAP_FILE)
    needed = {"Role_or_Dept", "Module_Name", "ReqFlag"}
    if not needed.issubset(role_map.columns):
        return pd.DataFrame()

    matrix = role_map.pivot_table(
        index="Role_or_Dept",
        columns="Module_Name",
        values="ReqFlag",
        aggfunc="first"
    )
    matrix = matrix.reset_index()
    return matrix


# -------------------------------------------------
# WRITE OUTPUT EXCEL
# -------------------------------------------------
def write_output_excel(
    report_card,
    exceptions_view,
    last_active_view,
    module_risk_df,
    req_matrix_df,
    enriched_df,
    attempts_path_used,
    emp_path_used,
):
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    out_xlsx = OUTPUT_DIR / f"Training_Attempts_Report_{ts}.xlsx"

    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
        report_card.to_excel(writer, sheet_name="Report_Card", index=False)
        exceptions_view.to_excel(writer, sheet_name="Exceptions", index=False)
        last_active_view.to_excel(writer, sheet_name="Last_Active", index=False)
        module_risk_df.to_excel(writer, sheet_name="Module_Risk", index=False)
        req_matrix_df.to_excel(writer, sheet_name="Requirement_Matrix", index=False)

        # Metadata / trace
        meta_df = pd.DataFrame({
            "Source_AttemptCSV": [str(attempts_path_used)],
            "Source_EmployeeMaster": [str(emp_path_used)],
            "Generated_Timestamp": [ts],
        })
        meta_df.to_excel(writer, sheet_name="Run_Metadata", index=False)

        # Also include source columns for audit (like train_report did)
        pd.DataFrame({"ColumnName": enriched_df.columns.tolist()}).to_excel(
            writer, sheet_name="Source_Columns", index=False
        )

    return out_xlsx


# -------------------------------------------------
# UPDATE SESSION CHECKLIST HTML
# -------------------------------------------------
def update_session_checklist(out_xlsx_path, project_start_ny):
    """
    Stamp runcheck_latest.html with most recent run info and cycle time.
    We'll do a simple timestamp and a rough hour-diff since project_start_ny.
    project_start_ny is like "2025-10-29T07:36:00-04:00"
    We'll parse the first 19 chars "YYYY-MM-DDTHH:MM:SS" (ignore tz math).
    """
    latest_file = CHECKLIST_DIR / "runcheck_latest.html"
    if not latest_file.exists():
        # nothing to update, just return
        return None

    # parse project start naive
    base_part = project_start_ny[:19]
    start_dt = datetime.strptime(base_part, "%Y-%m-%dT%H:%M:%S")
    now_local = datetime.now()
    cycle_hours = (now_local - start_dt).total_seconds() / 3600.0

    tsstamp = now_local.strftime("%Y-%m-%d %H:%M")
    rel_xlsx = os.path.relpath(out_xlsx_path, BASE_DIR)

    html_latest = latest_file.read_text(encoding="utf-8")

    injected_block = f"""
    <p><strong>Most recent run:</strong> {tsstamp} local time</p>
    <p><strong>Latest output workbook:</strong> {rel_xlsx}</p>
    <p><strong>Cycle time since 2025-10-29 07:36 AM ET:</strong> {cycle_hours:.2f} hours</p>
    """

    marker = "(will be updated by train_report.py)"
    if marker in html_latest:
        html_latest = html_latest.replace(marker, injected_block)
    else:
        # append before </body>
        html_latest = html_latest.replace(
            "</body>",
            injected_block + "\n</body>"
        )

    # archive a timestamped copy
    ts_for_name = now_local.strftime("%Y%m%d_%H%M")
    archive_path = CHECKLIST_DIR / f"runcheck_{ts_for_name}.html"
    archive_path.write_text(html_latest, encoding="utf-8")

    # overwrite latest
    latest_file.write_text(html_latest, encoding="utf-8")

    return archive_path


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Build full training attainment report with HR enrichment, "
            "and output Excel dashboards."
        )
    )
    parser.add_argument(
        "--emp-mstr",
        required=True,
        help="Path to employee master Excel file, e.g. emp_mstr/All_Employees_New_Hires_Terms_10_29_25.xlsx"
    )
    args = parser.parse_args()

    emp_path = Path(args.emp_mstr)
    if not emp_path.exists():
        raise FileNotFoundError(f"Employee master file not found: {emp_path}")

    settings = load_settings()

    # 1. load attempts
    attempts_df, attempts_path_used = load_attempts_df(
        pass_string=settings.get("pass_string", "passed")
    )

    # 2. load emp master
    emp_df = load_emp_master(emp_path)

    # 3. merge
    enriched_df = attach_emp_info(attempts_df, emp_df)

    # 4. build tabs
    report_card = build_report_card(enriched_df)
    exceptions_view = build_exceptions(
        enriched_df,
        flag_score_min=settings.get("flag_score_min", 70),
    )
    last_active_view = build_last_active(enriched_df)
    module_risk_df = build_module_risk(enriched_df)
    req_matrix_df = build_requirement_matrix()

    # 5. write Excel output
    out_xlsx = write_output_excel(
        report_card,
        exceptions_view,
        last_active_view,
        module_risk_df,
        req_matrix_df,
        enriched_df,
        attempts_path_used,
        emp_path,
    )

    print("[INFO] Wrote Excel report:", out_xlsx)

    # 6. update session checklist HTML / archive
    update_session_checklist(
        out_xlsx_path=out_xlsx,
        project_start_ny=settings.get(
            "project_start_ny", "2025-10-29T07:36:00-04:00"
        ),
    )

    # 7. produce unmatched list for cleanup (bonus)
    unmatched = (
        enriched_df.loc[~enriched_df["Emp_MatchFound"]]
        .groupby("User.Full Name", dropna=True)
        .size()
        .reset_index(name="RowCount")
        .sort_values("RowCount", ascending=False)
    )
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    unmatched_path = OUTPUT_DIR / f"unmatched_names_{ts}.csv"
    unmatched.to_csv(unmatched_path, index=False)

    print("[INFO] Wrote unmatched list:", unmatched_path)
    print("[INFO] Done.")


if __name__ == "__main__":
    main()
