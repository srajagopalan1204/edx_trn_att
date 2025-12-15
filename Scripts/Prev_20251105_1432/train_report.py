import os
import glob
from datetime import datetime
import json
import numpy as np
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # repo root
INPUT_DIR = BASE_DIR / "Inputs"
OUTPUT_DIR = BASE_DIR / "Outputs"
CONFIG_DIR = BASE_DIR / "Config"
CHECKLIST_DIR = BASE_DIR / "Session_Checklists"

SETTINGS_FILE = CONFIG_DIR / "settings.json5"
ROLE_MAP_FILE = CONFIG_DIR / "role_module_map.xlsx"

# --- Utility: load config ---
def load_settings():
    txt = SETTINGS_FILE.read_text(encoding="utf-8")
    cleaned = []
    for line in txt.splitlines():
        line_stripped = line.strip()
        if line_stripped.startswith("//"):
            continue
        cleaned.append(line)
    cleaned_txt = "\n".join(cleaned)
    return json.loads(cleaned_txt)

# --- Utility: pick latest CSV from Inputs ---
def get_latest_attempt_csv():
    csvs = glob.glob(str(INPUT_DIR / "*.csv"))
    if not csvs:
        raise FileNotFoundError("No CSV files found in Inputs/. Please drop SR04-Trn_Att_YYYYMMDD_HHMM.csv")
    csvs.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return csvs[0]

# --- Core data prep ---
def prep_attempts(df, pass_string="passed"):
    if "Start Time" not in df.columns:
        raise KeyError("Expected column 'Start Time' not found in input CSV.")
    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce")

    # numeric score and valid score
    if "Score" in df.columns:
        df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
        df["Score_valid"] = df["Score"].where(df["Score"] >= 0, np.nan)
    else:
        df["Score_valid"] = np.nan

    # Passed_Flag based on Result
    if "Result" in df.columns:
        df["Result_norm"] = df["Result"].astype(str).str.lower().str.strip()
        df["Passed_Flag"] = df["Result_norm"].eq(pass_string.lower())
    else:
        df["Passed_Flag"] = False

    return df

# --- Build per-user/per-module summary (Report_Card etc.) ---
def build_user_module_summary(df):
    user_col = "User.Full Name"
    module_col = "Version Name"
    for col in [user_col, module_col]:
        if col not in df.columns:
            raise KeyError(f"Expected column '{col}' not found.")

    group_cols = [user_col, module_col]

    summary = (
        df.groupby(group_cols).agg(
            Attempts=("Start Time", "count"),
            FirstAttempt=("Start Time", "min"),
            LastAttempt=("Start Time", "max"),
            MinScore=("Score_valid", "min"),
            MaxScore=("Score_valid", "max"),
            EverPassed=("Passed_Flag", "any"),
        )
        .reset_index()
    )

    summary["FirstAttempt_date"] = summary["FirstAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    summary["LastAttempt_date"] = summary["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")

    report_card = summary[
        [user_col, module_col, "Attempts", "MinScore", "MaxScore",
         "FirstAttempt_date", "LastAttempt_date", "EverPassed"]
    ].sort_values([user_col, module_col]).reset_index(drop=True)

    # Exceptions view
    def build_reason(row):
        reasons = []
        maxscore = row["MaxScore"]
        passed = row["EverPassed"]
        if pd.isna(maxscore):
            reasons.append("no score")
        elif maxscore < 70:
            reasons.append("score <70")
        if not passed:
            reasons.append("no pass")
        return ", ".join(reasons) if reasons else ""

    exceptions = summary.copy()
    exceptions["Reason"] = exceptions.apply(build_reason, axis=1)
    mask_flag = (
        (exceptions["EverPassed"] == False)
        | (exceptions["MaxScore"].fillna(-1) < 70)
        | (exceptions["MaxScore"].isna())
    )

    exceptions_flagged = exceptions.loc[mask_flag].copy()
    exceptions_flagged["LastAttempt_date"] = exceptions_flagged["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")

    exceptions_view = exceptions_flagged[
        [user_col, module_col, "Attempts", "MaxScore",
         "EverPassed", "LastAttempt_date", "Reason"]
    ].sort_values([user_col, "EverPassed", "MaxScore"]).reset_index(drop=True)

    # Last Active
    last_active = (
        df.groupby(user_col)["Start Time"]
        .max()
        .reset_index()
        .rename(columns={"Start Time": "LastActivity"})
    )
    last_active["LastActivity_date"] = last_active["LastActivity"].dt.strftime("%Y-%m-%d %H:%M")
    last_active_view = last_active[
        [user_col, "LastActivity_date"]
    ].sort_values(user_col).reset_index(drop=True)

    return report_card, exceptions_view, last_active_view

# --- Module Risk Dashboard ---
def build_module_risk(df):
    user_col = "User.Full Name"
    module_col = "Version Name"
    module_stats = []

    for module, g_mod in df.groupby(module_col):
        users_opened = set(g_mod[user_col].dropna().unique().tolist())
        passing_scores = g_mod.loc[g_mod["Passed_Flag"], "Score_valid"].dropna().tolist()
        succeeded_users = set(
            g_mod.loc[g_mod["Passed_Flag"], user_col].dropna().unique().tolist()
        )

        user_to_attempts_to_pass = []
        user_to_time_to_pass_hours = []

        for u, g_um in g_mod.groupby(user_col):
            g_um_sorted = g_um.sort_values("Start Time").reset_index(drop=True)
            first_attempt_time = g_um_sorted["Start Time"].min()

            pass_rows = g_um_sorted.index[g_um_sorted["Passed_Flag"] == True].tolist()
            if pass_rows:
                first_pass_idx = pass_rows[0]
                first_pass_time = g_um_sorted.loc[first_pass_idx, "Start Time"]

                attempts_before_pass = first_pass_idx + 1  # idx is 0-based
                user_to_attempts_to_pass.append(attempts_before_pass)

                delta_hours = (first_pass_time - first_attempt_time).total_seconds() / 3600.0
                user_to_time_to_pass_hours.append(delta_hours)

        users_opened_ct = len(users_opened)
        users_succeeded = len(succeeded_users)
        users_not_passed = users_opened_ct - users_succeeded

        conversion_pct = (
            100.0 * (users_succeeded / users_opened_ct)
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

        module_stats.append({
            "Module": module,
            "UsersOpened": users_opened_ct,
            "UsersSucceeded": users_succeeded,
            "UsersStillNotPassed": users_not_passed,
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

    module_risk_df = pd.DataFrame(module_stats).sort_values("Module").reset_index(drop=True)
    return module_risk_df

# --- R / N / Z Requirement Matrix placeholder ---
def build_requirement_matrix():
    if not ROLE_MAP_FILE.exists():
        return pd.DataFrame()
    role_map = pd.read_excel(ROLE_MAP_FILE)
    needed_cols = {"Role_or_Dept", "Module_Name", "ReqFlag"}
    if not needed_cols.issubset(role_map.columns):
        return pd.DataFrame()
    matrix = role_map.pivot_table(
        index="Role_or_Dept",
        columns="Module_Name",
        values="ReqFlag",
        aggfunc="first"
    )
    matrix = matrix.reset_index()
    return matrix

# --- Write Excel ---
def write_output_excel(report_card, exceptions_view, last_active_view,
                       module_risk_df, req_matrix_df, df_source):
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    out_xlsx_path = OUTPUT_DIR / f"Training_Attempts_Report_{ts}.xlsx"
    with pd.ExcelWriter(out_xlsx_path, engine="xlsxwriter") as writer:
        report_card.to_excel(writer, sheet_name="Report_Card", index=False)
        exceptions_view.to_excel(writer, sheet_name="Exceptions", index=False)
        last_active_view.to_excel(writer, sheet_name="Last_Active", index=False)
        module_risk_df.to_excel(writer, sheet_name="Module_Risk", index=False)
        req_matrix_df.to_excel(writer, sheet_name="Requirement_Matrix", index=False)
        pd.DataFrame({"ColumnName": df_source.columns.tolist()}).to_excel(
            writer, sheet_name="Source_Columns", index=False
        )
    return out_xlsx_path

# --- Update session checklist HTML ---
def write_session_checklist(out_xlsx_path, project_start_ny):
    # project_start_ny looks like "2025-10-29T07:36:00-04:00"
    base_part = project_start_ny[:19]  # "YYYY-MM-DDTHH:MM:SS"
    start_dt = datetime.strptime(base_part, "%Y-%m-%dT%H:%M:%S")
    now_local = datetime.now()
    cycle_hours = (now_local - start_dt).total_seconds() / 3600.0

    tsstamp = now_local.strftime("%Y-%m-%d %H:%M")
    rel_xlsx = os.path.relpath(out_xlsx_path, BASE_DIR)

    latest_path = CHECKLIST_DIR / "runcheck_latest.html"
    html_latest = latest_path.read_text(encoding="utf-8")

    ts_for_name = now_local.strftime("%Y%m%d_%H%M")
    archive_path = CHECKLIST_DIR / f"runcheck_{ts_for_name}.html"

    injected_block = f"""        <p><strong>Most recent run:</strong> {tsstamp} local time</p>
    <p><strong>Latest output workbook:</strong> {rel_xlsx}</p>
    <p><strong>Cycle time since 2025-10-29 07:36 AM ET:</strong> {cycle_hours:.2f} hours</p>
    """

    marker = "(will be updated by train_report.py)"
    if marker in html_latest:
        html_latest = html_latest.replace(marker, injected_block)
    else:
        html_latest = html_latest.replace(
            "</body>",
            injected_block + "\n</body>"
        )

    archive_path.write_text(html_latest, encoding="utf-8")
    latest_path.write_text(html_latest, encoding="utf-8")

    return archive_path

def main():
    settings = load_settings()
    csv_path = get_latest_attempt_csv()
    print(f"[INFO] Using input CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    df = prep_attempts(df, pass_string=settings.get("pass_string", "passed"))

    report_card, exceptions_view, last_active_view = build_user_module_summary(df)
    module_risk_df = build_module_risk(df)
    req_matrix_df = build_requirement_matrix()

    out_xlsx = write_output_excel(
        report_card,
        exceptions_view,
        last_active_view,
        module_risk_df,
        req_matrix_df,
        df
    )

    print(f"[INFO] Wrote Excel report: {out_xlsx}")

    archive_checklist = write_session_checklist(
        out_xlsx_path=out_xlsx,
        project_start_ny=settings.get("project_start_ny", "2025-10-29T07:36:00-04:00")
    )
    print(f"[INFO] Updated session checklist: {archive_checklist}")
    print("[INFO] Done.")

if __name__ == "__main__":
    main()
