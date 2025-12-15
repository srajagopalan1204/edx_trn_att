# Scripts/build_module_risk_from_enriched_v4.py
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np, pandas as pd
from lib.timeutil_v4 import ts_ny

def load_enriched(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    need = ["User.Full Name","Version Name","Start Time","Entity","SOP","SubSOP"]
    miss = [c for c in need if c not in df.columns]
    if miss: raise SystemExit(f"[ERR] Enriched UAP missing columns: {miss}")
    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce")
    df["Score"] = pd.to_numeric(df.get("Score", np.nan), errors="coerce")
    if "Result" in df.columns:
        df["Passed_Flag"] = df["Result"].astype(str).str.lower().str.strip().eq("passed")
    elif "EverPassed" in df.columns:
        df["Passed_Flag"] = df["EverPassed"].astype(bool)
    else:
        df["Passed_Flag"] = False
    return df

def _module_metrics(g: pd.DataFrame) -> dict:
    users = g["User.Full Name"].dropna().unique().tolist()
    opened = len(users)
    succ_users = g.loc[g["Passed_Flag"], "User.Full Name"].dropna().unique().tolist()
    succ = len(succ_users)

    att_to_pass, hrs_to_pass = [], []
    for u, gu in g.sort_values("Start Time").groupby("User.Full Name"):
        idxs = gu.index[gu["Passed_Flag"]].tolist()
        if idxs:
            i = idxs[0]
            first_t = gu["Start Time"].min()
            pass_t = gu.loc[i, "Start Time"]
            att_to_pass.append(gu.index.get_loc(i) + 1)
            hrs_to_pass.append((pass_t - first_t).total_seconds()/3600)

    def stats(lst):
        return (np.nan, np.nan, np.nan) if not lst else (min(lst), float(np.mean(lst)), max(lst))

    att_min, att_avg, att_max = stats(att_to_pass)
    tmin, tavg, tmax = stats(hrs_to_pass)

    # explicit success-only time avg (same as tavg, but clearer name)
    tavg_success_only = tavg

    pass_scores = g.loc[g["Passed_Flag"], "Score"].dropna().tolist()
    smin, savg, smax = stats(pass_scores)

    conv = 100.0*succ/opened if opened else np.nan

    return dict(
        UsersOpened=opened, UsersSucceeded=succ,
        UsersStillNotPassed=(opened - succ) if opened else np.nan,
        ConversionPct=conv,
        AttemptsToPass_Min=att_min, AttemptsToPass_Avg=att_avg, AttemptsToPass_Max=att_max,
        TimeToPassHrs_Min=tmin, TimeToPassHrs_Avg=tavg, TimeToPassHrs_Max=tmax,
        TimeToPassHrs_Avg_SuccessOnly=tavg_success_only,
        PassScore_Min=smin, PassScore_Avg=savg, PassScore_Max=smax
    )

def main():
    ap = argparse.ArgumentParser(description="Compute Module Risk from enriched UAP (overall + by Entity/SOP/SubSOP).")
    ap.add_argument("--enriched", required=True)
    ap.add_argument("--out-dir", default="Outputs")
    ap.add_argument("--excel", action="store_true")
    args = ap.parse_args()

    df = load_enriched(Path(args.enriched))

    rows = []
    for mod, g in df.groupby("Version Name"):
        m = _module_metrics(g)
        rows.append({"Module": mod, **m})
    overall = pd.DataFrame(rows).sort_values("Module")

    rows2 = []
    for keys, g in df.groupby(["Module","Entity","SOP","SubSOP"], dropna=False):
        # if "Module" not present (some enrichers use Version Name only), fall back
        if isinstance(keys, tuple):
            mod = keys[0] if pd.notna(keys[0]) else g["Version Name"].iloc[0]
            ent, sop, sub = keys[1], keys[2], keys[3]
        else:
            mod, ent, sop, sub = keys, None, None, None
        m = _module_metrics(g)
        rows2.append({"Module": mod, "Entity": ent, "SOP": sop, "SubSOP": sub, **m})
    by_policy = pd.DataFrame(rows2).sort_values(["Entity","SOP","SubSOP","Module"], na_position="last")

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    ts = ts_ny()
    p1 = out_dir / f"Module_Risk_From_Enriched_{ts}.csv"
    p2 = out_dir / f"Module_Risk_By_Policy_From_Enriched_{ts}.csv"
    overall.to_csv(p1, index=False)
    by_policy.to_csv(p2, index=False)
    print(f"[INFO] Wrote: {p1}")
    print(f"[INFO] Wrote: {p2}")

    if args.excel:
        x = out_dir / f"Module_Risk_Package_{ts}.xlsx"
        with pd.ExcelWriter(x, engine="xlsxwriter") as w:
            overall.to_excel(w, sheet_name="Module_Risk", index=False)
            by_policy.to_excel(w, sheet_name="Module_Risk_By_Policy", index=False)
        print(f"[INFO] Wrote: {x}")

    print(f"[SUMMARY] Rows: {len(df):,} | Modules (overall): {overall.shape[0]} | By-Policy rows: {by_policy.shape[0]}")

if __name__ == "__main__":
    main()
