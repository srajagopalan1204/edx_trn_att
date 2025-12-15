# Scripts/build_full_report_v4.py
from __future__ import annotations
import argparse, json, os
from pathlib import Path
from datetime import date
import numpy as np, pandas as pd
from lib.timeutil_v4 import ts_ny

BASE = Path(__file__).resolve().parent.parent
TRANSI = BASE / "Transi"
OUT = BASE / "Outputs"

def load_settings(path=BASE/"Config/settings.json5") -> dict:
    if not path.exists(): return {}
    txt = path.read_text(encoding="utf-8")
    cleaned = "\n".join([ln for ln in txt.splitlines() if not ln.strip().startswith("//")])
    try: return json.loads(cleaned or "{}")
    except Exception: return {}

def norm_name(s) -> str:
    if pd.isna(s): return ""
    s = str(s).upper().replace("."," ").strip()
    return " ".join(s.split())

def load_name_overrides(path: Path) -> dict:
    if not path.exists(): return {}
    df = pd.read_csv(path)
    maps = {}
    def add(a,b):
        if a in df.columns and b in df.columns:
            for _,r in df[[a,b]].dropna().iterrows():
                u = norm_name(r[a]); e = norm_name(r[b])
                if u and e: maps[u]=e
    add("User_Full_Name","Emp_Name")
    add("User.Full Name","HR_Name")
    return maps

def backup_findname():
    src = TRANSI / "FindName.csv"
    if src.exists():
        prev = TRANSI / "Prev"
        prev.mkdir(parents=True, exist_ok=True)
        ts = ts_ny()
        (prev / f"FindName_{ts}.csv").write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"[INFO] Backed up FindName to Transi/Prev/FindName_{ts}.csv")
    else:
        print("[WARN] FindName.csv not found; continuing.")

def load_uap(uap: Path, pass_string="passed") -> pd.DataFrame:
    df = pd.read_csv(uap)
    need = ["User.Full Name","Version Name","Start Time"]
    miss = [c for c in need if c not in df.columns]
    if miss: raise SystemExit(f"[ERR] Attempts CSV missing columns: {miss}")
    df["Start Time"] = pd.to_datetime(df["Start Time"], errors="coerce")
    df["Score_valid"] = pd.to_numeric(df.get("Score", np.nan), errors="coerce")
    if "Result" in df.columns:
        df["Passed_Flag"] = df["Result"].astype(str).str.lower().str.strip().eq(str(pass_string).lower())
    else:
        df["Passed_Flag"] = False
    df["match_name"] = df["User.Full Name"].map(norm_name)
    return df

def apply_overrides(df: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    if overrides:
        df["match_name"] = df["match_name"].apply(lambda x: overrides.get(x,x))
    return df

def load_dnr(path: Path) -> tuple[set[str], pd.DataFrame]:
    if not path.exists(): return set(), pd.DataFrame(columns=["User.Full Name"])
    d = pd.read_csv(path)
    if "Exclude" not in d.columns:
        # infer: exclude 'Y' for obvious term/inactive; else N
        hs = d.get("HR_Status", pd.Series(dtype=str)).astype(str).str.upper().str.strip()
        d["Exclude"] = np.where(hs.isin({"TERMINATED","INACTIVE"}), "Y", "N")
        print("[WARN] DNR missing 'Exclude' — inferred from HR_Status (TERMINATED/INACTIVE→Y, else N).")
    names = set(d["User.Full Name"].astype(str).map(norm_name))
    return names, d

def apply_dnr(df: pd.DataFrame, dnr_df: pd.DataFrame) -> pd.DataFrame:
    if dnr_df.empty: return df
    d = dnr_df.copy()
    d["__match"] = d["User.Full Name"].map(norm_name)
    exc = set(d.loc[d["Exclude"].astype(str).str.upper().eq("Y"), "__match"])
    before = len(df)
    out = df[~df["match_name"].isin(exc)].copy()
    print(f"[INFO] DNR applied (Exclude='Y'): removed {before-len(out)} rows for {len(exc)} names.")
    return out

def load_emp_master(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    hdr = 0
    req = ["Name","Location","Location Number","Postion/Title","Role Name","Role Description",
           "Infor ID","Ad Ons","Manager","Email","Primary Functional Group"]
    for r in range(min(10, raw.shape[0])):
        vals = raw.iloc[r].astype(str).tolist()
        if all(x in vals for x in req): hdr=r; break
    emp = pd.read_excel(path, header=hdr)
    emp = emp[emp["Name"].notna()]
    emp["match_name"] = emp["Name"].map(norm_name)
    ren = {
        "Name":"Emp_Name","Location":"Emp_Location","Location Number":"Emp_LocationNumber",
        "Postion/Title":"Emp_Title","Role Name":"Emp_RoleName","Role Description":"Emp_RoleDescription",
        "Infor ID":"Emp_InforID","Ad Ons":"Emp_AdOns","Manager":"Emp_Manager","Email":"Emp_Email",
        "Primary Functional Group":"Emp_PrimaryFunctionalGroup",
    }
    emp = emp.rename(columns={k:v for k,v in ren.items() if k in emp.columns})
    keep = ["match_name","Emp_Name","Emp_InforID","Emp_Manager","Emp_Title","Emp_RoleName",
            "Emp_RoleDescription","Emp_PrimaryFunctionalGroup","Emp_Location","Emp_LocationNumber","Emp_Email","Emp_AdOns"]
    emp = emp[[c for c in keep if c in emp.columns]].drop_duplicates("match_name")
    return emp

def attach_emp_info(attempts: pd.DataFrame, emp: pd.DataFrame | None) -> pd.DataFrame:
    if emp is None or emp.empty:
        out = attempts.copy(); out["Emp_Name"]=np.nan; return out
    return attempts.merge(emp, on="match_name", how="left")

def add_date_only(df: pd.DataFrame, src: str, dest: str):
    ts = pd.to_datetime(df[src], errors="coerce")
    df[dest] = ts.dt.date

def report_card(df: pd.DataFrame) -> pd.DataFrame:
    u, m = "User.Full Name", "Version Name"
    g = (df.groupby([u,m]).agg(
        Attempts=("Start Time","count"),
        FirstAttempt=("Start Time","min"),
        LastAttempt=("Start Time","max"),
        MinScore=("Score_valid","min"), MaxScore=("Score_valid","max"),
        EverPassed=("Passed_Flag","any")
    ).reset_index())
    g["FirstAttempt_date"] = g["FirstAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    g["LastAttempt_date"]  = g["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    add_date_only(g,"FirstAttempt","FirstAttempt_D")
    add_date_only(g,"LastAttempt","LastAttempt_D")
    emp_cols = [c for c in df.columns if c.startswith("Emp_")]
    lu = df[[u]+emp_cols].drop_duplicates(u)
    out = g.merge(lu, on=u, how="left")
    cols = [u,"Emp_InforID","Emp_Manager","Emp_PrimaryFunctionalGroup","Emp_Title","Emp_RoleName",
            "Emp_RoleDescription","Emp_Location","Emp_LocationNumber", m,"Attempts","MinScore","MaxScore",
            "FirstAttempt_date","FirstAttempt_D","LastAttempt_date","LastAttempt_D","EverPassed"]
    return out[[c for c in cols if c in out.columns]].sort_values([u,m]).reset_index(drop=True)

def exceptions_view(df: pd.DataFrame, min_score=70) -> pd.DataFrame:
    u,m="User.Full Name","Version Name"
    g=(df.groupby([u,m]).agg(
        Attempts=("Start Time","count"),
        FirstAttempt=("Start Time","min"),
        LastAttempt=("Start Time","max"),
        MaxScore=("Score_valid","max"),
        EverPassed=("Passed_Flag","any")
    ).reset_index())
    g["Reason"] = g.apply(lambda r: ", ".join(
        [x for x in [
            ("no score" if pd.isna(r["MaxScore"]) else None),
            (f"score <{min_score}" if pd.notna(r["MaxScore"]) and r["MaxScore"]<min_score else None),
            ("no pass" if not r["EverPassed"] else None)
        ] if x]), axis=1)
    g["LastAttempt_date"] = g["LastAttempt"].dt.strftime("%Y-%m-%d %H:%M")
    emp_cols = [c for c in df.columns if c.startswith("Emp_")]
    lu = df[[u]+emp_cols].drop_duplicates(u)
    out = g.merge(lu, on=u, how="left")
    keep = [u,"Emp_Manager","Emp_PrimaryFunctionalGroup","Emp_Title","Emp_RoleName","Emp_Location","Emp_LocationNumber",
            m,"Attempts","MaxScore","EverPassed","LastAttempt_date","Reason"]
    return out[[c for c in keep if c in out.columns]].sort_values([u,"EverPassed","MaxScore"]).reset_index(drop=True)

def last_active(df: pd.DataFrame) -> pd.DataFrame:
    u="User.Full Name"
    lt = df.groupby(u)["Start Time"].max().reset_index().rename(columns={"Start Time":"LastActivity"})
    lt["LastActivity_date"] = lt["LastActivity"].dt.strftime("%Y-%m-%d %H:%M")
    lt["LastActivity_D"] = lt["LastActivity"].dt.date
    today = pd.Timestamp(date.today())
    lt["DaysSinceActive"] = (today - lt["LastActivity"].dt.floor("D")).dt.days
    emp_cols = [c for c in df.columns if c.startswith("Emp_")]
    lu = df[[u]+emp_cols].drop_duplicates(u)
    out = lt.merge(lu, on=u, how="left")
    keep=[u,"Emp_InforID","Emp_Manager","Emp_PrimaryFunctionalGroup","Emp_Title","Emp_RoleName","Emp_RoleDescription",
          "Emp_Location","Emp_LocationNumber","LastActivity_date","LastActivity_D","DaysSinceActive"]
    return out[[c for c in keep if c in out.columns]].sort_values(u).reset_index(drop=True)

def module_metrics(g: pd.DataFrame) -> dict:
    u="User.Full Name"
    users=set(g[u].dropna().unique().tolist())
    opened=len(users)
    succ=set(g.loc[g["Passed_Flag"],u].dropna().unique().tolist())
    att_to_pass=[]; hrs_to_pass=[]
    for name,gu in g.sort_values("Start Time").groupby(u):
        idx = gu.index[gu["Passed_Flag"]].tolist()
        if idx:
            i=idx[0]
            first=gu["Start Time"].min()
            passed=gu.loc[i,"Start Time"]
            att_to_pass.append(gu.index.get_loc(i)+1)
            hrs_to_pass.append((passed-first).total_seconds()/3600)
    def stats(lst):
        return (np.nan,np.nan,np.nan) if not lst else (min(lst),float(np.mean(lst)),max(lst))
    att_min,att_avg,att_max = stats(att_to_pass)
    tmin,tavg,tmax = stats(hrs_to_pass)
    tavg_success_only = tavg
    pass_scores = g.loc[g["Passed_Flag"],"Score_valid"].dropna().tolist()
    smin,savg,smax = stats(pass_scores)
    conv = 100.0*len(succ)/opened if opened else np.nan
    return dict(
        UsersOpened=opened, UsersSucceeded=len(succ),
        UsersStillNotPassed=(opened-len(succ)) if opened else np.nan,
        ConversionPct=conv,
        AttemptsToPass_Min=att_min, AttemptsToPass_Avg=att_avg, AttemptsToPass_Max=att_max,
        TimeToPassHrs_Min=tmin, TimeToPassHrs_Avg=tavg, TimeToPassHrs_Max=tmax,
        TimeToPassHrs_Avg_SuccessOnly=tavg_success_only,
        PassScore_Min=smin, PassScore_Avg=savg, PassScore_Max=smax
    )

def build_module_risk(df: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for mod,g in df.groupby("Version Name"):
        rows.append({"Module":mod, **module_metrics(g)})
    return pd.DataFrame(rows).sort_values("Module")

def load_catalog_latest() -> pd.DataFrame | None:
    latest = TRANSI / "Module_Catalog_LATEST.csv"
    if latest.exists():
        cat = pd.read_csv(latest)
        need=["Module","Entity","SOP","SubSOP"]
        miss=[c for c in need if c not in cat.columns]
        if miss:
            print(f"[WARN] LATEST catalog missing {miss}; ignoring.")
            return None
        cat["__key"] = cat["Module"].astype(str).str.strip().str.lower()
        return cat
    # fallback to newest timestamped
    found = sorted((BASE/"Outputs/ModMstr").glob("module_catalog_*.csv"))
    if not found:
        print("[WARN] No module catalog found."); return None
    cat = pd.read_csv(found[-1]); 
    cat["__key"]=cat["Module"].astype(str).str.strip().str.lower()
    return cat

def build_module_risk_by_policy(df: pd.DataFrame, catalog: pd.DataFrame | None) -> pd.DataFrame:
    if catalog is None: return pd.DataFrame()
    tmp = df.copy()
    tmp["__key"] = tmp["Version Name"].astype(str).str.strip().str.lower()
    tmp = tmp.merge(catalog[["__key","Entity","SOP","SubSOP"]], on="__key", how="left")
    rows=[]
    for keys,g in tmp.groupby(["Module","Entity","SOP","SubSOP"], dropna=False):
        mod,ent,sop,sub = keys
        if pd.isna(mod): mod = g["Version Name"].iloc[0]
        rows.append({"Module":mod,"Entity":ent,"SOP":sop,"SubSOP":sub, **module_metrics(g)})
    return pd.DataFrame(rows).sort_values(["Entity","SOP","SubSOP","Module"], na_position="last")

def write_excel(path: Path, rc: pd.DataFrame, exc: pd.DataFrame, la: pd.DataFrame,
                mr: pd.DataFrame, mrp: pd.DataFrame, src_attempts: Path, emp_used: Path | None):
    with pd.ExcelWriter(path, engine="xlsxwriter") as w:
        rc.to_excel(w, sheet_name="Report_Card", index=False)
        exc.to_excel(w, sheet_name="Exceptions", index=False)
        la.to_excel(w, sheet_name="Last_Active", index=False)
        mr.to_excel(w, sheet_name="Module_Risk", index=False)
        if not mrp.empty:
            mrp.to_excel(w, sheet_name="Module_Risk_By_Policy", index=False)
        meta = pd.DataFrame({
            "Source_AttemptCSV":[str(src_attempts)],
            "Source_EmployeeMaster":[str(emp_used or "")],
            "Generated_Timestamp":[ts_ny()],
        })
        meta.to_excel(w, sheet_name="Run_Metadata", index=False)
    return path

def main():
    ap = argparse.ArgumentParser(description="Build full training report (v4).")
    ap.add_argument("--uap", required=True)
    ap.add_argument("--findname", required=True)
    ap.add_argument("--emp-mstr", help="Employee master Excel")
    ap.add_argument("--dnr", help="DNR CSV (with Exclude flag preferred)")
    ap.add_argument("--out-xlsx", required=True)
    args = ap.parse_args()

    settings = load_settings()
    backup_findname()

    uap = load_uap(Path(args.uap), pass_string=settings.get("pass_string","passed"))
    overrides = load_name_overrides(Path(args.findname))
    uap = apply_overrides(uap, overrides)

    # DNR
    if args.dnr:
        _, dnr_df = load_dnr(Path(args.dnr))
        uap = apply_dnr(uap, dnr_df)

    # HR
    emp_df=None; emp_path=None
    if args.emp_mstr:
        emp_path = Path(args.emp_mstr)
        emp_df = load_emp_master(emp_path)

    enriched = attach_emp_info(uap, emp_df)

    rc = report_card(enriched)
    exc = exceptions_view(enriched, min_score=settings.get("flag_score_min",70))
    la = last_active(enriched)
    mr = build_module_risk(enriched)
    catalog = load_catalog_latest()
    mrp = build_module_risk_by_policy(enriched.assign(Module=enriched["Version Name"]), catalog)

    out_xlsx = write_excel(Path(args.out_xlsx), rc, exc, la, mr, mrp, Path(args.uap), emp_path)
    print(f"[INFO] Wrote Excel report: {out_xlsx}")
    # unmatched names (if HR provided)
    if emp_df is not None:
        unmatched=(enriched.loc[enriched["Emp_Name"].isna()]
                   .groupby("User.Full Name", dropna=True).size()
                   .reset_index(name="RowCount").sort_values("RowCount", ascending=False))
        p = OUT / f"unmatched_names_{ts_ny()}.csv"; OUT.mkdir(parents=True, exist_ok=True)
        unmatched.to_csv(p, index=False); print(f"[INFO] Wrote unmatched list: {p}")
    print("[INFO] Done.")

if __name__ == "__main__":
    main()
