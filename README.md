# edx_trn_att

UAP-based Training Attainment Management

You run this to:
- Generate per-user training report cards
- Flag exceptions / coaching needs
- Track last activity
- Score module risk (which content is failing people)
- Build an R / N / Z requirement matrix (role vs module expectations)
- Produce a session run checklist so you can stop and restart without losing context

## How you run it (Codespaces)

1. Put the newest training attempt CSV in `Inputs/`  
   Example name: `SR04-Trn_Att_20251029_1019.csv`

2. (Optional) Update `Config/role_module_map.xlsx`  
   Mark each module as:
   - R = Required
   - N = Nice to have
   - Z = Not expected in that role

3. In the Codespaces terminal:
```bash
python Scripts/train_report.py
```

4. Look in `Outputs/` for the generated Excel:
   `Training_Attempts_Report_YYYYMMDD_HHMM.xlsx`

   Tabs include:
   - Report_Card
   - Exceptions
   - Last_Active
   - Module_Risk
   - Requirement_Matrix
   - Source_Columns

5. Check `Session_Checklists/runcheck_latest.html`
   - This is your "resume checklist" so you can pick up next time without re-remembering everything.

## Environment
- You are running in GitHub Codespaces with Python 3.
- Office PC can run PowerShell + Excel 2019 macros for printing or manager handouts.
- Home machine is restricted, so heavy lifting stays in Codespaces, and you just download the Excel.

/workspaces/edx_trn_att/Scripts/build_full_report_wrkd_251105.py the version that is mentioned below 

TS=$(date +%Y%m%d_%H%M)
python Scripts/build_full_report.py \
  --uap Inputs/SR04-Trn_Att_20251029_0905.csv \
  --findname Transi/FindName.csv \
  --out-xlsx "Outputs/Training_Attempts_Report_${TS}.xlsx" \
  --emp-mstr emp_mstr/All_Employees_New_Hires_Terms_10_29_25.xlsx \
  --role-area-policy Inputs/classify/role_area_policy_normalized_20251031_1623_v2.csv \
  --req-matrix Inputs/req_matrix/role_module_requirements_seed_20251031_1623_v2.csv \
  --dnr /workspaces/edx_trn_att/Outputs/DNR_110325_1914.csv



check the output file to ensure all tabs have data 

python Scripts/inspect_excel_preview.py \
  --in /workspaces/edx_trn_att/Outputs/Training_Attempts_Report_20251103_1602.xlsx \
  --rows 5

copy and paste to an excel sheet and convert text to column for ease of view

next time t o add the summary 



after 110525 1454
Scripts/lib/timeutil_v4.py added 
Scripts/merge_module_catalog_v4.py
enrich_uap_with_catalog_v4.py