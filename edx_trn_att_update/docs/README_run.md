# UAP Training Attainment — Updated Scripts (103125_1653)

## Quick start
```bash
cd edx_trn_att_update

# 1) Name matching (worklist → approvals → apply)
python Scripts/name_translate.py --uap Inputs/uap/SR04-Trn_Att.csv --emp-mstr "emp_mstr/*.xlsx"
# -> edit Transi/FindName_Resp_Worklist_<ts>.csv, set Flag=U and HR_Name for approved rows
python Scripts/name_translate.py --update yes --worklist Transi/FindName_Resp_Worklist_<ts>.csv

# 2) Build enriched workbook (with DTR classification and policy optional)
python Scripts/build_full_report.py   --uap Inputs/uap/SR04-Trn_Att.csv   --emp-mstr emp_mstr/All_Employees_New_Hires_Terms_10_29_25.xlsx   --findname Transi/FindName.csv   --exclude-dnr yes   --classify "Inputs/classify/*.csv"   --policy Inputs/req_matrix/Requirement_Area_Policy.csv   --out-xlsx Outputs/Training_Attempts_Report_103125_1653.xlsx

# 3) Add Executive Summary
python Scripts/insert_exec_summary.py --in-xlsx Outputs/Training_Attempts_Report_103125_1653.xlsx
```

## Notes
- Pivots and dashboards are created manually or via VBA outside these scripts.
- Module_Risk can slice by Entity/SOP/SubSOP if DTR classification is provided.
