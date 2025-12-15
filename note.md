uploaded the files 

unzip /workspaces/edx_trn_att/edx_trn_att_scaffold_20251029_1200.zip -d /workspaces/edx_trn_att
@srajagopalan1204 ➜ /workspaces/edx_trn_att (main) $ unzip /workspaces/edx_trn_att/edx_trn_att_scaffold_20251029_1200.zip -d /workspaces/edx_trn_att
Archive:  /workspaces/edx_trn_att/edx_trn_att_scaffold_20251029_1200.zip
   creating: /workspaces/edx_trn_att/Config/
   creating: /workspaces/edx_trn_att/Docs/
   creating: /workspaces/edx_trn_att/Inputs/
   creating: /workspaces/edx_trn_att/Outputs/
   creating: /workspaces/edx_trn_att/Scripts/
   creating: /workspaces/edx_trn_att/Session_Checklists/
  inflating: /workspaces/edx_trn_att/.gitignore  
  inflating: /workspaces/edx_trn_att/requirements.txt  
replace /workspaces/edx_trn_att/README.md? [y]es, [n]o, [A]ll, [N]one, [r]ename: y
  inflating: /workspaces/edx_trn_att/README.md  
  inflating: /workspaces/edx_trn_att/Inputs/README.md  
  inflating: /workspaces/edx_trn_att/Outputs/.gitkeep  
  inflating: /workspaces/edx_trn_att/Scripts/train_report.py  
  inflating: /workspaces/edx_trn_att/Config/settings.json5  
  inflating: /workspaces/edx_trn_att/Config/role_module_map.xlsx  
  inflating: /workspaces/edx_trn_att/Session_Checklists/runcheck_latest.html  
  inflating: /workspaces/edx_trn_att/Docs/SOP_Run_Report.md  

  pip install -r requirements.txt 

  @srajagopalan1204 ➜ /workspaces/edx_trn_att (main) $ pip install -r requirements.txt
Requirement already satisfied: pandas in /home/codespace/.local/lib/python3.12/site-packages (from -r requirements.txt (line 1)) (2.3.1)
Requirement already satisfied: numpy in /home/codespace/.local/lib/python3.12/site-packages (from -r requirements.txt (line 2)) (2.3.1)
Requirement already satisfied: XlsxWriter in /usr/local/python/3.12.1/lib/python3.12/site-packages (from -r requirements.txt (line 3)) (3.2.9)
Collecting openpyxl (from -r requirements.txt (line 4))
  Downloading openpyxl-3.1.5-py2.py3-none-any.whl.metadata (2.5 kB)
Requirement already satisfied: python-dateutil>=2.8.2 in /home/codespace/.local/lib/python3.12/site-packages (from pandas->-r requirements.txt (line 1)) (2.9.0.post0)
Requirement already satisfied: pytz>=2020.1 in /home/codespace/.local/lib/python3.12/site-packages (from pandas->-r requirements.txt (line 1)) (2025.2)
Requirement already satisfied: tzdata>=2022.7 in /home/codespace/.local/lib/python3.12/site-packages (from pandas->-r requirements.txt (line 1)) (2025.2)
Collecting et-xmlfile (from openpyxl->-r requirements.txt (line 4))
  Downloading et_xmlfile-2.0.0-py3-none-any.whl.metadata (2.7 kB)
Requirement already satisfied: six>=1.5 in /home/codespace/.local/lib/python3.12/site-packages (from python-dateutil>=2.8.2->pandas->-r requirements.txt (line 1)) (1.17.0)
Downloading openpyxl-3.1.5-py2.py3-none-any.whl (250 kB)
Downloading et_xmlfile-2.0.0-py3-none-any.whl (18 kB)
Installing collected packages: et-xmlfile, openpyxl
Successfully installed et-xmlfile-2.0.0 openpyxl-3.1.5

[notice] A new release of pip is available: 25.1.1 -> 25.3
[notice] To update, run: python3 -m pip install --upgrade pip

  python Scripts/train_report.py


  the above worked but we received the employee file 
  so a merged report is being planned 
  where is the emp list file located on the laptop 
  "C:\Users\scottuser\Documents\SonetLumier\UAP_ref\Emp_lists\All_Employees_New_Hires_Terms_10_29_25.xlsx" 
  where is source file that was UAP report 
SR04-Trn_Att is run on the UAP site created by subi