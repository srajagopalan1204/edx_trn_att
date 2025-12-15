# Inputs

Drop your raw training attempt export(s) here.

Expected format:
- CSV file containing columns like:
  - User.Full Name
  - User.UserName
  - User.Department (or Role/Location/etc.)
  - Version Name  (training module / lesson)
  - Start Time
  - Score
  - Result       ("passed", "failed", "unknown"...)
  - Status       ("completed", "incomplete"...)

Naming convention:
SR04-Trn_Att_YYYYMMDD_HHMM.csv

The script will automatically grab the most recent CSV in this folder.
