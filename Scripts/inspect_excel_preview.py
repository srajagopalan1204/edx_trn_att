#!/usr/bin/env python3
"""
inspect_excel_preview.py

Read an Excel workbook and, for each sheet, print:
- Sheet name
- Column headers
- First N rows (default 5)

Also (optionally) emit a single, timestamped CSV that:
- Includes one HEADER row per sheet with ColumnList
- Includes first N data rows per sheet
- Ends with a SOURCE row that records the input file path
# 1) Save a markdown preview (as before) + write timestamped CSV into ./Outputs/
python inspect_excel_preview.py --in "/path/to/workbook.xlsx" \
  --rows 5 \
  --out "Outputs/workbook_preview.md" \
  --csv-out "Outputs/"

# 2) Write CSV to an explicit file path; {ts} expands to timestamp
python inspect_excel_preview.py --in "/path/to/workbook.xlsx" \
  --rows 10 \
  --csv-out "Outputs/workbook_preview_{ts}.csv"

# 3) Only CSV output, no markdown file
python inspect_excel_preview.py --in "/path/to/workbook.xlsx" --csv-out "Outputs/"

"""

import argparse
from pathlib import Path
from datetime import datetime
import sys
import pandas as pd

def preview_sheet_markdown(xl_path: Path, sheet_name: str, n: int) -> str:
    """Return a markdown block for one sheet."""
    df = pd.read_excel(xl_path, sheet_name=sheet_name, nrows=max(n, 1), dtype=str)
    df.columns = [str(c) for c in df.columns]

    lines = []
    lines.append(f"## Sheet: `{sheet_name}`")
    if df.empty:
        lines.append(f"_No rows found in the first {n} rows._")
        return "\n".join(lines) + "\n"

    col_list = ", ".join([f"`{c}`" for c in df.columns])
    lines.append(f"**Columns ({len(df.columns)}):** {col_list}")

    df_disp = df.fillna("")
    try:
        md_table = df_disp.to_markdown(index=False)
        lines.append("")
        lines.append(md_table)
        lines.append("")
    except Exception:
        headers = list(df_disp.columns)
        lines.append("")
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for _, row in df_disp.iterrows():
            lines.append("| " + " | ".join(str(x) for x in row.tolist()) + " |")
        lines.append("")
    return "\n".join(lines)

def build_long_csv_blocks(xl_path: Path, sheet_names: list[str], n: int) -> pd.DataFrame:
    """
    Build one long CSV with rows:
      - HEADER: one per sheet (ColumnList populated)
      - DATA: first N rows from sheet (RowIndex 1..N)
      - SOURCE: one final row with Source_File
    Also includes all data columns (union across sheets).
    """
    # First pass: discover union of all columns
    union_cols = set()
    per_sheet_frames = []
    for sheet in sheet_names:
        try:
            df = pd.read_excel(xl_path, sheet_name=sheet, nrows=max(n, 1))
            df.columns = [str(c) for c in df.columns]
            union_cols.update(df.columns)
            per_sheet_frames.append((sheet, df))
        except Exception:
            # still add an empty HEADER row for visibility
            per_sheet_frames.append((sheet, pd.DataFrame()))

    union_cols = list(union_cols)  # stable-ish order

    # Build rows
    rows = []
    for sheet, df in per_sheet_frames:
        # HEADER row
        col_list = "; ".join(list(df.columns)) if not df.empty else ""
        rows.append({
            "Sheet": sheet,
            "RowType": "HEADER",
            "RowIndex": "",
            "ColumnList": col_list,
            # data columns left blank for HEADER
            **{c: "" for c in union_cols},
        })

        # DATA rows
        if not df.empty:
            df2 = df.head(n).copy()
            df2.columns = [str(c) for c in df2.columns]
            df2 = df2.fillna("")

            # For each data row, place values in union columns, others blank
            for i, (_, r) in enumerate(df2.iterrows(), start=1):
                data_dict = {c: "" for c in union_cols}
                for c in df2.columns:
                    data_dict[c] = r[c]
                rows.append({
                    "Sheet": sheet,
                    "RowType": "DATA",
                    "RowIndex": i,
                    "ColumnList": "",
                    **data_dict
                })

    # SOURCE trailer row
    rows.append({
        "Sheet": "__ALL__",
        "RowType": "SOURCE",
        "RowIndex": "",
        "ColumnList": "",
        **({c: "" for c in union_cols} | {"Source_File": str(xl_path.resolve())})
    })

    # Ensure Source_File column exists (for trailer)
    df_out = pd.DataFrame(rows)
    if "Source_File" not in df_out.columns:
        df_out["Source_File"] = ""
    # Column ordering
    leading = ["Sheet", "RowType", "RowIndex", "ColumnList", "Source_File"]
    rest = [c for c in df_out.columns if c not in leading]
    return df_out[leading + rest]

def resolve_csv_out(csv_out_arg: str | None, xl_path: Path) -> Path | None:
    if not csv_out_arg:
        return None
    p = Path(csv_out_arg)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    if p.suffix == "":  # likely a directory
        p.mkdir(parents=True, exist_ok=True)
        return p / f"{xl_path.stem}_preview_{ts}.csv"
    # If user gave a file path, ensure dir exists
    p.parent.mkdir(parents=True, exist_ok=True)
    # If filename has placeholder {ts}, expand it
    if "{ts}" in p.name:
        return p.with_name(p.name.replace("{ts}", ts))
    return p

def main():
    ap = argparse.ArgumentParser(description="Preview Excel sheets with headers and first N rows; optional CSV output.")
    ap.add_argument("--in", dest="in_path", required=True, help="Path to the Excel file (.xlsx/.xlsm/.xls).")
    ap.add_argument("--rows", type=int, default=5, help="Number of rows to show per sheet (default: 5).")
    ap.add_argument("--out", help="Optional path to write a markdown report (.md).")
    ap.add_argument("--csv-out", help="Optional CSV output (file path or directory). If directory, auto-names with timestamp.")
    args = ap.parse_args()

    xl_path = Path(args.in_path)
    if not xl_path.exists():
        print(f"[ERROR] File not found: {xl_path}", file=sys.stderr)
        sys.exit(1)

    try:
        xl = pd.ExcelFile(xl_path)
    except Exception as e:
        print(f"[ERROR] Could not open Excel file: {e}", file=sys.stderr)
        sys.exit(2)

    # ----- Markdown preview to stdout (and optional --out) -----
    blocks = [f"# Excel Preview: {xl_path.name}\n", f"_Sheets detected: {len(xl.sheet_names)}_\n"]
    for sheet in xl.sheet_names:
        try:
            blocks.append(preview_sheet_markdown(xl_path, sheet, args.rows))
        except Exception as e:
            blocks.append(f"## Sheet: `{sheet}`\n[ERROR] Failed to read sheet: {e}\n")
    report = "\n".join(blocks)
    print(report)

    if args.out:
        out_md = Path(args.out)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text(report, encoding="utf-8")
        print(f"\n[INFO] Wrote markdown report -> {out_md}")

    # ----- CSV long preview -----
    if args.csv_out:
        out_csv = resolve_csv_out(args.csv_out, xl_path)
        df_long = build_long_csv_blocks(xl_path, xl.sheet_names, args.rows)
        df_long.to_csv(out_csv, index=False, encoding="utf-8")
        print(f"[INFO] Wrote CSV preview -> {out_csv}")

if __name__ == "__main__":
    main()
