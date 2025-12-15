#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_module_translation.py

Purpose
-------
Builds a translation/worklist that links UAP attempts (CSV) to the
"SR04 Document Tracking Report v2" (XLSX) so each module can inherit
Entity/SOP/SubSOP via Parent.Context from the document catalog.

Strategy
--------
1) Load two inputs:
   - UAP CSV (must contain: Document.Name, Document.File Name, Document.Parent.Name)
   - DOC XLSX (must contain: Name, File Name, Parent.Name, Parent.Context)
2) Normalize text (lowercase, trim, collapse spaces, strip extensions for filename compare).
3) Try matches in this order (highest priority first):
   A) Exact: UAP.Document.FileName == DOC.File Name
   B) Exact: UAP.Document.Name == DOC.Name (normalized)
   C) Exact: UAP.Document.Parent.Name == DOC.Parent.Name
   D) Containment/partial: token overlap between names (>=0.9 ratio) or filename stem containment
   E) Fuzzy-ish (SequenceMatcher ratio) >= 0.85 on names
4) For each distinct UAP module key (Document.Name if present else Version Name),
   pick the best candidate (highest priority, then best score).
5) Emit outputs:
   - {out}/module_translation_candidates_{TS}.csv
   - {out}/uap_modules_unmatched_{TS}.csv
   - {out}/doc_modules_unmatched_{TS}.csv
   - {out}/FindModuleContext_Worklist_{TS}.csv (template you can edit and re-import later)
"""

import argparse
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict, Any

import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import re

PRIORITY_ORDER = ["EXACT_FILENAME", "EXACT_NAME", "EXACT_PARENT", "PARTIAL", "FUZZY"]

def norm(s: str) -> str:
    if pd.isna(s):
        return ""
    t = str(s).strip().lower()
    # collapse whitespace
    t = " ".join(t.split())
    return t

def filename_stem(s: str) -> str:
    """Strip common UDC suffixing and extension; return lowercase stem."""
    s = norm(s)
    if not s:
        return ""
    # remove extension
    if "." in s:
        s = s.rsplit(".", 1)[0]
    # drop typical version hash bits inside parentheses, e.g. "name (1)"
    s = re.sub(r"\(\d+\)$", "", s).strip()
    return s

def token_ratio(a: str, b: str) -> float:
    """Simple token-set Jaccard-like ratio."""
    ta = set(norm(a).split())
    tb = set(norm(b).split())
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0

def seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def load_uap(uap_path: Path) -> pd.DataFrame:
    df = pd.read_csv(uap_path)
    # Prefer these columns if present
    needed_any = ["Document.Name", "Document.File Name", "Document.Parent.Name", "Version Name"]
    # We only error if ALL likely fields are missing
    if all(c not in df.columns for c in ["Document.Name", "Version Name"]):
        raise KeyError(f"UAP file must have at least 'Document.Name' or 'Version Name'. Columns: {list(df.columns)}")

    # Build normalized keys
    df["UAP_Name"] = df["Document.Name"] if "Document.Name" in df.columns else df["Version Name"]
    df["UAP_Name"] = df["UAP_Name"].astype(str)

    df["UAP_FileName"] = df["Document.File Name"].astype(str) if "Document.File Name" in df.columns else ""
    df["UAP_ParentName"] = df["Document.Parent.Name"].astype(str) if "Document.Parent.Name" in df.columns else ""

    df["UAP_Name_norm"] = df["UAP_Name"].map(norm)
    df["UAP_File_norm"] = df["UAP_FileName"].map(filename_stem)
    df["UAP_Parent_norm"] = df["UAP_ParentName"].map(norm)

    return df

def load_doc(doc_path: Path) -> pd.DataFrame:
    df = pd.read_excel(doc_path)
    for col in ["Name", "File Name", "Parent.Name", "Parent.Context"]:
        if col not in df.columns:
            raise KeyError(f"Document report missing column '{col}'. Columns: {list(df.columns)}")
    # Normalized keys
    df["DOC_Name_norm"] = df["Name"].astype(str).map(norm)
    df["DOC_File_norm"] = df["File Name"].astype(str).map(filename_stem)
    df["DOC_Parent_norm"] = df["Parent.Name"].astype(str).map(norm)
    return df

def best_match_for(u: Dict[str, Any], doc_df: pd.DataFrame) -> Tuple[str, float, pd.Series]:
    """
    Returns (match_type, score, doc_row). If none found, match_type='NONE', score=0, doc_row=None.
    Priority & thresholds:
      - EXACT_FILENAME: exact stem match (score 1.0)
      - EXACT_NAME    : exact normalized name match (1.0)
      - EXACT_PARENT  : exact normalized parent name (1.0)
      - PARTIAL       : token overlap >= 0.90 OR filename stem containment (0.9)
      - FUZZY         : SequenceMatcher >= 0.85 on names
    """
    uname = u["UAP_Name"]
    uname_norm = u["UAP_Name_norm"]
    ufile = u["UAP_File_norm"]
    upar  = u["UAP_Parent_norm"]

    # A) exact filename stem
    if ufile:
        exact_file = doc_df[doc_df["DOC_File_norm"] == ufile]
        if not exact_file.empty:
            return ("EXACT_FILENAME", 1.0, exact_file.iloc[0])

    # B) exact doc name
    exact_name = doc_df[doc_df["DOC_Name_norm"] == uname_norm]
    if not exact_name.empty:
        return ("EXACT_NAME", 1.0, exact_name.iloc[0])

    # C) exact parent name (fallback)
    if upar:
        exact_parent = doc_df[doc_df["DOC_Parent_norm"] == upar]
        if not exact_parent.empty:
            return ("EXACT_PARENT", 1.0, exact_parent.iloc[0])

    # D) partial: token overlap, or filename containment
    partial_rows = []
    if uname_norm:
        tok_scores = doc_df["DOC_Name_norm"].map(lambda s: token_ratio(uname_norm, s))
        cand = doc_df.loc[tok_scores >= 0.90].copy()
        if not cand.empty:
            cand["__score"] = tok_scores.loc[cand.index]
            partial_rows.append(cand)

    if ufile:
        contains = doc_df["DOC_File_norm"].map(lambda s: (ufile in s) or (s in ufile))
        cand2 = doc_df[contains].copy()
        if not cand2.empty:
            cand2["__score"] = 0.90
            partial_rows.append(cand2)

    if partial_rows:
        allp = pd.concat(partial_rows, ignore_index=True)
        best_idx = allp["__score"].astype(float).idxmax()
        row = allp.loc[best_idx]
        return ("PARTIAL", float(row["__score"]), row.drop(labels=["__score"]))

    # E) fuzzy: SequenceMatcher on names >= 0.85
    if uname_norm:
        seq_scores = doc_df["DOC_Name_norm"].map(lambda s: SequenceMatcher(None, uname_norm, s).ratio())
        cand3 = doc_df.loc[seq_scores >= 0.85].copy()
        if not cand3.empty:
            cand3["__score"] = seq_scores.loc[cand3.index]
            best_idx = cand3["__score"].astype(float).idxmax()
            row = cand3.loc[best_idx]
            return ("FUZZY", float(row["__score"]), row.drop(labels=["__score"]))

    return ("NONE", 0.0, None)

def main():
    ap = argparse.ArgumentParser(description="Build UAP↔Document translation/worklist for module context.")
    ap.add_argument("--uap", required=True, help="Path to UAP attempts CSV (v2)")
    ap.add_argument("--doc", required=True, help="Path to SR04 Document Tracking Report v2 (xlsx)")
    ap.add_argument("--out-dir", default="Outputs/Transi", help="Directory to write outputs")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    uap_path = Path(args.uap)
    doc_path = Path(args.doc)

    uap = load_uap(uap_path)
    doc = load_doc(doc_path)

    # distinct UAP modules to evaluate
    mods = (
        uap[["UAP_Name", "UAP_Name_norm", "UAP_File_norm", "UAP_Parent_norm"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    rows = []
    for _, r in mods.iterrows():
        mtype, score, docrow = best_match_for(r, doc)
        out = {
            "UAP_Module": r["UAP_Name"],
            "UAP_File_stem": r["UAP_File_norm"],
            "UAP_Parent": r["UAP_Parent_norm"],
            "match_type": mtype,
            "match_score": round(float(score), 3),
        }
        if docrow is not None:
            out.update({
                "DOC_Name": docrow.get("Name", ""),
                "DOC_File_Name": docrow.get("File Name", ""),
                "DOC_Parent_Name": docrow.get("Parent.Name", ""),
                "Parent.Context": docrow.get("Parent.Context", ""),
            })
        rows.append(out)

    cand = pd.DataFrame(rows)

    # Build worklist template (user-editable)
    worklist = cand.copy()
    worklist.insert(0, "Decision", "")  # M=match, P=possible, N=none
    worklist["Final_Module"] = ""
    worklist["Final_Context"] = ""
    worklist["Notes"] = ""

    # Unmatched summaries
    unmatched = cand[cand["match_type"].eq("NONE")][["UAP_Module", "UAP_File_stem", "UAP_Parent"]].copy()

    # Doc names never chosen (rough indication)
    matched_doc_names = set(cand["DOC_Name"].dropna().astype(str).unique().tolist())
    doc_unused = doc[~doc["Name"].astype(str).isin(matched_doc_names)][["Name", "File Name", "Parent.Name", "Parent.Context"]]

    # Write outputs
    out_candidates = out_dir / f"module_translation_candidates_{ts}.csv"
    out_unmatched = out_dir / f"uap_modules_unmatched_{ts}.csv"
    out_doc_unused = out_dir / f"doc_modules_unused_{ts}.csv"
    out_worklist = out_dir / f"FindModuleContext_Worklist_{ts}.csv"

    cand.to_csv(out_candidates, index=False, encoding="utf-8")
    unmatched.to_csv(out_unmatched, index=False, encoding="utf-8")
    doc_unused.to_csv(out_doc_unused, index=False, encoding="utf-8")
    worklist.to_csv(out_worklist, index=False, encoding="utf-8")

    # Summary
    total = len(mods)
    matched = (cand["match_type"] != "NONE").sum()
    print(f"[SUMMARY] Distinct UAP modules: {total} | matched: {matched} | unmatched: {total - matched}")
    print(f"[INFO] Candidates: {out_candidates}")
    print(f"[INFO] Unmatched:  {out_unmatched}")
    print(f"[INFO] Doc unused: {out_doc_unused}")
    print(f"[INFO] Worklist:   {out_worklist}")

if __name__ == "__main__":
    main()
