# src/excel_export.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
import json

import pandas as pd
import openpyxl


def _is_nan(x: Any) -> bool:
    try:
        return x != x  # NaN != NaN
    except Exception:
        return False


def _to_excel_cell(v: Any) -> Any:
    """
    Převede hodnotu z DataFrame na hodnotu, kterou openpyxl umí uložit do buňky.
    - None/NaN -> None
    - list/tuple/set/dict -> JSON string
    - Path -> str
    - numpy scalar -> item()
    """
    if v is None or _is_nan(v):
        return None

    if hasattr(v, "item") and callable(getattr(v, "item")):
        try:
            v = v.item()
        except Exception:
            pass

    if isinstance(v, Path):
        return str(v)

    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", errors="ignore")
        except Exception:
            return str(v)

    if isinstance(v, (list, tuple, set, dict)):
        try:
            if isinstance(v, set):
                v = sorted(list(v))
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)

    return v


def export_excel(
    template_path: str,
    out_path: str,
    cfg: Any,
    registry_df: Optional[pd.DataFrame] = None,
    products_df: Optional[pd.DataFrame] = None,
    comparison_df: Optional[pd.DataFrame] = None,
    excluded_df: Optional[pd.DataFrame] = None,
    evidence_df: Optional[pd.DataFrame] = None,
    bom_options_df: Optional[pd.DataFrame] = None,
    components_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Exportuje výsledky do XLSX.
    - Products: jen benchmarkované produkty (bez base_set)
    - Components: base_set produkty
    """
    wb = openpyxl.load_workbook(template_path)

    registry_df = pd.DataFrame() if registry_df is None else registry_df.copy()
    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    comparison_df = pd.DataFrame() if comparison_df is None else comparison_df.copy()
    excluded_df = pd.DataFrame() if excluded_df is None else excluded_df.copy()
    evidence_df = pd.DataFrame() if evidence_df is None else evidence_df.copy()
    bom_options_df = pd.DataFrame() if bom_options_df is None else bom_options_df.copy()
    components_df = pd.DataFrame() if components_df is None else components_df.copy()

    # --- AUTO split base_set -> Components ---
    if not products_df.empty and components_df.empty:
        df = products_df.copy()

        cand = df["candidate_type"].astype(str).str.lower() if "candidate_type" in df.columns else pd.Series([""] * len(df))
        comp = df["complete_system"].astype(str).str.lower() if "complete_system" in df.columns else pd.Series([""] * len(df))

        is_component = cand.isin(["base_set", "component"]) | comp.str.contains("component/base-set", na=False) | comp.str.contains("component", na=False)

        components_df = df[is_component].copy()
        products_df = df[~is_component].copy()

    def write_df(sheet_name: str, df: pd.DataFrame):
        # přepiš sheet
        if sheet_name in wb.sheetnames:
            ws_old = wb[sheet_name]
            wb.remove(ws_old)
        ws = wb.create_sheet(sheet_name)

        # hlavička
        ws.append([str(c) for c in df.columns])

        # řádky
        for _, row in df.iterrows():
            ws.append([_to_excel_cell(v) for v in row.tolist()])

    write_df("Candidates_All", registry_df)
    write_df("Products", products_df)
    write_df("Components", components_df)
    write_df("Comparison", comparison_df)
    write_df("Excluded", excluded_df)
    write_df("Evidence", evidence_df)
    write_df("BOM_Options", bom_options_df)

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)
