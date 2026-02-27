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

    # --- AUTO split base_set -> Components ---
    if products_df is not None and not products_df.empty and components_df is None:
        df = products_df.copy()

        cand = df["candidate_type"].astype(str).str.lower() if "candidate_type" in df.columns else pd.Series([""] * len(df))
        comp = df["complete_system"].astype(str).str.lower() if "complete_system" in df.columns else pd.Series([""] * len(df))

        is_component = cand.isin(["base_set", "component"]) | comp.str.contains("component/base-set", na=False) | comp.str.contains("component", na=False)

        components_df = df[is_component].copy()
        products_df = df[~is_component].copy()

    def write_df(sheet_name: str, df: Optional[pd.DataFrame]):
        if df is None:
            return

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

    write_df("Registry", registry_df)
    write_df("Products", products_df)
    write_df("Components", components_df)
    write_df("Comparison", comparison_df)
    write_df("Excluded", excluded_df)
    write_df("Evidence", evidence_df)
    write_df("BOM_Options", bom_options_df)

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)