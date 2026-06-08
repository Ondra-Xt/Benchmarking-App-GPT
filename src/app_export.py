"""Shared XLSX export entrypoint used by the Streamlit application."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.excel_export import export_excel


def export_streamlit_workbook(
    template_path: str | Path,
    out_path: str | Path,
    cfg: Any,
    *,
    registry: pd.DataFrame,
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    excluded: pd.DataFrame,
    evidence: pd.DataFrame,
    bom_options: pd.DataFrame,
) -> None:
    """Export the frames stored by Streamlit after ``pipeline.run_update``.

    ``run_update`` returns component-only rows in ``excluded`` rather than as a
    separate components dataframe.  Keeping this wrapper shared by the app and
    regression tests protects that real session-state contract.
    """
    export_excel(
        str(template_path),
        str(out_path),
        cfg,
        registry_df=registry,
        products_df=products,
        comparison_df=comparison,
        excluded_df=excluded,
        evidence_df=evidence,
        bom_options_df=bom_options,
        components_df=None,
    )
