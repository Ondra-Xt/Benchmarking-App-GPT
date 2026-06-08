"""Fail-closed XLSX export entrypoint used by the Streamlit application."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.excel_export import export_excel
from tools.validate_xlsx_export import (
    CheckResult,
    validate_app_export_baseline,
    validate_xlsx,
)


class AppExportValidationError(RuntimeError):
    """Raised when a Streamlit session cannot produce the canonical workbook."""

    def __init__(self, results: list[CheckResult]):
        self.results = results
        failures = [f"{result.name}: {result.detail}" for result in results if not result.passed]
        detail = "; ".join(failures[:12]) or "unknown workbook validation failure"
        super().__init__(
            "XLSX export blocked because the current Streamlit session is not the canonical "
            f"full benchmark state. {detail}. Run discovery/update with all connectors, then retry."
        )


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
    """Atomically export only a canonical, baseline-valid Streamlit workbook.

    The app session can reflect a connector-filtered or stale partial run.  Export to a
    temporary sibling file first, validate fixed canonical sheet/family counts, and only
    then atomically publish the path used by the download button.
    """
    destination = Path(out_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp.xlsx")
    destination.unlink(missing_ok=True)
    try:
        export_excel(
            str(template_path),
            str(temporary),
            cfg,
            registry_df=registry,
            products_df=products,
            comparison_df=comparison,
            excluded_df=excluded,
            evidence_df=evidence,
            bom_options_df=bom_options,
            components_df=None,
        )
        passed, results = validate_app_export_baseline(str(temporary))
        if not passed:
            raise AppExportValidationError(results)
        full_passed, full_results = validate_xlsx(str(temporary))
        if not full_passed:
            raise AppExportValidationError(full_results)
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        destination.unlink(missing_ok=True)
        raise
