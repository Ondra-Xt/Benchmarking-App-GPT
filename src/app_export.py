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
    """Raised when export input cannot produce the canonical workbook."""

    def __init__(
        self,
        results: list[CheckResult],
        *,
        input_description: str = "current Streamlit session",
    ):
        self.results = results
        failures = [f"{result.name}: {result.detail}" for result in results if not result.passed]
        detail = "; ".join(failures[:12]) or "unknown workbook validation failure"
        super().__init__(
            f"XLSX export blocked because the {input_description} is not the canonical "
            f"full benchmark state. {detail}. No XLSX was created."
        )



def _failure(name: str, detail: str) -> CheckResult:
    return CheckResult(name, False, detail)


def _success(name: str, detail: str = "ok") -> CheckResult:
    return CheckResult(name, True, detail)


def _sheet_product_ids(frame: pd.DataFrame) -> pd.Series:
    if "product_id" not in frame.columns:
        return pd.Series([""] * len(frame), index=frame.index, dtype=str)
    return frame["product_id"].fillna("").astype(str).str.strip()


def validate_session_snapshot_xlsx(path: str | Path) -> tuple[bool, list[CheckResult]]:
    """Validate a current-session workbook structurally, without canonical row counts."""
    results: list[CheckResult] = []
    try:
        xls = pd.ExcelFile(path)
    except Exception as exc:
        return False, [_failure("session_snapshot_open_workbook", f"{type(exc).__name__}: {exc}")]

    required = ["Products", "Comparison"]
    missing = [name for name in required if name not in xls.sheet_names]
    results.append(CheckResult("session_snapshot_required_sheets", not missing, f"missing={missing}"))
    if missing:
        xls.close()
        return False, results

    optional = ["Candidates_All", "BOM_Options", "Excluded", "Evidence"]
    loaded: dict[str, pd.DataFrame] = {}
    try:
        for name in required + [sheet for sheet in optional if sheet in xls.sheet_names]:
            loaded[name] = pd.read_excel(xls, sheet_name=name)
    except Exception as exc:
        xls.close()
        return False, results + [_failure("session_snapshot_read_sheets", f"{type(exc).__name__}: {exc}")]
    finally:
        xls.close()

    products = loaded["Products"]
    comparison = loaded["Comparison"]
    candidates = loaded.get("Candidates_All", pd.DataFrame())
    bom = loaded.get("BOM_Options", pd.DataFrame())

    empty_session = products.empty and comparison.empty and candidates.empty
    results.append(CheckResult("session_snapshot_not_empty", not empty_session, "Products/Comparison/Candidates_All are all empty" if empty_session else "ok"))

    product_ids = _sheet_product_ids(products)
    has_product_id_col = "product_id" in products.columns
    results.append(CheckResult("session_snapshot_products_product_id_column", has_product_id_col, "product_id column present" if has_product_id_col else "missing product_id column"))
    if has_product_id_col:
        empty_ids = int(product_ids.eq("").sum())
        results.append(CheckResult("session_snapshot_products_product_id_nonempty", empty_ids == 0, f"empty={empty_ids}"))
        duplicate_ids = sorted(product_ids[product_ids.ne("") & product_ids.duplicated()].unique().tolist())
        results.append(CheckResult("session_snapshot_products_unique_product_ids", not duplicate_ids, f"duplicates={duplicate_ids[:10]}"))

    if "product_id" not in comparison.columns:
        results.append(_failure("session_snapshot_comparison_product_id_column", "missing product_id column"))
    else:
        comparison_ids = _sheet_product_ids(comparison)
        empty_comparison_ids = int(comparison_ids.eq("").sum())
        results.append(CheckResult("session_snapshot_comparison_product_id_nonempty", empty_comparison_ids == 0, f"empty={empty_comparison_ids}"))

    if not bom.empty:
        parent_col = "product_id" if "product_id" in bom.columns else "base_product_id" if "base_product_id" in bom.columns else ""
        child_col = next((col for col in ("option_product_id", "component_id", "grate_id") if col in bom.columns), "")
        if parent_col and child_col:
            parents = bom[parent_col].fillna("").astype(str).str.strip()
            children = bom[child_col].fillna("").astype(str).str.strip()
            self_refs = bom[parents.ne("") & parents.eq(children)]
            results.append(CheckResult("session_snapshot_bom_no_self_reference", self_refs.empty, f"rows={self_refs.index.tolist()[:10]}"))

            product_roles = pd.Series(dtype=str)
            if "system_role" in products.columns:
                product_roles = products.set_index(product_ids)["system_role"].fillna("").astype(str).str.lower()
            product_families = pd.Series(dtype=str)
            if "product_family" in products.columns:
                product_families = products.set_index(product_ids)["product_family"].fillna("").astype(str).str.lower()
            is_grate = set(product_roles[product_roles.str.contains("grate", na=False)].index) | set(product_families[product_families.str.contains("grate", na=False)].index)
            if is_grate:
                grate_links = bom[parents.isin(is_grate) & children.isin(is_grate)]
                results.append(CheckResult("session_snapshot_bom_no_grate_to_grate_links", grate_links.empty, f"rows={grate_links.index.tolist()[:10]}"))
        else:
            results.append(_success("session_snapshot_bom_link_columns", "no BOM link columns to validate"))

    return all(result.passed for result in results), results


def export_session_snapshot_workbook(
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
    """Atomically export current Streamlit frames with structural validation only."""
    destination = Path(out_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp.xlsx")
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
        passed, results = validate_session_snapshot_xlsx(temporary)
        if not passed:
            raise AppExportValidationError(results, input_description="current Streamlit session snapshot")
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

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
    input_description: str = "current Streamlit session",
) -> None:
    """Atomically export only a canonical, baseline-valid Streamlit workbook.

    The app session can reflect a connector-filtered or stale partial run.  Export to a
    temporary sibling file first, validate fixed canonical sheet/family counts, and only
    then atomically publish the path used by the download button.
    """
    destination = Path(out_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp.xlsx")
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
            raise AppExportValidationError(results, input_description=input_description)
        full_passed, full_results = validate_xlsx(str(temporary))
        if not full_passed:
            raise AppExportValidationError(full_results, input_description=input_description)
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
