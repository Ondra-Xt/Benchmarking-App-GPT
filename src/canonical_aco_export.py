"""Canonical, session-independent ACO benchmark workbook generation."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.app_export import export_streamlit_workbook
from src.config import default_config
from src.pipeline import run_discovery, run_update

ACO_CONNECTORS = ("aco",)


@dataclass(frozen=True)
class CanonicalAcoFrames:
    registry: pd.DataFrame
    products: pd.DataFrame
    comparison: pd.DataFrame
    excluded: pd.DataFrame
    evidence: pd.DataFrame
    bom_options: pd.DataFrame


def _upsert_catalog_rows(
    frame: pd.DataFrame,
    catalog_rows: pd.DataFrame,
) -> pd.DataFrame:
    """Overlay catalog-backed fields by stable product ID, appending absent rows."""
    frame = pd.DataFrame() if frame is None else frame.copy()
    catalog_rows = pd.DataFrame() if catalog_rows is None else catalog_rows.copy()
    if catalog_rows.empty:
        return frame
    if frame.empty:
        return catalog_rows.reset_index(drop=True)
    if "product_id" not in frame.columns:
        return pd.concat([frame, catalog_rows], ignore_index=True, sort=False)

    existing_ids = frame["product_id"].fillna("").astype(str).str.strip()
    for _, catalog_row in catalog_rows.iterrows():
        product_id = str(catalog_row.get("product_id") or "").strip()
        if not product_id:
            continue
        matches = existing_ids.eq(product_id)
        if matches.any():
            for column, value in catalog_row.items():
                if column not in frame.columns:
                    frame[column] = ""
                frame.loc[matches, column] = value
        else:
            frame = pd.concat([frame, pd.DataFrame([catalog_row])], ignore_index=True, sort=False)
            existing_ids = frame["product_id"].fillna("").astype(str).str.strip()
    return frame.reset_index(drop=True)


def apply_cplus_catalog_matrix_inputs(
    products: pd.DataFrame,
    excluded: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Materialize the protected C+ bases and validated 15-grate candidate set.

    Live discovery can retain the grate articles while classifying most rows without
    the family/role metadata required by the explicit C+ matrix.  The canonical path
    overlays the checked-in catalog-backed metadata before workbook generation.
    """
    from tools.report_cplus_compatible_grate_evidence import (
        catalog_backed_cplus_products_and_components,
    )

    catalog_products, catalog_components = catalog_backed_cplus_products_and_components()
    products = _upsert_catalog_rows(products, catalog_products)

    # Component candidates normally live in Excluded. Overlay rows already present in
    # Products as well, then add only genuinely absent catalog articles to Excluded.
    products = _upsert_catalog_rows(
        products,
        catalog_components[
            catalog_components["product_id"].isin(
                products.get("product_id", pd.Series(dtype=str)).fillna("").astype(str)
            )
        ],
    )
    product_ids = set(
        products.get("product_id", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    )
    excluded_catalog = catalog_components[~catalog_components["product_id"].isin(product_ids)]
    excluded = _upsert_catalog_rows(excluded, excluded_catalog)
    return products, excluded


def build_canonical_aco_frames(
    cfg: Any | None = None,
    *,
    target_length_mm: int = 1200,
    tolerance_mm: int = 100,
) -> CanonicalAcoFrames:
    """Run only the ACO connector and return the canonical exporter inputs."""
    cfg = default_config() if cfg is None else cfg
    registry, _debug = run_discovery(
        target_length_mm=target_length_mm,
        tolerance_mm=tolerance_mm,
        selected_connectors=ACO_CONNECTORS,
    )
    products, comparison, excluded, evidence, bom_options = run_update(
        registry,
        cfg,
        target_length_mm=target_length_mm,
        tolerance_mm=tolerance_mm,
        selected_connectors=ACO_CONNECTORS,
    )
    products, excluded = apply_cplus_catalog_matrix_inputs(products, excluded)
    return CanonicalAcoFrames(
        registry=registry,
        products=products,
        comparison=comparison,
        excluded=excluded,
        evidence=evidence,
        bom_options=bom_options,
    )


def export_canonical_aco_workbook(
    template_path: str | Path,
    out_path: str | Path,
    cfg: Any | None = None,
    *,
    target_length_mm: int = 1200,
    tolerance_mm: int = 100,
    frame_builder: Callable[..., CanonicalAcoFrames] | None = None,
) -> Path:
    """Build and atomically publish a validator-approved ACO workbook."""
    cfg = default_config() if cfg is None else cfg
    frame_builder = build_canonical_aco_frames if frame_builder is None else frame_builder
    frames = frame_builder(
        cfg,
        target_length_mm=target_length_mm,
        tolerance_mm=tolerance_mm,
    )
    export_streamlit_workbook(
        template_path,
        out_path,
        cfg,
        registry=frames.registry,
        products=frames.products,
        comparison=frames.comparison,
        excluded=frames.excluded,
        evidence=frames.evidence,
        bom_options=frames.bom_options,
        input_description="canonical ACO pipeline output",
    )
    return Path(out_path)


def workbook_summary(path: str | Path) -> dict[str, int]:
    """Return the concise counts printed by the canonical export CLI."""
    with pd.ExcelFile(path, engine="openpyxl") as xls:
        products = pd.read_excel(xls, sheet_name="Products")
        return {
            "Products": len(products),
            "BOM_Options": len(pd.read_excel(xls, sheet_name="BOM_Options")),
            "Final_Assemblies": len(pd.read_excel(xls, sheet_name="Final_Assemblies")),
            "Eplus_Compatible_Grate_Evidence": len(
                pd.read_excel(xls, sheet_name="Eplus_Compatible_Grate_Evidence")
            ),
            "Cplus_Compatible_Grate_Evidence": len(
                pd.read_excel(xls, sheet_name="Cplus_Compatible_Grate_Evidence")
            ),
            "Cplus_assembled": int(
                products.get("product_id", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.startswith("aco-assembled-showerdrain-cplus-")
                .sum()
            ),
        }
