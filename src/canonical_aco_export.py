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
