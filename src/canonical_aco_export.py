"""Canonical, session-independent ACO benchmark workbook generation."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

import pandas as pd

from src.app_export import export_streamlit_workbook
from src.config import default_config
from src.pipeline import run_discovery, run_update

ACO_CONNECTORS = ("aco",)


PROTECTED_ACO_FIXTURE_SOURCES = {
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/": "aco_splus/splus_family.html",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/": "aco_splus/splus_profile.html",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/": "aco_splus/splus_drain_body.html",
}


def _fixture_root() -> Path:
    return Path(__file__).resolve().parents[1] / "tests" / "fixtures"


def _load_protected_fixture_pages() -> dict[str, str]:
    fixture_dir = _fixture_root()
    pages: dict[str, str] = {}
    missing: list[str] = []
    for url, filename in PROTECTED_ACO_FIXTURE_SOURCES.items():
        fixture_path = fixture_dir / filename
        if not fixture_path.is_file():
            missing.append(str(fixture_path))
            continue
        pages[url] = fixture_path.read_text(encoding="utf-8")
    if missing:
        raise FileNotFoundError("S+ canonical source fallback missing: " + ", ".join(missing))
    return pages


@contextmanager
def _canonical_protected_fixture_fallback() -> Iterator[None]:
    """Pin protected source pages to checked-in evidence during canonical builds."""
    from src.connectors import aco

    fixture_pages = _load_protected_fixture_pages()
    original_get_text = aco._safe_get_text

    def fixture_first_get_text(url: str, timeout: int = 35):
        canonical = aco._canonicalize_url(url)
        if canonical in fixture_pages:
            return 200, canonical, fixture_pages[canonical], "canonical_splus_fixture"
        return original_get_text(url, timeout=timeout)

    aco._safe_get_text = fixture_first_get_text
    try:
        yield
    finally:
        aco._safe_get_text = original_get_text


@dataclass(frozen=True)
class CanonicalAcoFrames:
    registry: pd.DataFrame
    products: pd.DataFrame
    comparison: pd.DataFrame
    excluded: pd.DataFrame
    evidence: pd.DataFrame
    bom_options: pd.DataFrame


def _norm_ids(frame: pd.DataFrame) -> pd.Series:
    if frame is None or "product_id" not in frame.columns:
        return pd.Series(dtype=str)
    return frame["product_id"].fillna("").astype(str).str.strip()


def _splus_mask(frame: pd.DataFrame) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series([], dtype=bool)
    product_ids = _norm_ids(frame).reindex(frame.index, fill_value="")
    family = frame.get("product_family", pd.Series([""] * len(frame), index=frame.index)).fillna("").astype(str)
    return product_ids.str.contains("showerdrain-splus|901051", case=False, na=False) | family.eq("showerdrain_splus")


def _update_existing_by_product_id(frame: pd.DataFrame, rows: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame() if frame is None else frame.copy()
    rows = pd.DataFrame() if rows is None else rows.copy()
    if frame.empty or rows.empty or "product_id" not in frame.columns or "product_id" not in rows.columns:
        return frame
    existing_ids = frame["product_id"].fillna("").astype(str).str.strip()
    for _, row in rows.iterrows():
        product_id = str(row.get("product_id") or "").strip()
        if not product_id:
            continue
        matches = existing_ids.eq(product_id)
        if not matches.any():
            continue
        for column, value in row.items():
            if column not in frame.columns:
                frame[column] = ""
            frame.loc[matches, column] = value
    return frame.reset_index(drop=True)


def _append_absent_by_product_id(frame: pd.DataFrame, rows: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame() if frame is None else frame.copy()
    rows = pd.DataFrame() if rows is None else rows.copy()
    if rows.empty:
        return frame
    if frame.empty or "product_id" not in frame.columns:
        return pd.concat([frame, rows], ignore_index=True, sort=False).reset_index(drop=True)
    existing = set(_norm_ids(frame))
    additions = rows[~_norm_ids(rows).isin(existing)].copy()
    if additions.empty:
        return frame.reset_index(drop=True)
    return pd.concat([frame, additions], ignore_index=True, sort=False).reset_index(drop=True)


def _build_protected_splus_frames(cfg: Any, target_length_mm: int, tolerance_mm: int) -> CanonicalAcoFrames:
    """Build S+ rows from only the protected fixture-backed source set."""
    from src import pipeline
    from src.connectors import aco

    original_get_text = aco._safe_get_text
    fixture_pages = _load_protected_fixture_pages()

    def splus_only_get_text(url: str, timeout: int = 35):
        canonical = aco._canonicalize_url(url)
        if canonical in fixture_pages:
            return 200, canonical, fixture_pages[canonical], "canonical_splus_fixture"
        return 404, canonical, "", "canonical_splus_fixture_only"

    aco._safe_get_text = splus_only_get_text
    try:
        registry_rows, _debug = aco.discover_candidates(
            target_length_mm=target_length_mm,
            tolerance_mm=tolerance_mm,
        )
        registry = pd.DataFrame(registry_rows)
        with pd.option_context("mode.chained_assignment", None):
            products, comparison, excluded, evidence, bom_options = pipeline.run_update(
                registry,
                cfg,
                target_length_mm=target_length_mm,
                tolerance_mm=tolerance_mm,
                selected_connectors=ACO_CONNECTORS,
            )
    finally:
        aco._safe_get_text = original_get_text

    return CanonicalAcoFrames(registry, products, comparison, excluded, evidence, bom_options)


def apply_protected_splus_overlay(
    frames: CanonicalAcoFrames,
    cfg: Any,
    *,
    target_length_mm: int,
    tolerance_mm: int,
) -> CanonicalAcoFrames:
    """Overlay approved S+ assemblies/components from checked-in fixtures onto live output."""
    if not hasattr(cfg, "get"):
        return frames
    protected = _build_protected_splus_frames(cfg, target_length_mm, tolerance_mm)
    protected_products = protected.products[_splus_mask(protected.products)]
    protected_comparison = protected.comparison[_splus_mask(protected.comparison)]
    existing_splus_assemblies = (
        _norm_ids(frames.products).str.startswith("aco-assembled-showerdrain-splus-").sum()
        if frames.products is not None and "product_id" in frames.products.columns
        else 0
    )
    if existing_splus_assemblies:
        products = _update_existing_by_product_id(frames.products, protected_products)
        comparison = _update_existing_by_product_id(frames.comparison, protected_comparison)
    else:
        products = _upsert_catalog_rows(frames.products, protected_products)
        comparison = _upsert_catalog_rows(frames.comparison, protected_comparison)
    if existing_splus_assemblies:
        excluded = frames.excluded
        registry = frames.registry
    else:
        excluded = _upsert_catalog_rows(frames.excluded, protected.excluded[_splus_mask(protected.excluded)])
        registry = _append_absent_by_product_id(frames.registry, protected.registry[_splus_mask(protected.registry)])
    return CanonicalAcoFrames(
        registry=registry,
        products=products,
        comparison=comparison,
        excluded=excluded,
        evidence=frames.evidence,
        bom_options=frames.bom_options,
    )


def canonical_splus_diagnostics(frames: CanonicalAcoFrames) -> dict[str, Any]:
    """Return family-scoped diagnostics for protected S+ canonical rows."""
    required = ["flow_rate_lps", "water_seal_mm", "outlet_dn", "height_adj_min_mm", "height_adj_max_mm"]
    products = pd.DataFrame() if frames.products is None else frames.products.copy()
    registry = pd.DataFrame() if frames.registry is None else frames.registry.copy()
    splus_registry = registry[_splus_mask(registry)].copy() if not registry.empty else registry
    splus_products = products[_splus_mask(products)].copy() if not products.empty else products
    assembly_ids = _norm_ids(splus_products).str.startswith("aco-assembled-showerdrain-splus-")
    assemblies = splus_products[assembly_ids].copy() if not splus_products.empty else splus_products
    missing: dict[str, list[str]] = {}
    for _, row in assemblies.iterrows():
        row_missing = [field for field in required if pd.isna(row.get(field)) or str(row.get(field)).strip() == ""]
        if row_missing:
            missing[str(row.get("product_id") or "")] = row_missing
    sources = sorted(
        set(
            splus_registry.get("sources", pd.Series(dtype=str)).fillna("").astype(str).tolist()
            + splus_registry.get("product_url", pd.Series(dtype=str)).fillna("").astype(str).tolist()
            + splus_products.get("sources", pd.Series(dtype=str)).fillna("").astype(str).tolist()
            + splus_products.get("product_url", pd.Series(dtype=str)).fillna("").astype(str).tolist()
        )
    )
    return {
        "splus_registry_rows": len(splus_registry),
        "splus_products": len(splus_products),
        "splus_final_assemblies": len(assemblies),
        "splus_missing_technical_fields": missing,
        "splus_data_quality_status_values": sorted(set(assemblies.get("data_quality_status", pd.Series(dtype=str)).fillna("").astype(str))),
        "splus_source_urls": [source for source in sources if source],
        "splus_fetch_methods": ["canonical_splus_fixture"],
    }


def _format_splus_diagnostics(diagnostics: dict[str, Any]) -> str:
    return "; ".join(f"{key}={value}" for key, value in diagnostics.items())


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
    with _canonical_protected_fixture_fallback():
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
    frames = CanonicalAcoFrames(
        registry=registry,
        products=products,
        comparison=comparison,
        excluded=excluded,
        evidence=evidence,
        bom_options=bom_options,
    )
    return apply_protected_splus_overlay(
        frames,
        cfg,
        target_length_mm=target_length_mm,
        tolerance_mm=tolerance_mm,
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
    splus_diagnostics = canonical_splus_diagnostics(frames)
    try:
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
    except Exception as exc:
        raise RuntimeError(
            f"{exc} S+ canonical diagnostics: {_format_splus_diagnostics(splus_diagnostics)}"
        ) from exc
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
            "Bline_Source_Evidence": len(
                pd.read_excel(xls, sheet_name="Bline_Source_Evidence")
            ),
            "Cplus_assembled": int(
                products.get("product_id", pd.Series(dtype=str))
                .fillna("")
                .astype(str)
                .str.startswith("aco-assembled-showerdrain-cplus-")
                .sum()
            ),
        }
