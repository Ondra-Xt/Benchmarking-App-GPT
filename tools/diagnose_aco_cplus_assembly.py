import sys
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _scoring_field_coverage

PROTECTED_CPLUS_BASE_IDS = (
    "aco-showerdrain-cplus-standard-h92",
    "aco-showerdrain-cplus-low-h69",
)
REQUIRED_SCORING_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)


@dataclass
class CPlusDiagnostic:
    proposed_ids: list[str]
    base_count: int
    valid_grate_count: int
    duplicate_ids: list[str]
    missing_grate_components: list[str]
    dangling_component_ids: int
    self_reference_rows: int
    grate_to_grate_links: int
    missing_base_scoring_fields: dict[str, list[str]]
    products_count: int
    comparison_count: int
    coverage_count: int
    bom_count: int


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _base_slug(product_id: str) -> str:
    prefix = "aco-showerdrain-cplus-"
    if product_id.startswith(prefix):
        return product_id[len(prefix):]
    return product_id


def compute_cplus_diagnostic(products: pd.DataFrame, comparison: pd.DataFrame, excluded: pd.DataFrame, bom: pd.DataFrame, coverage: pd.DataFrame) -> CPlusDiagnostic:
    product_ids = _norm(products, "product_id")
    base_mask = product_ids.isin(PROTECTED_CPLUS_BASE_IDS)
    bases = products[base_mask].copy()

    bom_option_type = _norm(bom, "option_type").str.lower()
    bom_parent_family = _norm(bom, "parent_family").str.lower()
    bom_option_family = _norm(bom, "option_family").str.lower()
    bom_component_ids = _norm(bom, "component_id")
    bom_product_ids = _norm(bom, "product_id")
    bom_grate = bom[(bom_option_type == "compatible_grate") & ((bom_parent_family == "showerdrain_cplus") | (bom_option_family == "showerdrain_cplus"))].copy()

    components_universe = set(_norm(excluded, "product_id").tolist())
    valid_grate_ids = sorted({cid for cid in bom_component_ids[bom_grate.index].tolist() if cid and cid in components_universe})
    missing_grate_components = sorted({cid for cid in bom_component_ids[bom_grate.index].tolist() if cid and cid not in components_universe})

    proposed_ids: list[str] = []
    for base_id in sorted(_norm(bases, "product_id").tolist()):
        base_slug = _base_slug(base_id)
        for grate_id in valid_grate_ids:
            grate_slug = grate_id.replace("aco-", "", 1) if grate_id.startswith("aco-") else grate_id
            proposed_ids.append(f"aco-assembled-showerdrain-cplus-{base_slug}__{grate_slug}")

    all_existing_ids = set(product_ids.tolist()) | set(_norm(comparison, "product_id").tolist())
    duplicate_ids = sorted([pid for pid in proposed_ids if pid in all_existing_ids])

    dangling_component_ids = int(((bom_option_type == "compatible_grate") & (bom_component_ids != "") & ~bom_component_ids.isin(components_universe)).sum())
    self_reference_rows = int(((bom_product_ids != "") & (bom_product_ids == bom_component_ids)).sum())

    grate_ids_global = {
        pid
        for pid in components_universe
        if "grate" in pid.lower()
    }
    grate_to_grate_links = int(((bom_option_type == "compatible_grate") & bom_product_ids.isin(grate_ids_global) & bom_component_ids.isin(grate_ids_global)).sum())

    coverage_subset = coverage[_norm(coverage, "product_id").isin(PROTECTED_CPLUS_BASE_IDS)].copy()
    missing_base_scoring_fields: dict[str, list[str]] = {}
    for _, row in coverage_subset.iterrows():
        pid = str(row.get("product_id") or "")
        missing = []
        for field in REQUIRED_SCORING_FIELDS:
            value = row.get(field, "")
            if pd.isna(value) or str(value).strip() == "":
                missing.append(field)
        if missing:
            missing_base_scoring_fields[pid] = missing

    return CPlusDiagnostic(
        proposed_ids=sorted(proposed_ids),
        base_count=len(bases),
        valid_grate_count=len(valid_grate_ids),
        duplicate_ids=duplicate_ids,
        missing_grate_components=missing_grate_components,
        dangling_component_ids=dangling_component_ids,
        self_reference_rows=self_reference_rows,
        grate_to_grate_links=grate_to_grate_links,
        missing_base_scoring_fields=missing_base_scoring_fields,
        products_count=len(products),
        comparison_count=len(comparison),
        coverage_count=len(coverage),
        bom_count=len(bom),
    )


def _print_report(diag: CPlusDiagnostic) -> None:
    n = len(diag.proposed_ids)
    print("ACO ShowerDrain C+ assembly diagnostic (proposal only; no C+ assembly executed)")
    print(f"C+ base rows found: {diag.base_count}")
    print(f"Valid C+ grate components found: {diag.valid_grate_count}")
    print(f"Proposed C+ assembled product count N: {n}")
    print("Proposed assembled product IDs:")
    for pid in diag.proposed_ids:
        print(f"- {pid}")

    print("\nExpected future count changes if C+ assembly were enabled:")
    print(f"- Products: {diag.products_count} + {n}")
    print(f"- Comparison: {diag.comparison_count} + {n}")
    print(f"- Scoring_Field_Coverage: {diag.coverage_count} + {n}")
    print(f"- C+ assembled: 0 -> {n}")
    print(f"- BOM_Options: {diag.bom_count} (should remain unchanged unless implementation adds explicit rows)")

    print("\nRisk checks:")
    print(f"- duplicate assembled IDs: {len(diag.duplicate_ids)}")
    if diag.duplicate_ids:
        for pid in diag.duplicate_ids:
            print(f"  - {pid}")
    print(f"- missing grate component rows: {len(diag.missing_grate_components)}")
    if diag.missing_grate_components:
        for cid in diag.missing_grate_components:
            print(f"  - {cid}")
    print(f"- dangling component_id: {diag.dangling_component_ids}")
    print(f"- self-reference: {diag.self_reference_rows}")
    print(f"- grate-to-grate links: {diag.grate_to_grate_links}")
    print(f"- missing base fields needed for scoring: {len(diag.missing_base_scoring_fields)}")
    for pid, fields in diag.missing_base_scoring_fields.items():
        print(f"  - {pid}: {', '.join(fields)}")


def main() -> int:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, _evidence, bom = pipeline.run_update(registry, default_config())
    coverage = _scoring_field_coverage(products, comparison)

    diag = compute_cplus_diagnostic(products, comparison, excluded, bom, coverage)
    _print_report(diag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
