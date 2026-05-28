import sys
from dataclasses import dataclass

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _scoring_field_coverage

FAMILIES = [
    "showerdrain_splus",
    "showerdrain_c",
    "showerdrain_cplus",
    "showerdrain_mplus",
    "showerdrain_eplus",
    "showerdrain_b",
    "easyflow",
    "easyflowplus",
]

HYDRAULIC_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)

ACTIVE_ASSEMBLED_PREFIX = {
    "showerdrain_splus": "aco-assembled-showerdrain-splus-",
    "showerdrain_c": "aco-assembled-showerdrain-c-",
}


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _has_hydraulic_values(row: pd.Series) -> bool:
    for f in HYDRAULIC_FIELDS:
        v = row.get(f, "")
        if pd.isna(v) or str(v).strip() == "":
            return False
    return True


def _infer_family_from_id(pid: str) -> str:
    s = pid.lower()
    for fam in FAMILIES:
        needle = fam.replace("_", "-")
        if needle in s or fam in s:
            return fam
    return ""


@dataclass
class FamilyDiagnostic:
    family: str
    base_rows: int
    protected_rows: int
    valid_drain_rows: int
    compatible_grate_rows: int
    optional_accessory_rows: int
    valid_grate_components: int
    proposed_n: int
    proposed_examples: list[str]
    current_assembled: int
    status: str
    blocking_reason: str


@dataclass
class GlobalRisk:
    duplicate_ids: list[str]
    missing_component_rows: list[str]
    dangling_component_ids: int
    self_reference_rows: int
    grate_to_grate_links: int
    missing_base_scoring_fields: dict[str, list[str]]
    leaked_component_ids_in_products: list[str]
    leaked_component_ids_in_comparison: list[str]


def compute_readiness(products: pd.DataFrame, comparison: pd.DataFrame, excluded: pd.DataFrame, bom: pd.DataFrame) -> tuple[list[FamilyDiagnostic], GlobalRisk]:
    prod_ids = _norm(products, "product_id")
    comp_ids = _norm(comparison, "product_id")
    excl_ids = set(_norm(excluded, "product_id").tolist())

    bom_option_type = _norm(bom, "option_type").str.lower()
    bom_parent_family = _norm(bom, "parent_family").str.lower()
    bom_option_family = _norm(bom, "option_family").str.lower()
    bom_product_id = _norm(bom, "product_id")
    bom_component_id = _norm(bom, "component_id")

    family_reports: list[FamilyDiagnostic] = []
    for family in FAMILIES:
        fam_hyphen = family.replace("_", "-")
        base_mask = prod_ids.str.lower().str.contains(fam_hyphen) | prod_ids.str.lower().str.contains(family)
        base_rows = products[base_mask].copy()
        base_count = len(base_rows)

        if family in ACTIVE_ASSEMBLED_PREFIX:
            assembled_pref = ACTIVE_ASSEMBLED_PREFIX[family]
            protected_rows = int(prod_ids.str.startswith(assembled_pref).sum())
        else:
            protected_rows = 0

        valid_drain_rows = int(base_rows.apply(_has_hydraulic_values, axis=1).sum()) if base_count else 0

        family_bom_mask = (bom_parent_family == family) | (bom_option_family == family)
        fam_compat = bom[(bom_option_type == "compatible_grate") & family_bom_mask]
        fam_optional = bom[(bom_option_type == "optional_accessory") & family_bom_mask]
        compatible_rows = len(fam_compat)
        optional_rows = len(fam_optional)

        grate_candidates = [cid for cid in _norm(fam_compat, "component_id").tolist() if cid]
        valid_grates = sorted({cid for cid in grate_candidates if cid in excl_ids})

        proposed_ids = []
        for base_id in sorted(_norm(base_rows, "product_id").tolist()):
            for grate_id in valid_grates:
                base_slug = base_id.replace("aco-", "", 1)
                grate_slug = grate_id.replace("aco-", "", 1)
                proposed_ids.append(f"aco-assembled-{family.replace('_', '-')}-{base_slug}__{grate_slug}")

        if protected_rows > 0:
            status = "ALREADY_ACTIVE / BASELINE_PROTECTED"
            reason = "assembled output already present in baseline"
        elif proposed_ids:
            status = "READY"
            reason = ""
        else:
            status = "BLOCKED"
            if base_count == 0:
                reason = "no base product rows"
            elif compatible_rows == 0:
                reason = "no source-backed compatible_grate BOM rows"
            elif len(valid_grates) == 0:
                reason = "no compatible_grate component_id rows present in Components/excluded"
            elif valid_drain_rows == 0:
                reason = "no drain/body rows with complete hydraulic fields"
            else:
                reason = "no valid assembly combinations"

        family_reports.append(
            FamilyDiagnostic(
                family=family,
                base_rows=base_count,
                protected_rows=protected_rows,
                valid_drain_rows=valid_drain_rows,
                compatible_grate_rows=compatible_rows,
                optional_accessory_rows=optional_rows,
                valid_grate_components=len(valid_grates),
                proposed_n=len(proposed_ids),
                proposed_examples=proposed_ids[:5],
                current_assembled=protected_rows,
                status=status,
                blocking_reason=reason,
            )
        )

    duplicates = sorted(set(pid for pid in prod_ids.tolist() if pid in set(comp_ids.tolist())))
    missing_component_rows = sorted({cid for cid in bom_component_id.tolist() if cid and cid not in excl_ids})
    dangling_component_ids = int(((bom_component_id != "") & ~bom_component_id.isin(excl_ids)).sum())
    self_reference_rows = int(((bom_product_id != "") & (bom_product_id == bom_component_id)).sum())
    grate_ids = {cid for cid in excl_ids if "grate" in cid.lower()}
    grate_to_grate_links = int(((bom_option_type == "compatible_grate") & bom_product_id.isin(grate_ids) & bom_component_id.isin(grate_ids)).sum())

    base_rows = products[~prod_ids.str.startswith("aco-assembled-")]
    missing_base_scoring_fields: dict[str, list[str]] = {}
    for _, r in base_rows.iterrows():
        missing = [f for f in HYDRAULIC_FIELDS if pd.isna(r.get(f, "")) or str(r.get(f, "")).strip() == ""]
        if missing:
            pid = str(r.get("product_id") or "")
            if pid:
                missing_base_scoring_fields[pid] = missing

    leaked_products = sorted([pid for pid in prod_ids.tolist() if pid in excl_ids])
    leaked_comparison = sorted([pid for pid in comp_ids.tolist() if pid in excl_ids])

    return family_reports, GlobalRisk(duplicates, missing_component_rows, dangling_component_ids, self_reference_rows, grate_to_grate_links, missing_base_scoring_fields, leaked_products, leaked_comparison)


def print_report(reports: list[FamilyDiagnostic], risk: GlobalRisk) -> None:
    print("ACO assembly readiness diagnostic (read-only; no pipeline behavior changes)")
    for r in reports:
        print(f"\nFamily: {r.family}")
        print(f"* base rows: {r.base_rows}")
        print(f"* direct protected product rows: {r.protected_rows}")
        print(f"* valid drain/body rows with hydraulic fields: {r.valid_drain_rows}")
        print(f"* compatible_grate rows: {r.compatible_grate_rows}")
        print(f"* optional_accessory rows: {r.optional_accessory_rows}")
        print(f"* valid grate components: {r.valid_grate_components}")
        print(f"* proposed assembled N: {r.proposed_n}")
        print(f"* proposed assembled product_id examples: {r.proposed_examples}")
        print(f"* current assembled: {r.current_assembled}")
        print(f"* status: {r.status}")
        if r.blocking_reason:
            print(f"* blocking reason: {r.blocking_reason}")

    print("\nRisk checks")
    print(f"* duplicate assembled IDs (Products∩Comparison): {len(risk.duplicate_ids)}")
    print(f"* missing component_id in Components/excluded: {len(risk.missing_component_rows)}")
    print(f"* dangling component_id: {risk.dangling_component_ids}")
    print(f"* self-reference: {risk.self_reference_rows}")
    print(f"* grate-to-grate links: {risk.grate_to_grate_links}")
    print(f"* missing base scoring fields: {len(risk.missing_base_scoring_fields)}")
    print(f"* component leakage into Products: {len(risk.leaked_component_ids_in_products)}")
    print(f"* component leakage into Comparison: {len(risk.leaked_component_ids_in_comparison)}")


def main() -> int:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, _evidence, bom = pipeline.run_update(registry, default_config())
    _coverage = _scoring_field_coverage(products, comparison)
    reports, risk = compute_readiness(products, comparison, excluded, bom)
    print_report(reports, risk)
    return 0


if __name__ == "__main__":
    sys.exit(main())
