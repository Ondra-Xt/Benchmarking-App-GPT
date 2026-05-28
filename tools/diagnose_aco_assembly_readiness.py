import sys
from dataclasses import dataclass

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco

REQUIRED_SCORING_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)

FAMILIES = (
    "showerdrain_splus",
    "showerdrain_c",
    "showerdrain_cplus",
    "showerdrain_mplus",
    "showerdrain_eplus",
    "easyflow",
    "easyflowplus",
)


@dataclass
class FamilyReadiness:
    family: str
    status: str
    current_assembled: int
    proposed_n: int
    reasons: list[str]
    proposed_ids: list[str]
    missing_base_scoring_fields: dict[str, list[str]]
    cross_family_mixing: list[str]


@dataclass
class ReadinessDiagnostic:
    families: dict[str, FamilyReadiness]
    duplicate_product_ids_in_products: list[str]
    duplicate_product_ids_in_comparison: list[str]
    duplicate_proposed_ids_by_family: dict[str, list[str]]


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _missing_scoring(row: pd.Series) -> list[str]:
    missing = []
    for field in REQUIRED_SCORING_FIELDS:
        val = row.get(field, "")
        if pd.isna(val) or str(val).strip() == "":
            missing.append(field)
    return missing


def _dup_ids(series: pd.Series) -> list[str]:
    vc = series[series != ""].value_counts()
    return sorted(vc[vc > 1].index.tolist())


def _build_family(products: pd.DataFrame, excluded: pd.DataFrame, bom: pd.DataFrame, family: str) -> FamilyReadiness:
    pids = _norm(products, "product_id")
    fam = _norm(products, "product_family")
    assembled_mask = pids.str.startswith("aco-assembled-")
    current_assembled = int((assembled_mask & (fam == family)).sum())

    reasons: list[str] = []
    proposed_ids: list[str] = []
    cross_family_mixing: list[str] = []
    missing_by_base: dict[str, list[str]] = {}

    if current_assembled > 0:
        return FamilyReadiness(family, "ALREADY_ACTIVE / BASELINE_PROTECTED", current_assembled, 0, ["current assembled > 0"], [], {}, [])

    base_mask = (~assembled_mask) & (fam == family)
    bases = products[base_mask].copy()
    if bases.empty:
        return FamilyReadiness(family, "BLOCKED", 0, 0, ["no base rows"], [], {}, [])

    b_opt = _norm(bom, "option_type").str.lower()
    b_parent = _norm(bom, "parent_family")
    b_ofam = _norm(bom, "option_family")
    b_prod = _norm(bom, "product_id")
    b_comp = _norm(bom, "component_id")
    b_evidence = _norm(bom, "compatibility_evidence").str.lower()

    family_bom = bom[(b_opt == "compatible_grate") & (b_parent == family) & (b_ofam == family)].copy()
    family_bom = family_bom[_norm(family_bom, "product_id").isin(_norm(bases, "product_id"))]
    if family_bom.empty:
        return FamilyReadiness(family, "BLOCKED", 0, 0, ["no compatible_grate BOM rows"], [], {}, [])

    valid_components = set(_norm(excluded, "product_id").tolist())
    base_fam_by_id = dict(zip(_norm(bases, "product_id"), _norm(bases, "product_family")))
    comp_fam_by_id = dict(zip(_norm(excluded, "product_id"), _norm(excluded, "option_family")))

    for idx in family_bom.index:
        base_id = b_prod.loc[idx]
        comp_id = b_comp.loc[idx]
        if not comp_id or comp_id not in valid_components:
            continue
        base_family = base_fam_by_id.get(base_id, "")
        comp_family = comp_fam_by_id.get(comp_id, "")
        explicit_cross = "cross-family" in b_evidence.loc[idx] or "cross family" in b_evidence.loc[idx]
        if base_family != family:
            cross_family_mixing.append(f"base {base_id} family={base_family}")
            continue
        if comp_family != family and not explicit_cross:
            cross_family_mixing.append(f"component {comp_id} family={comp_family}")
            continue
        proposed_ids.append(f"aco-assembled-{base_id}__{comp_id.replace('aco-', '', 1)}")

    if not proposed_ids:
        if cross_family_mixing:
            return FamilyReadiness(family, "BLOCKED", 0, 0, ["cross-family mixing detected without explicit evidence"], [], {}, sorted(set(cross_family_mixing)))
        return FamilyReadiness(family, "BLOCKED", 0, 0, ["no valid grate components"], [], {}, [])

    for _, row in bases.iterrows():
        pid = str(row.get("product_id") or "")
        missing = _missing_scoring(row)
        if missing:
            missing_by_base[pid] = missing

    if missing_by_base:
        reasons.append("proposed rows have missing base scoring fields")
    if cross_family_mixing:
        reasons.append("cross-family mixing detected without explicit evidence")

    if reasons:
        status = "BLOCKED"
    else:
        status = "READY"
    return FamilyReadiness(family, status, 0, len(proposed_ids), reasons, sorted(proposed_ids), missing_by_base, sorted(set(cross_family_mixing)))


def compute_readiness(products: pd.DataFrame, comparison: pd.DataFrame, excluded: pd.DataFrame, bom: pd.DataFrame) -> ReadinessDiagnostic:
    families = {f: _build_family(products, excluded, bom, f) for f in FAMILIES}
    dup_products = _dup_ids(_norm(products, "product_id"))
    dup_comparison = _dup_ids(_norm(comparison, "product_id"))
    dup_proposed = {}
    for f, fr in families.items():
        dup_proposed[f] = _dup_ids(pd.Series(fr.proposed_ids, dtype="string"))
    return ReadinessDiagnostic(families, dup_products, dup_comparison, dup_proposed)


def print_report(diag: ReadinessDiagnostic) -> None:
    print("ACO assembly readiness diagnostic")
    for family in FAMILIES:
        fr = diag.families[family]
        print(f"- {family}: {fr.status}, current assembled = {fr.current_assembled}, proposed N = {fr.proposed_n}")
        for reason in fr.reasons:
            print(f"  reason: {reason}")
        if fr.missing_base_scoring_fields:
            print("  missing base scoring fields:")
            for pid, fields in fr.missing_base_scoring_fields.items():
                print(f"    - {pid} -> {', '.join(fields)}")
        if fr.cross_family_mixing:
            print("  cross-family violations:")
            for m in fr.cross_family_mixing:
                print(f"    - {m}")
        for pid in fr.proposed_ids[:5]:
            print(f"  example: {pid}")

    print("\nGlobal risk checks:")
    print(f"- duplicate product_id within Products: {len(diag.duplicate_product_ids_in_products)}")
    print(f"- duplicate product_id within Comparison: {len(diag.duplicate_product_ids_in_comparison)}")
    print("- duplicate proposed assembled IDs by family:")
    for fam in FAMILIES:
        print(f"  - {fam}: {len(diag.duplicate_proposed_ids_by_family.get(fam, []))}")


def main() -> int:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, _evidence, bom = pipeline.run_update(registry, default_config())
    diag = compute_readiness(products, comparison, excluded, bom)
    print_report(diag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
