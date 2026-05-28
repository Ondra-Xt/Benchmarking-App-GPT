import sys
from dataclasses import dataclass
from typing import Iterable

import pandas as pd
from bs4 import BeautifulSoup

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
    base_field_values: dict[str, dict[str, str]]
    missing_expected_base_columns: list[str]
    cplus_bom_for_base: list[dict[str, str]]
    cplus_bom_family_rows: list[dict[str, str]]
    cplus_bom_candidate_component_ids: list[str]
    candidate_component_status: dict[str, bool]
    candidate_component_rows: list[dict[str, str]]
    products_count: int
    comparison_count: int
    coverage_count: int
    bom_count: int
    cplus_source_urls: list[str]
    cplus_design_urls: list[str]
    cplus_grate_evidence_urls: list[str]
    cplus_evidence_notes: list[str]
    compatibility_mode: str


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _base_slug(product_id: str) -> str:
    prefix = "aco-showerdrain-cplus-"
    if product_id.startswith(prefix):
        return product_id[len(prefix):]
    return product_id


def _classify_compatibility_mode(notes: Iterable[str]) -> str:
    note_text = " ".join(notes).lower()
    if "artikelmatrix" in note_text or "article-to-article" in note_text:
        return "explicit_article_to_article"
    if "base-to-family" in note_text:
        return "explicit_base_to_family"
    if "implicit family-level" in note_text:
        return "implicit_family_level"
    return "absent"


def compute_cplus_diagnostic(products: pd.DataFrame, comparison: pd.DataFrame, excluded: pd.DataFrame, bom: pd.DataFrame, coverage: pd.DataFrame) -> CPlusDiagnostic:
    product_ids = _norm(products, "product_id")
    base_mask = product_ids.isin(PROTECTED_CPLUS_BASE_IDS)
    bases = products[base_mask].copy()

    bom_option_type = _norm(bom, "option_type").str.lower()
    bom_parent_family = _norm(bom, "parent_family").str.lower()
    bom_option_family = _norm(bom, "option_family").str.lower()
    bom_component_ids = _norm(bom, "component_id")
    bom_product_ids = _norm(bom, "product_id")
    cplus_family_mask = (bom_parent_family == "showerdrain_cplus") | (bom_option_family == "showerdrain_cplus")
    base_bom_mask = bom_product_ids.isin(PROTECTED_CPLUS_BASE_IDS)
    bom_grate = bom[(bom_option_type == "compatible_grate") & cplus_family_mask].copy()

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

    base_field_values: dict[str, dict[str, str]] = {}
    missing_expected_base_columns = [field for field in REQUIRED_SCORING_FIELDS if field not in products.columns]
    missing_base_scoring_fields: dict[str, list[str]] = {}
    for _, row in bases.iterrows():
        pid = str(row.get("product_id") or "")
        missing = []
        observed: dict[str, str] = {}
        for field in REQUIRED_SCORING_FIELDS:
            value = row.get(field, "")
            value_str = "" if pd.isna(value) else str(value)
            observed[field] = value_str
            if pd.isna(value) or str(value).strip() == "":
                missing.append(field)
        base_field_values[pid] = observed
        if missing:
            missing_base_scoring_fields[pid] = missing

    cplus_bom_for_base = bom[base_bom_mask].fillna("").astype(str).to_dict("records")
    cplus_bom_family_rows = bom[cplus_family_mask].fillna("").astype(str).to_dict("records")
    cplus_bom_candidate_component_ids = sorted({cid for cid in bom_component_ids[base_bom_mask | cplus_family_mask].tolist() if cid})
    candidate_component_status = {cid: cid in components_universe for cid in cplus_bom_candidate_component_ids}
    candidate_component_rows = excluded[_norm(excluded, "product_id").isin(cplus_bom_candidate_component_ids)].fillna("").astype(str).to_dict("records")

    cplus_source_urls, cplus_design_urls, cplus_grate_evidence_urls, cplus_evidence_notes = discover_cplus_sources()
    compatibility_mode = _classify_compatibility_mode(cplus_evidence_notes)

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
        base_field_values=base_field_values,
        missing_expected_base_columns=missing_expected_base_columns,
        cplus_bom_for_base=cplus_bom_for_base,
        cplus_bom_family_rows=cplus_bom_family_rows,
        cplus_bom_candidate_component_ids=cplus_bom_candidate_component_ids,
        candidate_component_status=candidate_component_status,
        candidate_component_rows=candidate_component_rows,
        products_count=len(products),
        comparison_count=len(comparison),
        coverage_count=len(coverage),
        bom_count=len(bom),
        cplus_source_urls=cplus_source_urls,
        cplus_design_urls=cplus_design_urls,
        cplus_grate_evidence_urls=cplus_grate_evidence_urls,
        cplus_evidence_notes=cplus_evidence_notes,
        compatibility_mode=compatibility_mode,
    )


def discover_cplus_sources() -> tuple[list[str], list[str], list[str], list[str]]:
    seeds = [
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-cplus/",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/",
    ]
    discovered = set()
    design_urls = set()
    evidence_urls = set()
    evidence_notes = set()
    for seed in seeds:
        discovered.add(seed)
        st, final, html, _err = aco._safe_get_text(seed, timeout=35)
        if st != 200 or not html:
            continue
        discovered.add(final)
        soup = BeautifulSoup(html, "lxml")
        for a_tag in soup.select("a[href]"):
            href = aco._abs(a_tag.get("href") or "", final)
            txt = (a_tag.get_text(" ", strip=True) or "").lower()
            if not href:
                continue
            if "design-rost" in href.lower() or any(k in txt for k in ("design-rost", "design-roste", "rost", "abdeckung", "grate", "cover")):
                design_urls.add(href)
        for u in list(design_urls):
            st_d, final_d, html_d, _err_d = aco._safe_get_text(u, timeout=35)
            if st_d != 200 or not html_d:
                continue
            discovered.add(final_d)
            page_l = html_d.lower()
            if "aco showerdrain c" in page_l and ("c+" in page_l or "cplus" in page_l):
                evidence_urls.add(final_d)
                if "artikel" in page_l and ("matrix" in page_l or "tabelle" in page_l):
                    evidence_notes.add(f"explicit article-to-article hint found on {final_d}")
                elif "kompatibel" in page_l:
                    evidence_notes.add(f"implicit family-level compatibility wording found on {final_d}")
                else:
                    evidence_notes.add(f"base-to-family wording inferred from shared family mention on {final_d}")
    return sorted(discovered), sorted(design_urls), sorted(evidence_urls), sorted(evidence_notes)


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
    print("\nC+ base field values from final Products DataFrame:")
    for pid in sorted(diag.base_field_values):
        print(f"- {pid}")
        for field in REQUIRED_SCORING_FIELDS:
            print(f"  - {field}: {diag.base_field_values[pid].get(field, '')}")
    if diag.missing_expected_base_columns:
        print("Expected scoring columns missing from final Products DataFrame:")
        print(f"- {', '.join(diag.missing_expected_base_columns)}")
        print("Available columns for C+ base rows in Products:")
        print(f"- {', '.join(sorted(diag.base_field_values[next(iter(diag.base_field_values))].keys()))}" if diag.base_field_values else "- (no C+ base rows)")

    print("\nC+ grate matching diagnostics:")
    print("BOM_Options rows where product_id is a protected C+ base ID:")
    for row in diag.cplus_bom_for_base:
        print(f"- {row}")
    print("BOM_Options rows where parent_family or option_family includes showerdrain_cplus:")
    for row in diag.cplus_bom_family_rows:
        print(f"- {row}")
    print("Candidate component_id values from those BOM rows:")
    for cid in diag.cplus_bom_candidate_component_ids:
        print(f"- {cid}")
    print("Candidate component existence in Components/excluded universe:")
    for cid in diag.cplus_bom_candidate_component_ids:
        print(f"- {cid}: {'present' if diag.candidate_component_status.get(cid, False) else 'missing'}")
    print("Matching component rows found in Components/excluded:")
    for row in diag.candidate_component_rows:
        print(f"- {row}")
    print("\nC+ source investigation:")
    print(f"- compatibility mode: {diag.compatibility_mode}")
    print("Source pages discovered:")
    for url in diag.cplus_source_urls:
        print(f"- {url}")
    print("Design grate / cover pages discovered:")
    for url in diag.cplus_design_urls:
        print(f"- {url}")
    print("Pages with C+ and ShowerDrain C grate compatibility text evidence:")
    for url in diag.cplus_grate_evidence_urls:
        print(f"- {url}")
    print("Evidence notes:")
    for note in diag.cplus_evidence_notes:
        print(f"- {note}")


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
