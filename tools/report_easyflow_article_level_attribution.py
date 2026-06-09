from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src import excel_export
from src.canonical_aco_export import build_canonical_aco_frames
from src.config import default_config
from tools import report_easyflow_article_variants

EASYFLOW_FAMILY = "easyflow"
CANDIDATE_VARIANT_TYPE = "candidate_body_variant"
EVIDENCE_ONLY_STATUS = "candidate_variant"
AMBIGUOUS_FIELDS = ("flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm")
ARTICLE_NUMBER_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")


@dataclass(frozen=True)
class AssemblyAttribution:
    assembled_product_id: str
    base_product_id: str
    component_id: str
    status: str
    unique_article_attribution_found: bool
    attributed_article_number: str
    matching_article_numbers: tuple[str, ...]
    distinct_flow_rates_lps: tuple[float, ...]
    distinct_height_ranges_mm: tuple[str, ...]
    blocking_reason: str


@dataclass(frozen=True)
class AttributionReport:
    assembly_results: tuple[AssemblyAttribution, ...]
    sheet_counts: dict[str, int]

    @property
    def unique_attribution_found_for_all(self) -> bool:
        return bool(self.assembly_results) and all(
            row.unique_article_attribution_found for row in self.assembly_results
        )


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _normalized_article(value: Any) -> str:
    digits = re.sub(r"\D+", "", _text(value))
    return digits if len(digits) == 8 else ""


def _article_display(value: Any) -> str:
    digits = _normalized_article(value)
    if not digits:
        return _text(value)
    return f"{digits[:4]}.{digits[4:6]}.{digits[6:]}"


def _float_value(value: Any) -> float | None:
    text = _text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _int_value(value: Any) -> int | None:
    number = _float_value(value)
    return None if number is None else int(number)


def _outlet_tokens(value: Any) -> set[str]:
    return {
        f"DN{match}"
        for match in re.findall(r"(?:DN\s*)?(\d{2,3})", _text(value).upper())
    }


def _explicit_article_references(*rows: pd.Series) -> tuple[str, ...]:
    references: list[str] = []
    fields = (
        "base_article_number",
        "article_number",
        "source_text_or_reason",
        "source_status_note",
    )
    for row in rows:
        for field in fields:
            value = _text(row.get(field, ""))
            if field in {"base_article_number", "article_number"}:
                candidates: Iterable[str] = (value,)
            else:
                candidates = ARTICLE_NUMBER_RE.findall(value)
            for candidate in candidates:
                normalized = _normalized_article(candidate)
                if normalized and normalized not in references:
                    references.append(normalized)
    return tuple(references)


def _variant_matches_assembly(variant: pd.Series, assembly: pd.Series) -> bool:
    assembly_ws = _int_value(assembly.get("water_seal_mm"))
    variant_ws = _int_value(variant.get("water_seal_mm"))
    if assembly_ws is not None and variant_ws is not None and assembly_ws != variant_ws:
        return False

    assembly_dn = _outlet_tokens(assembly.get("outlet_dn"))
    variant_dn = _outlet_tokens(variant.get("outlet_dn"))
    if assembly_dn and variant_dn and assembly_dn.isdisjoint(variant_dn):
        return False
    return True


def _source_backed_easyflow_variants(article_variants: pd.DataFrame) -> pd.DataFrame:
    variants = pd.DataFrame() if article_variants is None else article_variants.copy()
    required = {"article_number", "source_url", "variant_type", "product_family"}
    if variants.empty or not required.issubset(variants.columns):
        return variants.iloc[0:0].copy()

    mask = (
        variants["variant_type"].fillna("").astype(str).str.strip().eq(CANDIDATE_VARIANT_TYPE)
        & variants["product_family"].fillna("").astype(str).str.strip().str.lower().eq(EASYFLOW_FAMILY)
        & variants["article_number"].map(_normalized_article).ne("")
        & variants["source_url"].fillna("").astype(str).str.strip().ne("")
    )
    if "attribution_status" in variants.columns:
        mask &= variants["attribution_status"].fillna("").astype(str).str.strip().eq(EVIDENCE_ONLY_STATUS)
    return variants[mask].copy()


def _candidate_values(candidates: pd.DataFrame) -> tuple[tuple[str, ...], tuple[float, ...], tuple[str, ...]]:
    articles = tuple(
        sorted({_article_display(value) for value in candidates.get("article_number", pd.Series(dtype=str)) if _normalized_article(value)})
    )
    flows = tuple(
        sorted({value for value in (_float_value(raw) for raw in candidates.get("flow_rate_lps", pd.Series(dtype=object))) if value is not None})
    )
    heights = tuple(
        sorted(
            {
                f"{minimum}-{maximum}"
                for minimum, maximum in zip(
                    (_int_value(raw) for raw in candidates.get("height_adj_min_mm", pd.Series(dtype=object))),
                    (_int_value(raw) for raw in candidates.get("height_adj_max_mm", pd.Series(dtype=object))),
                )
                if minimum is not None and maximum is not None
            }
        )
    )
    return articles, flows, heights


def assess_easyflow_article_attribution(
    final_assemblies: pd.DataFrame,
    final_set_details: pd.DataFrame,
    article_variants: pd.DataFrame,
) -> AttributionReport:
    """Assess Easyflow article attribution without mutating production/export frames."""
    assemblies = pd.DataFrame() if final_assemblies is None else final_assemblies.copy()
    details = pd.DataFrame() if final_set_details is None else final_set_details.copy()
    variants = _source_backed_easyflow_variants(article_variants)

    family = assemblies.get("assembled_family", pd.Series("", index=assemblies.index)).fillna("").astype(str).str.lower()
    product_ids = assemblies.get("product_id", pd.Series("", index=assemblies.index)).fillna("").astype(str)
    easyflow = assemblies[(family.eq(EASYFLOW_FAMILY)) | product_ids.str.startswith("aco-assembled-easyflow-")].copy()

    results: list[AssemblyAttribution] = []
    for _, assembly in easyflow.iterrows():
        assembled_id = _text(assembly.get("product_id") or assembly.get("assembled_product_id"))
        detail_matches = details[
            details.get("assembled_product_id", pd.Series("", index=details.index)).fillna("").astype(str).eq(assembled_id)
        ]
        detail = detail_matches.iloc[0] if not detail_matches.empty else pd.Series(dtype=object)
        parsed_base_id, parsed_component_id = excel_export._parse_final_set_parts(assembled_id, EASYFLOW_FAMILY)
        base_product_id = _text(detail.get("base_product_id")) or parsed_base_id
        component_id = _text(detail.get("component_id")) or parsed_component_id

        candidates = variants.copy()
        if base_product_id and "base_product_id" in candidates.columns:
            candidates = candidates[
                candidates["base_product_id"].fillna("").astype(str).str.strip().eq(base_product_id)
            ]
        candidates = candidates[
            candidates.apply(lambda row: _variant_matches_assembly(row, assembly), axis=1)
        ]

        explicit_refs = set(_explicit_article_references(assembly, detail))
        if explicit_refs:
            explicitly_linked = candidates[
                candidates["article_number"].map(_normalized_article).isin(explicit_refs)
            ]
        else:
            explicitly_linked = candidates.iloc[0:0]

        selected = explicitly_linked if len(explicitly_linked) == 1 else candidates
        articles, flows, heights = _candidate_values(selected)

        if len(explicitly_linked) == 1:
            attributed = _article_display(explicitly_linked.iloc[0].get("article_number"))
            status = "unique_explicit_article_reference"
            unique = True
            reason = ""
        elif explicit_refs and explicitly_linked.empty:
            attributed = ""
            status = "explicit_article_reference_not_in_evidence"
            unique = False
            refs = ", ".join(_article_display(ref) for ref in sorted(explicit_refs))
            reason = (
                f"The Easyflow assembly references article {refs}, but no matching source-backed "
                "candidate exists in Article_Variants; keep flow/height blocked."
            )
        elif len(explicitly_linked) > 1:
            attributed = ""
            status = "multiple_explicit_article_references"
            unique = False
            reason = "Multiple explicit Easyflow article references remain linked to this assembly; keep flow/height blocked."
        elif len(candidates) == 1:
            attributed = _article_display(candidates.iloc[0].get("article_number"))
            status = "unique_source_backed_technical_match"
            unique = True
            reason = ""
        elif candidates.empty:
            attributed = ""
            status = "no_source_backed_article_match"
            unique = False
            reason = (
                "No source-backed Easyflow article variant matches the assembly base ID and inherited "
                "WS/DN facts; keep flow/height blocked."
            )
        else:
            attributed = ""
            status = "multiple_source_backed_article_matches"
            unique = False
            article_text = ", ".join(articles)
            flow_text = ", ".join(f"{value:g}" for value in flows) or "unknown"
            height_text = ", ".join(heights) or "unknown"
            reason = (
                f"The family-level Easyflow base {base_product_id or '(unknown)'} maps to {len(candidates)} "
                f"source-backed WS/DN-compatible articles ({article_text}). The selected top component "
                f"{component_id or '(unknown)'} does not identify the drain-body article. Candidate flow "
                f"values={flow_text} l/s and height ranges={height_text} mm; keep flow_rate_lps, "
                "height_adj_min_mm, and height_adj_max_mm blocked until one article is explicitly proven."
            )

        results.append(
            AssemblyAttribution(
                assembled_product_id=assembled_id,
                base_product_id=base_product_id,
                component_id=component_id,
                status=status,
                unique_article_attribution_found=unique,
                attributed_article_number=attributed,
                matching_article_numbers=articles,
                distinct_flow_rates_lps=flows,
                distinct_height_ranges_mm=heights,
                blocking_reason=reason,
            )
        )

    counts = {
        "Products": 0,
        "BOM_Options": 0,
        "Final_Assemblies": len(assemblies),
        "Final_Set_Details": len(details),
        "Article_Variants": len(article_variants),
        "Easyflow_Final_Assemblies": len(easyflow),
        "Easyflow_Unique_Attributions": sum(row.unique_article_attribution_found for row in results),
        "Easyflow_Blocked_Attributions": sum(not row.unique_article_attribution_found for row in results),
    }
    return AttributionReport(tuple(results), counts)


def report_from_workbook(path: str | Path) -> AttributionReport:
    with pd.ExcelFile(path, engine="openpyxl") as workbook:
        sheets = {
            name: pd.read_excel(workbook, sheet_name=name)
            for name in ("Products", "BOM_Options", "Final_Assemblies", "Final_Set_Details", "Article_Variants")
        }
    report = assess_easyflow_article_attribution(
        sheets["Final_Assemblies"], sheets["Final_Set_Details"], sheets["Article_Variants"]
    )
    counts = dict(report.sheet_counts)
    counts["Products"] = len(sheets["Products"])
    counts["BOM_Options"] = len(sheets["BOM_Options"])
    return AttributionReport(report.assembly_results, counts)


def build_live_report() -> AttributionReport:
    frames = build_canonical_aco_frames(default_config())
    final_assemblies = excel_export._extract_final_assemblies(frames.products)
    final_set_details = excel_export._extract_final_set_details(
        final_assemblies, frames.bom_options, frames.excluded
    )
    article_variants = report_easyflow_article_variants.build_article_variants_dataframe(
        frames.registry, frames.products
    )
    report = assess_easyflow_article_attribution(final_assemblies, final_set_details, article_variants)
    counts = dict(report.sheet_counts)
    counts["Products"] = len(frames.products)
    counts["BOM_Options"] = len(frames.bom_options)
    return AttributionReport(report.assembly_results, counts)


def print_report(report: AttributionReport) -> None:
    print("ACO Easyflow article-level attribution diagnostic (read-only)")
    print("\nBaseline counts:")
    for name, count in report.sheet_counts.items():
        print(f"- {name}: {count}")

    print("\nPer-assembly attribution:")
    for row in report.assembly_results:
        print(f"- {row.assembled_product_id}")
        print(f"  base_product_id: {row.base_product_id}")
        print(f"  component_id: {row.component_id}")
        print(f"  status: {row.status}")
        print(f"  unique_article_attribution_found: {row.unique_article_attribution_found}")
        print(f"  attributed_article_number: {row.attributed_article_number or 'none'}")
        print(f"  matching_articles: {', '.join(row.matching_article_numbers) or 'none'}")
        if row.blocking_reason:
            print(f"  blocking_reason: {row.blocking_reason}")

    print("\nConclusion:")
    if report.unique_attribution_found_for_all:
        print("A) Easyflow unique article-level attribution found for every assembly; diagnostic-only.")
    else:
        print("B) Easyflow unique article-level attribution not found; remains blocked with article-specific evidence.")
    print("- No Products, Comparison, BOM_Options, Final_Assemblies, Final_Set_Details, scoring, or XLSX output is modified.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report Easyflow article-level attribution without changing production output.")
    parser.add_argument("--xlsx", type=Path, help="Read an existing canonical workbook instead of running live ACO discovery.")
    args = parser.parse_args(argv)
    report = report_from_workbook(args.xlsx) if args.xlsx else build_live_report()
    print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
