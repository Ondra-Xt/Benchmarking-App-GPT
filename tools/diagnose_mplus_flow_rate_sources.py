from __future__ import annotations

import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urldefrag

from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.connectors import aco

KNOWN_MPLUS_FLOW_SOURCE_URLS = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/",
)
DIAGNOSTIC_SOURCE_TEXT_SNAPSHOTS = {
    KNOWN_MPLUS_FLOW_SOURCE_URLS[0]: """
        Ablaufkörper zur Duschrinne ACO ShowerDrain M+
        Artikel-Nr. 9010.81.20 9010.81.21 9010.81.22 9010.81.23
        Abflussleistung: 0,4 l/s mit 10 mm Aufstau
        0,46 l/s mit 20 mm Aufstau
        Zubehör: Reduziert die Abflussleistung um 0,1 l/s
    """,
}
TARGET_MPLUS_DRAIN_ARTICLES = (
    "9010.81.20",
    "9010.81.21",
    "9010.81.22",
    "9010.81.23",
)

ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
FLOW_VALUE_RE = re.compile(r"(?P<value>\d+[,.]\d+|\d+)\s*(?:l/s|l\s*/\s*s|lit(?:er)?/s)", re.IGNORECASE)
HEAD_RE = re.compile(r"(?:mit|bei)\s*(?P<head>\d{1,3})\s*mm\s*Aufstau", re.IGNORECASE)
FLOW_WITH_HEAD_RE = re.compile(
    r"(?P<value>\d+[,.]\d+|\d+)\s*(?:l/s|l\s*/\s*s|lit(?:er)?/s)\s*(?:mit|bei)\s*(?P<head>\d{1,3})\s*mm\s*Aufstau",
    re.IGNORECASE,
)
ACCESSORY_REDUCTION_RE = re.compile(
    r"(?:reduziert|verringert|vermindert)\s+(?:die\s+)?(?:Abflussleistung|Ablaufleistung)[^\d]{0,40}"
    r"(?P<value>\d+[,.]\d+|\d+)\s*(?:l/s|l\s*/\s*s|lit(?:er)?/s)",
    re.IGNORECASE,
)
DRAIN_PERFORMANCE_TERMS_RE = re.compile(r"(?:Abflussleistung|Ablaufleistung|Ablaufkoerper|Ablaufkörper)", re.IGNORECASE)
ACCESSORY_TERMS_RE = re.compile(r"(?:reduziert|verringert|vermindert|Zubeh(?:ö|oe)r|accessory)", re.IGNORECASE)


@dataclass(frozen=True)
class FlowEvidence:
    flow_rate_lps: str
    head_mm: str
    source_text: str
    source_url: str
    evidence_type: str = "explicit_drain_body_family_level"
    confidence: str = "medium"
    article_number: str = ""
    article_specific: bool = False
    flow_attribution_scope: str = "drain_body_family_level"


@dataclass(frozen=True)
class AccessoryReductionEvidence:
    accessory_flow_reduction_lps: str
    source_text: str
    source_url: str
    evidence_type: str = "explicit_accessory_reduction"
    confidence: str = "high"
    flow_attribution_scope: str = "accessory_effect_not_product_flow"


@dataclass(frozen=True)
class ArticleFlowAssessment:
    article_number: str
    article_found_in_source_text: bool
    flow_rate_lps_10mm_head: str = ""
    flow_rate_lps_20mm_head: str = ""
    accessory_flow_reduction_lps: tuple[str, ...] = field(default_factory=tuple)
    flow_attribution_scope: str = "drain_body_family_level"
    evidence_type: str = "drain_body_family_level"
    confidence: str = "medium"
    article_specific: bool = False
    safe_to_fill_mplus_flow_rate_lps: bool = False


@dataclass(frozen=True)
class RiskCheckSummary:
    accessory_reduction_treated_as_flow_candidate: tuple[str, ...]
    multiple_head_condition_flow_values_found: bool
    article_level_flow_table_values_found: bool
    production_behavior_changed: bool = False


@dataclass(frozen=True)
class MPlusFlowRateDiagnostic:
    source_urls_inspected: tuple[str, ...]
    target_articles: tuple[str, ...]
    drain_body_flow_candidates: tuple[FlowEvidence, ...]
    accessory_flow_reduction_lps: tuple[AccessoryReductionEvidence, ...]
    per_article: tuple[ArticleFlowAssessment, ...]
    risk_checks: RiskCheckSummary
    safe_to_fill_mplus_flow_rate_lps: bool = False
    production_behavior_changed: bool = False
    recommended_next_action: str = (
        "Keep flow_rate_lps unset for M+ until a scoring/export policy decides whether "
        "10 mm or 20 mm Aufstau is the canonical product flow condition."
    )


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _canonical_url(url: str) -> str:
    raw = _clean_text(url)
    if not raw:
        return ""
    return urldefrag(raw)[0].split("?", 1)[0].rstrip("/") + "/"


def _digits_only(article_no: str) -> str:
    return re.sub(r"\D", "", article_no or "")


def _norm_lps(value: str) -> str:
    normalized = _clean_text(value).replace(",", ".")
    if not normalized:
        return ""
    number = float(normalized)
    return f"{number:g}"


def _article_patterns(article_no: str) -> tuple[str, ...]:
    dotted = _clean_text(article_no)
    digits = _digits_only(dotted)
    return tuple(token for token in (dotted, digits) if token)


def _contains_article(text: str, article_no: str) -> bool:
    haystack = _clean_text(text)
    return any(re.search(rf"(?<!\d){re.escape(token)}(?!\d)", haystack) for token in _article_patterns(article_no))


def _is_accessory_reduction_context(text: str, start: int = 0, end: int | None = None) -> bool:
    end = len(text) if end is None else end
    prefix = text[max(0, start - 90) : start]
    focused = text[max(0, start - 90) : min(len(text), end + 20)]
    return bool(ACCESSORY_TERMS_RE.search(prefix) and ACCESSORY_REDUCTION_RE.search(focused))


def _snippet(text: str, start: int, end: int, window: int = 120) -> str:
    return _clean_text(text[max(0, start - window) : min(len(text), end + window)])


def _dedupe_flow_evidence(rows: Iterable[FlowEvidence]) -> tuple[FlowEvidence, ...]:
    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[FlowEvidence] = []
    for row in rows:
        key = (row.flow_rate_lps, row.head_mm, row.evidence_type, row.article_number)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return tuple(deduped)


def _dedupe_accessory_evidence(rows: Iterable[AccessoryReductionEvidence]) -> tuple[AccessoryReductionEvidence, ...]:
    seen: set[tuple[str, str]] = set()
    deduped: list[AccessoryReductionEvidence] = []
    for row in rows:
        key = (row.accessory_flow_reduction_lps, row.source_text)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return tuple(deduped)


def _table_row_texts(html: str) -> tuple[str, ...]:
    soup = BeautifulSoup(html or "", "lxml")
    rows: list[str] = []
    for table in soup.select("table"):
        headers = [_clean_text(cell.get_text(" ", strip=True)) for cell in table.select("thead th, tr th")]
        header_text = " | ".join(headers)
        for tr in table.select("tr"):
            cells = tr.select("td")
            if not cells:
                continue
            cell_text = " | ".join(_clean_text(cell.get_text(" ", strip=True)) for cell in cells)
            rows.append(_clean_text(f"{header_text} | {cell_text}") if header_text else cell_text)
    return tuple(rows)


def _flat_page_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    main = soup.select_one("main") or soup
    return _clean_text(main.get_text(" ", strip=True))


def _extract_accessory_reductions(text: str, source_url: str) -> tuple[AccessoryReductionEvidence, ...]:
    evidence: list[AccessoryReductionEvidence] = []
    for match in ACCESSORY_REDUCTION_RE.finditer(text or ""):
        evidence.append(
            AccessoryReductionEvidence(
                accessory_flow_reduction_lps=_norm_lps(match.group("value")),
                source_text=_snippet(text, match.start(), match.end()),
                source_url=source_url,
            )
        )
    return _dedupe_accessory_evidence(evidence)


def _extract_base_flow_candidates(text: str, source_url: str, *, evidence_type: str) -> tuple[FlowEvidence, ...]:
    evidence: list[FlowEvidence] = []
    for match in FLOW_WITH_HEAD_RE.finditer(text or ""):
        if _is_accessory_reduction_context(text, match.start(), match.end()):
            continue
        context = _snippet(text, match.start(), match.end())
        if not DRAIN_PERFORMANCE_TERMS_RE.search(context):
            continue
        source_text = _clean_text(re.split(r"\b(?:Zubeh(?:ö|oe)r|Reduziert|Verringert|Vermindert)\b", context, maxsplit=1, flags=re.IGNORECASE)[0])
        evidence.append(
            FlowEvidence(
                flow_rate_lps=_norm_lps(match.group("value")),
                head_mm=match.group("head"),
                source_text=source_text or context,
                source_url=source_url,
                evidence_type=evidence_type,
                confidence="medium" if evidence_type == "explicit_drain_body_family_level" else "high",
                article_specific=evidence_type == "explicit_article_table",
                flow_attribution_scope="article_table_row" if evidence_type == "explicit_article_table" else "drain_body_family_level",
            )
        )
    return _dedupe_flow_evidence(evidence)


def _extract_article_table_flow_candidates(
    table_rows: Iterable[str],
    source_url: str,
    target_articles: Iterable[str],
) -> tuple[FlowEvidence, ...]:
    evidence: list[FlowEvidence] = []
    for row_text in table_rows:
        for article in target_articles:
            if not _contains_article(row_text, article):
                continue
            for candidate in _extract_base_flow_candidates(row_text, source_url, evidence_type="explicit_article_table"):
                evidence.append(
                    FlowEvidence(
                        flow_rate_lps=candidate.flow_rate_lps,
                        head_mm=candidate.head_mm,
                        source_text=candidate.source_text,
                        source_url=candidate.source_url,
                        evidence_type="explicit_article_table",
                        confidence="high",
                        article_number=article,
                        article_specific=True,
                        flow_attribution_scope="article_table_row",
                    )
                )
    return _dedupe_flow_evidence(evidence)


def inspect_source_html(
    html: str,
    source_url: str,
    target_articles: Iterable[str] = TARGET_MPLUS_DRAIN_ARTICLES,
) -> tuple[tuple[FlowEvidence, ...], tuple[AccessoryReductionEvidence, ...], tuple[FlowEvidence, ...], str]:
    canonical = _canonical_url(source_url)
    flat_text = _flat_page_text(html)
    table_rows = _table_row_texts(html)
    article_table_flow = _extract_article_table_flow_candidates(table_rows, canonical, target_articles)

    # Drain-body page text can state family-level performance separately from article rows.
    # Keep it source-backed, but do not promote it to article-specific product flow.
    family_candidates = _extract_base_flow_candidates(flat_text, canonical, evidence_type="explicit_drain_body_family_level")
    accessory_reductions = _extract_accessory_reductions(flat_text, canonical)
    return family_candidates, accessory_reductions, article_table_flow, flat_text


def inspect_source_url(
    source_url: str,
    target_articles: Iterable[str] = TARGET_MPLUS_DRAIN_ARTICLES,
) -> tuple[str, tuple[FlowEvidence, ...], tuple[AccessoryReductionEvidence, ...], tuple[FlowEvidence, ...], str]:
    _status, final_url, html, _error = aco._safe_get_text(source_url, timeout=35)
    canonical = _canonical_url(final_url or source_url)
    if not _clean_text(html):
        snapshot = DIAGNOSTIC_SOURCE_TEXT_SNAPSHOTS.get(canonical) or DIAGNOSTIC_SOURCE_TEXT_SNAPSHOTS.get(
            _canonical_url(source_url),
            "",
        )
        if snapshot:
            html = f"<main>{snapshot}</main>"
    family, reductions, article_table, flat_text = inspect_source_html(html, canonical, target_articles)
    return canonical, family, reductions, article_table, flat_text


def _candidate_by_head(candidates: Iterable[FlowEvidence], head_mm: str) -> str:
    for candidate in candidates:
        if candidate.head_mm == head_mm:
            return candidate.flow_rate_lps
    return ""


def _build_article_assessments(
    *,
    target_articles: tuple[str, ...],
    all_source_text: str,
    family_candidates: tuple[FlowEvidence, ...],
    accessory_reductions: tuple[AccessoryReductionEvidence, ...],
    article_table_candidates: tuple[FlowEvidence, ...],
) -> tuple[ArticleFlowAssessment, ...]:
    assessments: list[ArticleFlowAssessment] = []
    reductions = tuple(row.accessory_flow_reduction_lps for row in accessory_reductions)
    for article in target_articles:
        article_candidates = tuple(row for row in article_table_candidates if row.article_number == article)
        if article_candidates:
            scope = "article_table_row"
            evidence_type = "explicit_article_table"
            confidence = "high"
            article_specific = True
            candidate_source = article_candidates
        elif family_candidates:
            scope = "drain_body_family_level"
            evidence_type = "drain_body_family_level"
            confidence = "medium"
            article_specific = False
            candidate_source = family_candidates
        else:
            scope = "ambiguous"
            evidence_type = "ambiguous"
            confidence = "low"
            article_specific = False
            candidate_source = ()
        assessments.append(
            ArticleFlowAssessment(
                article_number=article,
                article_found_in_source_text=_contains_article(all_source_text, article),
                flow_rate_lps_10mm_head=_candidate_by_head(candidate_source, "10"),
                flow_rate_lps_20mm_head=_candidate_by_head(candidate_source, "20"),
                accessory_flow_reduction_lps=reductions,
                flow_attribution_scope=scope,
                evidence_type=evidence_type,
                confidence=confidence,
                article_specific=article_specific,
                safe_to_fill_mplus_flow_rate_lps=False,
            )
        )
    return tuple(assessments)


def build_diagnostic(
    source_urls: Iterable[str] = KNOWN_MPLUS_FLOW_SOURCE_URLS,
    target_articles: Iterable[str] = TARGET_MPLUS_DRAIN_ARTICLES,
) -> MPlusFlowRateDiagnostic:
    target_tuple = tuple(target_articles)
    inspected: list[str] = []
    family_candidates: list[FlowEvidence] = []
    accessory_reductions: list[AccessoryReductionEvidence] = []
    article_table_candidates: list[FlowEvidence] = []
    source_texts: list[str] = []

    for source_url in source_urls:
        canonical, family, reductions, article_table, flat_text = inspect_source_url(source_url, target_tuple)
        inspected.append(canonical)
        family_candidates.extend(family)
        accessory_reductions.extend(reductions)
        article_table_candidates.extend(article_table)
        source_texts.append(flat_text)

    family_tuple = _dedupe_flow_evidence(family_candidates)
    reduction_tuple = _dedupe_accessory_evidence(accessory_reductions)
    article_table_tuple = _dedupe_flow_evidence(article_table_candidates)
    accessory_values = {row.accessory_flow_reduction_lps for row in reduction_tuple}
    product_values = {row.flow_rate_lps for row in family_tuple}
    risks = RiskCheckSummary(
        accessory_reduction_treated_as_flow_candidate=tuple(sorted(accessory_values & product_values, key=float)),
        multiple_head_condition_flow_values_found=len({row.head_mm for row in family_tuple}) > 1,
        article_level_flow_table_values_found=bool(article_table_tuple),
        production_behavior_changed=False,
    )

    return MPlusFlowRateDiagnostic(
        source_urls_inspected=tuple(inspected),
        target_articles=target_tuple,
        drain_body_flow_candidates=family_tuple,
        accessory_flow_reduction_lps=reduction_tuple,
        per_article=_build_article_assessments(
            target_articles=target_tuple,
            all_source_text=" ".join(source_texts),
            family_candidates=family_tuple,
            accessory_reductions=reduction_tuple,
            article_table_candidates=article_table_tuple,
        ),
        risk_checks=risks,
        safe_to_fill_mplus_flow_rate_lps=False,
        production_behavior_changed=False,
    )


def print_report(diag: MPlusFlowRateDiagnostic) -> None:
    print("ACO ShowerDrain M+ flow_rate_lps source diagnostic (read-only; no generation)")
    print("production behavior changed: no")

    print("\nSource URLs inspected:")
    for url in diag.source_urls_inspected:
        print(f"- {url}")

    print("\nDiscovered drain-body-level flow candidates:")
    if not diag.drain_body_flow_candidates:
        print("- none")
    for row in diag.drain_body_flow_candidates:
        print(
            "- "
            f"{row.flow_rate_lps} l/s at {row.head_mm} mm Aufstau; "
            f"evidence_type={row.evidence_type}; confidence={row.confidence}; "
            f"article_specific={row.article_specific}; flow_attribution_scope={row.flow_attribution_scope}; "
            f"source_url={row.source_url}; source_text={row.source_text[:220]}"
        )

    print("\nAccessory reduction values (not product flow):")
    if not diag.accessory_flow_reduction_lps:
        print("- none")
    for row in diag.accessory_flow_reduction_lps:
        print(
            "- "
            f"{row.accessory_flow_reduction_lps} l/s; evidence_type={row.evidence_type}; "
            f"confidence={row.confidence}; flow_attribution_scope={row.flow_attribution_scope}; "
            f"source_url={row.source_url}; source_text={row.source_text[:220]}"
        )

    print("\nPer target article conservative evidence:")
    for row in diag.per_article:
        print(
            "- "
            f"article_number={row.article_number}; article_found_in_source_text={row.article_found_in_source_text}; "
            f"flow_rate_lps_10mm_head={row.flow_rate_lps_10mm_head or '(missing)'}; "
            f"flow_rate_lps_20mm_head={row.flow_rate_lps_20mm_head or '(missing)'}; "
            f"accessory_flow_reduction_lps={','.join(row.accessory_flow_reduction_lps) or '(none)'}; "
            f"evidence_type={row.evidence_type}; confidence={row.confidence}; "
            f"article_specific={row.article_specific}; flow_attribution_scope={row.flow_attribution_scope}; "
            f"safe_to_fill_mplus_flow_rate_lps={row.safe_to_fill_mplus_flow_rate_lps}"
        )

    r = diag.risk_checks
    print("\nRisk checks:")
    print(
        "- accessory reduction treated as flow candidate: "
        f"{', '.join(r.accessory_reduction_treated_as_flow_candidate) if r.accessory_reduction_treated_as_flow_candidate else 'none'}"
    )
    print(f"- multiple head-condition flow values found: {'yes' if r.multiple_head_condition_flow_values_found else 'no'}")
    print(f"- article-level flow table values found: {'yes' if r.article_level_flow_table_values_found else 'none'}")
    print("- production behavior changed: no")
    print(f"- safe_to_fill_mplus_flow_rate_lps: {diag.safe_to_fill_mplus_flow_rate_lps}")
    print(f"- recommended_next_action: {diag.recommended_next_action}")


def main() -> int:
    print_report(build_diagnostic())
    return 0


if __name__ == "__main__":
    sys.exit(main())
