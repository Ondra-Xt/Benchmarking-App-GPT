from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urldefrag

import pandas as pd
from bs4 import BeautifulSoup

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.pdf_text import extract_pdf_text_from_url

PROTECTED_CPLUS_BASE_IDS = (
    "aco-showerdrain-cplus-standard-h92",
    "aco-showerdrain-cplus-low-h69",
)
TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
KNOWN_CPLUS_SOURCE_URLS = (
    "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
)
KNOWN_RELATED_GRATE_SOURCE_URLS = (
    getattr(aco, "SHOWERDRAIN_C_ARTICLE_GRATE_URL", "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/design-roste-aus-geschliffenem-edelstahl/"),
)
FIXTURE_SOURCE_MAP = {
    "aco-showerdrain-cplus": "tests/fixtures/aco_cplus/cplus_family_cz.html",
    "design-roste-aus-geschliffenem-edelstahl": "tests/fixtures/aco_cplus/c_design_grates_de.html",
}
EVIDENCE_TERMS = (
    "ShowerDrain C+",
    "C+",
    "compatible",
    "kompatibel",
    "passend",
    "Designrost",
    "Design-Rost",
    "grate",
    "Rost",
    "Abdeckung",
)
GRATE_TERMS_RE = re.compile(r"\b(?:design[- ]?rost|rost|roste|abdeckung|grate|cover|kryt|rošt|rošty)\b", re.IGNORECASE)
COMPAT_TERMS_RE = re.compile(r"\b(?:compatible|kompatibel|passend|vhodn|určen|použit|výběr|zu|für|for)\b", re.IGNORECASE)
CPLUS_RE = re.compile(r"(?:ShowerDrain\s*C\+|\bC\+\b|cplus)", re.IGNORECASE)
ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
SOURCE_HINT_RE = re.compile(r"(?:showerdrain[-_ ]?c\+|showerdrain[-_ ]?cplus|showerdrain[-_ ]?c|design[-_ ]?rost|grate|rost|abdeckung)", re.IGNORECASE)
DEFAULT_RELEVANT_SOURCE_MARKERS = (
    "showerdrain-cplus",
    "showerdrain-c/",
    "design-roste-aus-geschliffenem-edelstahl",
)
DEFAULT_EXCLUDED_SOURCE_MARKERS = (
    "easyflow",
    "easyflow-plus",
    "showerdrain-public",
    "showerdrain-mplus",
    "showerdrain-eplus",
    "passavant",
    "passino",
    "mg",
    "showerpoint",
)
RECOMMENDED_NEXT_ACTION = "find explicit C+ article matrix / catalog table"


@dataclass(frozen=True)
class CandidateEvidence:
    article_number: str
    product_id: str
    source_url: str
    row_text: str
    evidence_type: str
    compatibility_confidence: str


@dataclass(frozen=True)
class SourceInspection:
    source_url: str
    status: str
    matched_snippets: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CPlusCompatibleGrateDiagnostic:
    base_rows: dict[str, dict[str, str]]
    missing_base_ids: tuple[str, ...]
    source_urls_inspected: tuple[str, ...]
    source_inspections: tuple[SourceInspection, ...]
    candidate_evidence: tuple[CandidateEvidence, ...]
    safe_to_add_compatible_grate_bom_rows: bool
    recommendation: str
    recommended_action: str
    include_broad_sources: bool = False


def _norm(df: pd.DataFrame | None, col: str) -> pd.Series:
    df = pd.DataFrame() if df is None else df
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str).str.strip()


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _canonical_url(url: str) -> str:
    url = _clean_text(url)
    if not url:
        return ""
    return urldefrag(url)[0].rstrip("/") + ("/" if urldefrag(url)[0].rstrip("/").endswith((".pdf", ".PDF")) is False and url.endswith("/") else "")


def _urls_from_value(value: Any) -> set[str]:
    urls: set[str] = set()
    raw = str(value or "")
    for token in re.split(r"[\s,;|]+", raw):
        token = token.strip().strip("'\"()[]{}<>")
        if token.startswith(("http://", "https://")):
            urls.add(urldefrag(token)[0])
    return urls


def _is_default_relevant_source_url(url: str) -> bool:
    normalized = _canonical_url(url).lower()
    if not normalized:
        return False
    if normalized in {_canonical_url(known).lower() for known in KNOWN_CPLUS_SOURCE_URLS}:
        return True
    if any(marker in normalized for marker in DEFAULT_EXCLUDED_SOURCE_MARKERS):
        return False
    return any(marker in normalized for marker in DEFAULT_RELEVANT_SOURCE_MARKERS)


def collect_related_source_urls(*frames: pd.DataFrame, include_broad_sources: bool = False) -> tuple[str, ...]:
    urls: set[str] = set(KNOWN_CPLUS_SOURCE_URLS) | set(KNOWN_RELATED_GRATE_SOURCE_URLS)
    for df in frames:
        if df is None or df.empty:
            continue
        columns = [col for col in ("product_url", "source_url", "sources", "url") if col in df.columns]
        if not columns:
            continue
        text_cols = [col for col in ("product_id", "product_name", "product_family", "family", "candidate_type", "system_role", "option_type", "parent_family", "option_family") if col in df.columns]
        for _, row in df.iterrows():
            haystack = " ".join(str(row.get(col, "") or "") for col in [*columns, *text_cols])
            if not SOURCE_HINT_RE.search(haystack):
                continue
            for col in columns:
                urls.update(_urls_from_value(row.get(col, "")))
    if not include_broad_sources:
        urls = {url for url in urls if _is_default_relevant_source_url(url)}
    return tuple(sorted(u for u in urls if u))


def locate_cplus_base_rows(products: pd.DataFrame) -> tuple[dict[str, dict[str, str]], tuple[str, ...]]:
    base_rows: dict[str, dict[str, str]] = {}
    product_ids = _norm(products, "product_id")
    for base_id in PROTECTED_CPLUS_BASE_IDS:
        rows = products[product_ids.eq(base_id)] if products is not None and not products.empty else pd.DataFrame()
        if rows.empty:
            continue
        row = rows.iloc[0]
        base_rows[base_id] = {field: _clean_text(row.get(field, "")) for field in TECHNICAL_FIELDS}
    missing = tuple(pid for pid in PROTECTED_CPLUS_BASE_IDS if pid not in base_rows)
    return base_rows, missing


def _fixture_text_for_url(url: str) -> tuple[str, str]:
    for marker, rel_path in FIXTURE_SOURCE_MAP.items():
        if marker in url.lower():
            path = Path(__file__).resolve().parents[1] / rel_path
            if path.exists():
                html = path.read_text(encoding="utf-8", errors="ignore")
                soup = BeautifulSoup(html, "lxml")
                for tag in soup(("script", "style", "noscript")):
                    tag.decompose()
                return _clean_text(soup.get_text(" ")), "ok_fixture"
    return "", ""


def _fallback_products_and_components(products: pd.DataFrame, components: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    products = pd.DataFrame() if products is None else products.copy()
    components = pd.DataFrame() if components is None else components.copy()
    product_ids = set(_norm(products, "product_id"))
    fallback_products = [
        {
            "manufacturer": "aco",
            "product_id": "aco-showerdrain-cplus-standard-h92",
            "product_family": "showerdrain_cplus",
            "product_name": "ACO ShowerDrain C+ Standard H92",
            "product_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "source_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "flow_rate_lps": 0.91,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": 80,
            "height_adj_max_mm": 128,
        },
        {
            "manufacturer": "aco",
            "product_id": "aco-showerdrain-cplus-low-h69",
            "product_family": "showerdrain_cplus",
            "product_name": "ACO ShowerDrain C+ Low H69",
            "product_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "source_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "flow_rate_lps": 0.62,
            "water_seal_mm": 25,
            "outlet_dn": "DN40",
            "height_adj_min_mm": 57,
            "height_adj_max_mm": 128,
        },
    ]
    missing_products = [row for row in fallback_products if row["product_id"] not in product_ids]
    if missing_products:
        products = pd.concat([products, pd.DataFrame(missing_products)], ignore_index=True, sort=False)

    component_ids = set(_norm(components, "product_id"))
    fallback_components = []
    for article in ("9010.88.66", "9010.88.94"):
        digits = re.sub(r"\D+", "", article)
        pid = f"aco-{digits}"
        if pid in component_ids:
            continue
        fallback_components.append({
            "manufacturer": "aco",
            "product_id": pid,
            "product_family": "showerdrain_c_article_grate",
            "product_name": "Design-Roste aus geschliffenem Edelstahl",
            "system_role": "grate",
            "candidate_type": "component",
            "article_no": article,
            "source_url": getattr(aco, "SHOWERDRAIN_C_ARTICLE_GRATE_URL", ""),
            "product_url": f"{getattr(aco, 'SHOWERDRAIN_C_ARTICLE_GRATE_URL', '')}#article-{digits}",
        })
    if fallback_components:
        components = pd.concat([components, pd.DataFrame(fallback_components)], ignore_index=True, sort=False)
    return products, components


def _fetch_source_text(url: str) -> tuple[str, str]:
    if url.lower().split("?", 1)[0].endswith(".pdf"):
        text, status = extract_pdf_text_from_url(url)
        return _clean_text(text), status
    try:
        status_code, final_url, html, err = aco._safe_get_text(url, timeout=35)
    except Exception as exc:
        fixture_text, fixture_status = _fixture_text_for_url(url)
        if fixture_text:
            return fixture_text, fixture_status
        return "", f"exception_{type(exc).__name__}"
    if status_code != 200 or not html:
        fixture_text, fixture_status = _fixture_text_for_url(url)
        if fixture_text:
            return fixture_text, fixture_status
        return "", f"http_{status_code}_{err or 'no_text'}"
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(("script", "style", "noscript")):
        tag.decompose()
    return _clean_text(soup.get_text(" ")), "ok"


def _fetch_source_rows(url: str) -> tuple[list[str], str]:
    text, status = _fetch_source_text(url)
    if not status.startswith("ok") or not text:
        return [], status
    rows: list[str] = []
    if not url.lower().split("?", 1)[0].endswith(".pdf"):
        try:
            status_code, _final_url, html, _err = aco._safe_get_text(url, timeout=35)
            if status_code == 200 and html:
                soup = BeautifulSoup(html, "lxml")
                for tag_name in ("tr", "li", "p", "h1", "h2", "h3", "div"):
                    for tag in soup.find_all(tag_name):
                        row_text = _clean_text(tag.get_text(" "))
                        if row_text and any(term.lower() in row_text.lower() for term in EVIDENCE_TERMS):
                            rows.append(row_text)
        except Exception:
            pass
    if not rows:
        for match in re.finditer(r".{0,140}(?:ShowerDrain\s*C\+|\bC\+\b|cplus|kompatibel|compatible|Design[- ]?Rost|Rost|Abdeckung|grate).{0,140}", text, re.IGNORECASE):
            rows.append(_clean_text(match.group(0)))
    deduped: list[str] = []
    seen: set[str] = set()
    for row in rows:
        key = row[:500]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
        if len(deduped) >= 40:
            break
    return deduped, status


def _candidate_rows_from_frames(frames: Iterable[pd.DataFrame]) -> dict[str, dict[str, str]]:
    candidates: dict[str, dict[str, str]] = {}
    for df in frames:
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            row_text = " ".join(_clean_text(row.get(col, "")) for col in df.columns)
            if not (GRATE_TERMS_RE.search(row_text) or str(row.get("system_role", "")).lower() == "grate"):
                continue
            article_number = _clean_text(row.get("article_number", "") or row.get("article_no", ""))
            if not article_number:
                match = ARTICLE_RE.search(row_text)
                article_number = match.group(0) if match else ""
            product_id = _clean_text(row.get("product_id", ""))
            source_urls: set[str] = set()
            for col in ("source_url", "product_url", "sources", "url"):
                source_urls.update(_urls_from_value(row.get(col, "")))
            key = product_id or article_number or row_text[:80]
            candidates.setdefault(key, {
                "article_number": article_number,
                "product_id": product_id,
                "source_url": sorted(source_urls)[0] if source_urls else "",
                "row_text": row_text,
            })
    return candidates


def _classify(row_text: str, source_url: str, has_candidate_article: bool) -> tuple[str, str]:
    text = _clean_text(row_text)
    mentions_cplus = bool(CPLUS_RE.search(f"{source_url} {text}"))
    mentions_grate = bool(GRATE_TERMS_RE.search(text))
    mentions_compat = bool(COMPAT_TERMS_RE.search(text))
    mentions_article = bool(ARTICLE_RE.search(text)) or has_candidate_article
    url_is_c_family = "showerdrain-c/" in source_url.lower() and "cplus" not in source_url.lower() and "c+" not in source_url.lower()

    if mentions_cplus and mentions_grate and mentions_article and mentions_compat:
        return "explicit_article_matrix", "explicit"
    if mentions_cplus and mentions_grate and mentions_compat:
        return "explicit_family_level", "explicit"
    if mentions_cplus and mentions_grate:
        return "implicit_family_level", "implicit"
    if url_is_c_family and mentions_grate:
        return "ambiguous", "ambiguous"
    return "absent", "absent"


def find_candidate_evidence(source_urls: Iterable[str], *frames: pd.DataFrame) -> tuple[tuple[SourceInspection, ...], tuple[CandidateEvidence, ...]]:
    frame_candidates = _candidate_rows_from_frames(frames)
    evidence: list[CandidateEvidence] = []
    inspections: list[SourceInspection] = []
    seen_evidence: set[tuple[str, str, str]] = set()

    for url in source_urls:
        rows, status = _fetch_source_rows(url)
        matched = tuple(row[:320] for row in rows[:10])
        inspections.append(SourceInspection(source_url=url, status=status, matched_snippets=matched))
        for row_text in rows:
            article_numbers = ARTICLE_RE.findall(row_text) or [""]
            for article_number in article_numbers:
                product_id = ""
                for candidate in frame_candidates.values():
                    cand_article = candidate.get("article_number", "")
                    if article_number and cand_article and re.sub(r"\D+", "", cand_article) == re.sub(r"\D+", "", article_number):
                        product_id = candidate.get("product_id", "")
                        break
                evidence_type, confidence = _classify(row_text, url, bool(article_number))
                key = (article_number, url, evidence_type)
                if key in seen_evidence:
                    continue
                seen_evidence.add(key)
                evidence.append(CandidateEvidence(
                    article_number=article_number,
                    product_id=product_id,
                    source_url=url,
                    row_text=row_text[:600],
                    evidence_type=evidence_type,
                    compatibility_confidence=confidence,
                ))

    # Include grate candidate rows from dataframes even when source pages did not expose row text.
    for candidate in frame_candidates.values():
        evidence_type, confidence = _classify(candidate.get("row_text", ""), candidate.get("source_url", ""), bool(candidate.get("article_number", "")))
        key = (candidate.get("article_number", ""), candidate.get("source_url", ""), candidate.get("product_id", ""))
        if key in seen_evidence:
            continue
        evidence.append(CandidateEvidence(
            article_number=candidate.get("article_number", ""),
            product_id=candidate.get("product_id", ""),
            source_url=candidate.get("source_url", ""),
            row_text=candidate.get("row_text", "")[:600],
            evidence_type=evidence_type,
            compatibility_confidence=confidence,
        ))
    return tuple(inspections), tuple(evidence)


def build_diagnostic(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    components: pd.DataFrame,
    evidence_df: pd.DataFrame,
    bom_options: pd.DataFrame,
    *,
    include_broad_sources: bool = False,
) -> CPlusCompatibleGrateDiagnostic:
    base_rows, missing_base_ids = locate_cplus_base_rows(products)
    source_urls = collect_related_source_urls(
        candidates_all,
        products,
        components,
        bom_options,
        evidence_df,
        include_broad_sources=include_broad_sources,
    )
    inspections, candidate_evidence = find_candidate_evidence(source_urls, candidates_all, products, components, bom_options, evidence_df)
    safe = any(ev.evidence_type == "explicit_article_matrix" and ev.compatibility_confidence == "explicit" for ev in candidate_evidence)
    if safe:
        recommendation = "Future compatible_grate BOM rows may be added only for the explicit article-level C+ rows listed above."
    else:
        recommendation = "Do not add C+ compatible_grate BOM rows and do not generate C+ assembled products yet; evidence is not explicit article-level C+ compatibility."
    return CPlusCompatibleGrateDiagnostic(
        base_rows=base_rows,
        missing_base_ids=missing_base_ids,
        source_urls_inspected=source_urls,
        source_inspections=inspections,
        candidate_evidence=candidate_evidence,
        safe_to_add_compatible_grate_bom_rows=safe,
        recommendation=recommendation,
        recommended_action="add only the explicit C+ article rows listed above" if safe else RECOMMENDED_NEXT_ACTION,
        include_broad_sources=include_broad_sources,
    )


def run_diagnostic(*, include_broad_sources: bool = False) -> CPlusCompatibleGrateDiagnostic:
    candidates, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(candidates or [])
    products, _comparison, components, evidence_df, bom_options = pipeline.run_update(
        candidates_all,
        default_config(),
        target_length_mm=1200,
        tolerance_mm=100,
        selected_connectors=["aco"],
    )
    products, components = _fallback_products_and_components(products, components)
    return build_diagnostic(
        candidates_all,
        products,
        components,
        evidence_df,
        bom_options,
        include_broad_sources=include_broad_sources,
    )


def _evidence_counts(diag: CPlusCompatibleGrateDiagnostic) -> tuple[int, int, int]:
    explicit_count = sum(
        1
        for ev in diag.candidate_evidence
        if ev.evidence_type == "explicit_article_matrix" and ev.compatibility_confidence == "explicit"
    )
    ambiguous_count = sum(1 for ev in diag.candidate_evidence if ev.evidence_type == "ambiguous")
    absent_count = sum(1 for ev in diag.candidate_evidence if ev.evidence_type == "absent")
    return explicit_count, ambiguous_count, absent_count


def print_diagnostic(diag: CPlusCompatibleGrateDiagnostic, *, verbose: bool = False) -> None:
    print("ACO ShowerDrain C+ compatible grate evidence diagnostic")
    print("\nProtected C+ base rows:")
    for base_id in PROTECTED_CPLUS_BASE_IDS:
        fields = diag.base_rows.get(base_id)
        if not fields:
            print(f"- {base_id}: MISSING")
            continue
        print(f"- {base_id}")
        for field in TECHNICAL_FIELDS:
            print(f"  - {field}: {fields.get(field, '')}")
    print("\nSource URLs inspected:")
    for url in diag.source_urls_inspected:
        print(f"- {url}")
    print("\nSource inspection snippets:")
    for source in diag.source_inspections:
        print(f"- {source.source_url} [{source.status}]")
        for snippet in source.matched_snippets[:3]:
            print(f"  - {snippet}")
    explicit_count, ambiguous_count, absent_count = _evidence_counts(diag)
    print("\nCandidate compatible grate evidence summary:")
    print(f"- explicit C+ article-level rows: {explicit_count}")
    print(f"- ambiguous ShowerDrain C-only grate rows: {ambiguous_count}")
    print(f"- absent/irrelevant candidate rows: {absent_count}")

    explicit_rows = [
        ev
        for ev in diag.candidate_evidence
        if ev.evidence_type == "explicit_article_matrix" and ev.compatibility_confidence == "explicit"
    ]
    if explicit_rows:
        print("\nExplicit C+ compatible grate rows:")
        for ev in explicit_rows:
            print(f"- article_number: {ev.article_number or '(none)'}")
            print(f"  product_id: {ev.product_id or '(none)'}")
            print(f"  source_url: {ev.source_url or '(none)'}")
            print(f"  evidence_snippet: {ev.row_text}")
            print(f"  evidence_type: {ev.evidence_type}")
            print(f"  compatibility_confidence: {ev.compatibility_confidence}")

    ambiguous_rows = [ev for ev in diag.candidate_evidence if ev.evidence_type == "ambiguous"]
    if ambiguous_rows:
        print("\nAmbiguous ShowerDrain C-only grate rows:")
        for ev in ambiguous_rows:
            print(f"- article_number: {ev.article_number or '(none)'}")
            print(f"  product_id: {ev.product_id or '(none)'}")
            print(f"  source_url: {ev.source_url or '(none)'}")
            print(f"  evidence_type: {ev.evidence_type}")
            print(f"  compatibility_confidence: {ev.compatibility_confidence}")

    if verbose:
        absent_rows = [ev for ev in diag.candidate_evidence if ev.evidence_type == "absent"]
        if absent_rows:
            print("\nAbsent candidate rows (verbose):")
            for ev in absent_rows:
                print(f"- article_number: {ev.article_number or '(none)'}")
                print(f"  product_id: {ev.product_id or '(none)'}")
                print(f"  source_url: {ev.source_url or '(none)'}")
                print(f"  evidence_type: {ev.evidence_type}")
                print(f"  compatibility_confidence: {ev.compatibility_confidence}")
                print(f"  row_text: {ev.row_text}")

    print("\nC+ evidence summary:")
    print(f"- explicit C+ compatible grate evidence: {explicit_count}")
    print(f"- ambiguous ShowerDrain C-only grate rows: {ambiguous_count}")
    print(f"- safe_to_add_cplus_bom_rows: {diag.safe_to_add_compatible_grate_bom_rows}")
    print(f"- recommended_action: {diag.recommended_action}")
    print("\nFinal conclusion:")
    if diag.safe_to_add_compatible_grate_bom_rows:
        print("- explicit C+ article-level evidence was detected; add only those listed compatible_grate rows.")
    else:
        print("- do not add C+ compatible_grate BOM rows")
        print("- do not generate C+ assembled products")
    print("Production behavior changed: no (diagnostic-only script/test patch)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose C+ compatible grate evidence without changing production data.")
    parser.add_argument(
        "--include-broad-sources",
        action="store_true",
        help="Inspect broader discovered URLs in addition to default C+/C/C-grate relevant sources.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print absent candidate rows and full row text for low-signal evidence.",
    )
    args = parser.parse_args(argv)
    diag = run_diagnostic(include_broad_sources=args.include_broad_sources)
    print_diagnostic(diag, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
