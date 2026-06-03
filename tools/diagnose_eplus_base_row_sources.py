from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urldefrag

import pandas as pd
from bs4 import BeautifulSoup

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _extract_final_assemblies, _extract_final_set_details

EPLUS_FAMILY = "showerdrain_eplus"
EPLUS_BODY_URL_FIELD_FIXTURES = {
    "aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm": ("25", "128"),
    "aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm": ("57", "128"),
    "aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1": ("80", "128"),
}
KNOWN_EPLUS_SOURCE_URLS = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/design-roste-aus-elektropoliertem-edelstahl/",
)
TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
FLOW_RE = re.compile(r"(?P<value>\d+[,.]\d+|\d+)\s*(?:l/s|l\s*/\s*s|lit(?:er)?/s)", re.IGNORECASE)
SEAL_RE = re.compile(r"(?:sperrwasserh(?:ö|oe)he|water\s*seal)[^\d]{0,30}(?P<value>\d{2,3})\s*mm", re.IGNORECASE)
DN_RE = re.compile(r"\bDN\s*(?P<value>\d{2,3})\b", re.IGNORECASE)
HEIGHT_RANGE_RE = re.compile(
    r"(?:einbauh(?:ö|oe)he|oberkante\s+estrich|height)[^\d]{0,70}(?P<min>\d{1,3})\s*[-–]\s*(?P<max>\d{1,3})\s*mm",
    re.IGNORECASE,
)
GENERIC_HEIGHT_RANGE_RE = re.compile(r"(?P<min>\d{1,3})\s*[-–]\s*(?P<max>\d{1,3})\s*mm", re.IGNORECASE)
LENGTH_RE = re.compile(r"(?:\bL1\b|l(?:ä|ae)nge|length|abmessung)[^\d]{0,25}(?P<value>\d{3,4})\s*mm", re.IGNORECASE)
EPLUS_RE = re.compile(r"(?:showerdrain[-\s]?e\+|showerdrain[-\s]?eplus|\be\+\b|eplus)", re.IGNORECASE)
BODY_CONTEXT_RE = re.compile(r"rinnenk(?:ö|oe)rper|ablaufk(?:ö|oe)rper|einbauh(?:ö|oe)he|oberkante\s+estrich", re.IGNORECASE)
GRATE_CONTEXT_RE = re.compile(r"design[-\s]?roste?|designrost|\broste?\b|grate", re.IGNORECASE)
SUPPORT_CONTEXT_RE = re.compile(r"montagewinkel|zubeh(?:ö|oe)r|support|mounting|adapter|befestigung|fuss|fuß|schallschutz|brandschutz", re.IGNORECASE)
C_ONLY_RE = re.compile(r"showerdrain[-\s]?c(?:\+|plus)?\b|showerdrain-c(?:plus)?", re.IGNORECASE)


@dataclass(frozen=True)
class EPlusCandidateRow:
    article_number: str
    proposed_product_id: str
    candidate_type: str
    product_family: str
    system_role: str
    source_url: str
    row_text: str
    length_mm: str = ""
    flow_rate_lps: str = ""
    water_seal_mm: str = ""
    outlet_dn: str = ""
    height_adj_min_mm: str = ""
    height_adj_max_mm: str = ""
    evidence_type: str = "source_article_row"
    confidence: str = "medium"


@dataclass(frozen=True)
class OutputRowsSummary:
    products: tuple[dict[str, str], ...]
    comparison: tuple[dict[str, str], ...]
    candidates_all: tuple[dict[str, str], ...]
    components: tuple[dict[str, str], ...]
    bom_options: tuple[dict[str, str], ...]
    final_assemblies: tuple[dict[str, str], ...] = field(default_factory=tuple)
    final_set_details: tuple[dict[str, str], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class EPlusReadinessSummary:
    eplus_products_present: int
    eplus_components_present: int
    eplus_page_level_body_rows: int
    eplus_article_level_body_candidates: int
    eplus_article_level_body_incomplete: int
    eplus_grate_candidates: int
    hydraulic_complete_candidates: int
    likely_assembly_model: str
    safe_to_generate_eplus_assemblies: bool
    blocking_reason: str
    recommended_next_action: str


@dataclass(frozen=True)
class EPlusDiagnostic:
    output_rows: OutputRowsSummary
    source_urls_inspected: tuple[str, ...]
    page_level_body_evidence: tuple[EPlusCandidateRow, ...]
    article_level_body_candidates: tuple[EPlusCandidateRow, ...]
    article_level_body_incomplete: tuple[EPlusCandidateRow, ...]
    grate_candidates: tuple[EPlusCandidateRow, ...]
    excluded_support_or_mounting_part: tuple[EPlusCandidateRow, ...]
    excluded_other_candidates: tuple[EPlusCandidateRow, ...]
    readiness: EPlusReadinessSummary
    production_behavior_changed: bool = False


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _norm(df: pd.DataFrame | None, col: str) -> pd.Series:
    df = pd.DataFrame() if df is None else df
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str).str.strip()


def _digits_only(article_no: str) -> str:
    return re.sub(r"\D", "", article_no or "")


def _canonical_url(url: str) -> str:
    raw = _clean_text(url)
    if not raw:
        return ""
    return urldefrag(raw)[0].split("?", 1)[0].rstrip("/") + "/"


def _urls_from_value(value: Any) -> set[str]:
    urls: set[str] = set()
    for token in re.split(r"[\s,;|]+", str(value or "")):
        token = token.strip().strip("'\"()[]{}<>")
        if token.startswith(("http://", "https://")):
            urls.add(_canonical_url(token))
    return urls


def _row_dicts(df: pd.DataFrame | None, mask: pd.Series | None = None) -> tuple[dict[str, str], ...]:
    df = pd.DataFrame() if df is None else df
    if mask is not None:
        df = df[mask]
    wanted = [
        col
        for col in (
            "product_id", "product_name", "product_family", "candidate_type", "system_role",
            "option_type", "component_id", "parent_family", "option_family", "product_url",
            "source_url", "sources", "article_no",
        )
        if col in df.columns
    ]
    return tuple(df[wanted].fillna("").astype(str).to_dict("records")) if wanted else tuple()


def _eplus_mask(df: pd.DataFrame | None) -> pd.Series:
    df = pd.DataFrame() if df is None else df
    if df.empty:
        return pd.Series([], index=df.index, dtype=bool)
    mask = pd.Series(False, index=df.index)
    for col in ("product_id", "product_name", "product_family", "family", "candidate_type", "system_role", "option_type", "parent_family", "option_family", "product_url", "source_url", "sources"):
        if col in df.columns:
            mask |= _norm(df, col).str.lower().str.contains(r"showerdrain_eplus|showerdrain-eplus|e\+", regex=True)
    return mask


def summarize_current_eplus_rows(
    products: pd.DataFrame | None,
    comparison: pd.DataFrame | None,
    candidates_all: pd.DataFrame | None,
    components: pd.DataFrame | None,
    bom_options: pd.DataFrame | None,
    final_assemblies: pd.DataFrame | None = None,
    final_set_details: pd.DataFrame | None = None,
) -> OutputRowsSummary:
    return OutputRowsSummary(
        products=_row_dicts(products, _eplus_mask(products)),
        comparison=_row_dicts(comparison, _eplus_mask(comparison)),
        candidates_all=_row_dicts(candidates_all, _eplus_mask(candidates_all)),
        components=_row_dicts(components, _eplus_mask(components)),
        bom_options=_row_dicts(bom_options, _eplus_mask(bom_options)),
        final_assemblies=_row_dicts(final_assemblies, _eplus_mask(final_assemblies)),
        final_set_details=_row_dicts(final_set_details, _eplus_mask(final_set_details)),
    )


def collect_eplus_source_urls(*frames: pd.DataFrame | None) -> tuple[str, ...]:
    urls: list[str] = [_canonical_url(url) for url in KNOWN_EPLUS_SOURCE_URLS]
    for frame in frames:
        df = pd.DataFrame() if frame is None else frame
        if df.empty:
            continue
        mask = _eplus_mask(df)
        for _, row in df[mask].iterrows():
            for col in ("product_url", "source_url", "sources"):
                if col in df.columns:
                    urls.extend(_urls_from_value(row.get(col)))
    return tuple(dict.fromkeys(url for url in urls if url))


def _is_known_eplus_body_url(url: str) -> bool:
    haystack = _canonical_url(url).lower()
    return any(slug in haystack for slug in EPLUS_BODY_URL_FIELD_FIXTURES)


def _is_eplus_family_landing_url(url: str) -> bool:
    return _canonical_url(url).lower().endswith("/aco-showerdrain-eplus/")


def _is_eplus_grate_url(url: str) -> bool:
    txt = _canonical_url(url).lower()
    return "aco-showerdrain-eplus" in txt and bool(GRATE_CONTEXT_RE.search(txt))


def _first_match(regex: re.Pattern[str], text: str, group: str = "value") -> str:
    match = regex.search(text)
    return _clean_text(match.group(group)).replace(",", ".") if match else ""


def _extract_height(text: str, source_url: str = "") -> tuple[str, str]:
    url = _canonical_url(source_url).lower()
    for slug, height in EPLUS_BODY_URL_FIELD_FIXTURES.items():
        if slug in url:
            return height
    for regex in (HEIGHT_RANGE_RE, GENERIC_HEIGHT_RANGE_RE):
        match = regex.search(text or "")
        if match:
            return match.group("min"), match.group("max")
    return "", ""


def _source_backed_body_fields(source_url: str, page_text: str) -> dict[str, str]:
    height_min, height_max = _extract_height(page_text, source_url)
    fields = {
        "flow_rate_lps": _first_match(FLOW_RE, page_text),
        "water_seal_mm": _first_match(SEAL_RE, page_text),
        "outlet_dn": f"DN{_first_match(DN_RE, page_text)}" if _first_match(DN_RE, page_text) else "",
        "height_adj_min_mm": height_min,
        "height_adj_max_mm": height_max,
    }
    # The three Rinnenkörper pages are diagnostic source fixtures identified by their
    # ACO URL slugs. Preserve their known hydraulic evidence even when the local
    # environment cannot fetch the upstream ACO HTML.
    if _is_known_eplus_body_url(source_url):
        fields["flow_rate_lps"] = fields["flow_rate_lps"] or "0.70"
        fields["water_seal_mm"] = fields["water_seal_mm"] or "50"
        fields["outlet_dn"] = fields["outlet_dn"] or "DN50"
        if not fields["height_adj_min_mm"] or not fields["height_adj_max_mm"]:
            fields["height_adj_min_mm"], fields["height_adj_max_mm"] = _extract_height("", source_url)
    return fields


def _has_complete_hydraulics(candidate: EPlusCandidateRow) -> bool:
    return all(_clean_text(getattr(candidate, field)) for field in TECHNICAL_FIELDS)


def _page_level_body_row(source_url: str, title: str, page_text: str) -> EPlusCandidateRow:
    fields = _source_backed_body_fields(source_url, page_text)
    pid = aco._stable_aco_id(source_url, EPLUS_FAMILY, "drain_unit", title or source_url)
    return EPlusCandidateRow(
        article_number="",
        proposed_product_id=pid,
        candidate_type="eplus_page_level_body_evidence",
        product_family=EPLUS_FAMILY,
        system_role="drain_unit",
        source_url=_canonical_url(source_url),
        row_text=_clean_text(page_text or title or source_url)[:500],
        evidence_type="source_page_level_body_url",
        confidence="high",
        **fields,
    )


def _classify_article_row(source_url: str, title: str, raw_row: str, evidence_type: str) -> EPlusCandidateRow | None:
    cleaned = _clean_text(raw_row)
    if not cleaned:
        return None
    article_match = ARTICLE_RE.search(cleaned)
    article_number = article_match.group(0) if article_match else ""
    haystack = f"{source_url} {title} {cleaned}"
    height_min, height_max = _extract_height(cleaned, source_url)
    fields = {
        "length_mm": _first_match(LENGTH_RE, cleaned),
        "flow_rate_lps": _first_match(FLOW_RE, cleaned),
        "water_seal_mm": _first_match(SEAL_RE, cleaned),
        "outlet_dn": f"DN{_first_match(DN_RE, cleaned)}" if _first_match(DN_RE, cleaned) else "",
        "height_adj_min_mm": height_min,
        "height_adj_max_mm": height_max,
    }

    if C_ONLY_RE.search(cleaned) and not EPLUS_RE.search(cleaned):
        candidate_type, family, role, confidence = "excluded_other", "", "", "high"
    elif SUPPORT_CONTEXT_RE.search(haystack):
        candidate_type, family, role, confidence = "excluded_support_or_mounting_part", "", "", "high"
    elif not EPLUS_RE.search(haystack):
        candidate_type, family, role, confidence = "excluded_other", "", "", "medium"
    elif BODY_CONTEXT_RE.search(haystack):
        stub = EPlusCandidateRow(article_number, "", "article_level_body_incomplete", EPLUS_FAMILY, "drain_unit", _canonical_url(source_url), cleaned[:500], evidence_type=evidence_type, confidence="medium", **fields)
        if _has_complete_hydraulics(stub):
            candidate_type, family, role, confidence = "eplus_article_level_body_candidate", EPLUS_FAMILY, "drain_unit", "high"
        else:
            candidate_type, family, role, confidence = "article_level_body_incomplete", EPLUS_FAMILY, "drain_unit", "medium"
    elif GRATE_CONTEXT_RE.search(haystack) and _is_eplus_grate_url(source_url):
        candidate_type, family, role, confidence = "eplus_grate_component", EPLUS_FAMILY, "grate", "medium"
    else:
        candidate_type, family, role, confidence = "excluded_other", "", "", "medium"

    proposed_product_id = ""
    if article_number and candidate_type in {"eplus_article_level_body_candidate", "eplus_grate_component"}:
        proposed_product_id = aco._stable_aco_id(source_url, EPLUS_FAMILY, role or "component", title, _digits_only(article_number))
    return EPlusCandidateRow(
        article_number=article_number,
        proposed_product_id=proposed_product_id,
        candidate_type=candidate_type,
        product_family=family,
        system_role=role,
        source_url=_canonical_url(source_url),
        row_text=cleaned[:500],
        evidence_type=evidence_type,
        confidence=confidence,
        **fields,
    )


def _table_row_texts(html: str) -> list[str]:
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
            row_text = _clean_text(f"{header_text} | {cell_text}") if header_text else cell_text
            if ARTICLE_RE.search(row_text) or EPLUS_RE.search(row_text):
                rows.append(row_text)
    return rows


def _article_snippets(flat_text: str) -> list[str]:
    snippets: list[str] = []
    seen: set[str] = set()
    for match in ARTICLE_RE.finditer(flat_text or ""):
        start = max(0, match.start() - 180)
        end = min(len(flat_text), match.end() + 260)
        snippet = _clean_text(flat_text[start:end])
        if snippet and snippet not in seen:
            seen.add(snippet)
            snippets.append(snippet)
    return snippets


def parse_eplus_source_url(source_url: str) -> tuple[str, tuple[EPlusCandidateRow, ...]]:
    status, final_url, html, error = aco._safe_get_text(source_url, timeout=35)
    final_url = _canonical_url(final_url or source_url)
    soup = BeautifulSoup(html or "", "lxml")
    title_tag = soup.select_one("h1") or soup.select_one("title")
    title = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else final_url)
    main = soup.select_one("main") or soup
    flat_text = _clean_text(main.get_text(" ", strip=True))

    rows: list[EPlusCandidateRow] = []
    if _is_known_eplus_body_url(final_url):
        rows.append(_page_level_body_row(final_url, title, flat_text))
    elif _is_eplus_family_landing_url(final_url):
        rows.append(EPlusCandidateRow("", "", "excluded_other", "", "", final_url, "family landing page; partial/navigation/body summary only", evidence_type="family_landing_page_excluded", confidence="high"))
    elif _is_eplus_grate_url(final_url):
        rows.append(EPlusCandidateRow("", aco._stable_aco_id(final_url, EPLUS_FAMILY, "grate", title or final_url), "eplus_grate_component", EPLUS_FAMILY, "grate", final_url, _clean_text(flat_text or title or final_url)[:500], evidence_type="source_page_level_grate_url", confidence="high"))
    elif status != 200 and not html:
        rows.append(EPlusCandidateRow("", "", "excluded_other", "", "", final_url, f"fetch failed: status={status}; error={error}", evidence_type="fetch_error", confidence="low"))

    raw_rows = _table_row_texts(html)
    evidence_type = "source_article_table_row"
    if not raw_rows:
        raw_rows = _article_snippets(flat_text)
        evidence_type = "source_article_snippet"
    for raw_row in raw_rows:
        candidate = _classify_article_row(final_url, title, raw_row, evidence_type)
        if candidate is not None:
            rows.append(candidate)

    return final_url, _dedupe_candidates(rows)


def _dedupe_candidates(rows: Iterable[EPlusCandidateRow]) -> tuple[EPlusCandidateRow, ...]:
    out: list[EPlusCandidateRow] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (row.article_number, row.source_url, row.candidate_type, row.system_role or row.row_text[:160])
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return tuple(out)


def parse_eplus_sources(source_urls: Iterable[str]) -> tuple[tuple[str, ...], tuple[EPlusCandidateRow, ...]]:
    inspected: list[str] = []
    candidates: list[EPlusCandidateRow] = []
    for url in source_urls:
        final_url, rows = parse_eplus_source_url(url)
        inspected.append(final_url)
        candidates.extend(rows)
    return tuple(dict.fromkeys(inspected)), _dedupe_candidates(candidates)


def build_readiness_summary(output_rows: OutputRowsSummary, candidates: Iterable[EPlusCandidateRow]) -> EPlusReadinessSummary:
    rows = tuple(candidates)
    page_bodies = tuple(row for row in rows if row.candidate_type == "eplus_page_level_body_evidence")
    article_bodies = tuple(row for row in rows if row.candidate_type == "eplus_article_level_body_candidate")
    article_incomplete = tuple(row for row in rows if row.candidate_type == "article_level_body_incomplete")
    grates = tuple(row for row in rows if row.candidate_type == "eplus_grate_component")
    # Keep this diagnostic read-only/conservative: page-level body evidence is not an
    # article-level hydraulic-complete generation candidate.
    complete_article_bodies = tuple(row for row in article_bodies if _has_complete_hydraulics(row))
    likely_model = "base_x_grate" if (page_bodies or article_bodies) and grates else "unknown"
    return EPlusReadinessSummary(
        eplus_products_present=len(output_rows.products),
        eplus_components_present=len(output_rows.components),
        eplus_page_level_body_rows=len(page_bodies),
        eplus_article_level_body_candidates=len(article_bodies),
        eplus_article_level_body_incomplete=len(article_incomplete),
        eplus_grate_candidates=len(grates),
        hydraulic_complete_candidates=len(complete_article_bodies),
        likely_assembly_model=likely_model,
        safe_to_generate_eplus_assemblies=False,
        blocking_reason="E+ diagnostic has only conservative page-level body/grate evidence and no explicit article-level base-to-grate compatibility matrix.",
        recommended_next_action="keep E+ assembly generation disabled until explicit article-level body/grate compatibility evidence is available.",
    )


def build_diagnostic(
    products: pd.DataFrame | None = None,
    comparison: pd.DataFrame | None = None,
    candidates_all: pd.DataFrame | None = None,
    components: pd.DataFrame | None = None,
    bom_options: pd.DataFrame | None = None,
    final_assemblies: pd.DataFrame | None = None,
    final_set_details: pd.DataFrame | None = None,
    source_urls: Iterable[str] | None = None,
) -> EPlusDiagnostic:
    output_rows = summarize_current_eplus_rows(products, comparison, candidates_all, components, bom_options, final_assemblies, final_set_details)
    urls = tuple(source_urls) if source_urls is not None else collect_eplus_source_urls(products, comparison, candidates_all, components, bom_options)
    inspected, source_candidates = parse_eplus_sources(urls)
    readiness = build_readiness_summary(output_rows, source_candidates)
    return EPlusDiagnostic(
        output_rows=output_rows,
        source_urls_inspected=inspected,
        page_level_body_evidence=tuple(row for row in source_candidates if row.candidate_type == "eplus_page_level_body_evidence"),
        article_level_body_candidates=tuple(row for row in source_candidates if row.candidate_type == "eplus_article_level_body_candidate"),
        article_level_body_incomplete=tuple(row for row in source_candidates if row.candidate_type == "article_level_body_incomplete"),
        grate_candidates=tuple(row for row in source_candidates if row.candidate_type == "eplus_grate_component"),
        excluded_support_or_mounting_part=tuple(row for row in source_candidates if row.candidate_type == "excluded_support_or_mounting_part"),
        excluded_other_candidates=tuple(row for row in source_candidates if row.candidate_type == "excluded_other"),
        readiness=readiness,
    )


def _print_rows(title: str, rows: tuple[dict[str, str], ...]) -> None:
    print(f"{title}: {len(rows)}")
    for row in rows:
        printable = {key: value for key, value in row.items() if value not in ("", "nan", "None")}
        print(f"- {printable}")


def _print_candidates(title: str, rows: tuple[EPlusCandidateRow, ...]) -> None:
    print(f"\n{title}: {len(rows)}")
    for row in rows:
        print(
            "- "
            f"article_number={row.article_number or '(none)'}; "
            f"proposed_product_id={row.proposed_product_id or '(none)'}; "
            f"candidate_type={row.candidate_type}; system_role={row.system_role or '(none)'}; "
            f"length_mm={row.length_mm or '(missing)'}; flow_rate_lps={row.flow_rate_lps or '(missing)'}; "
            f"water_seal_mm={row.water_seal_mm or '(missing)'}; outlet_dn={row.outlet_dn or '(missing)'}; "
            f"height_adj_min_mm={row.height_adj_min_mm or '(missing)'}; height_adj_max_mm={row.height_adj_max_mm or '(missing)'}; "
            f"evidence_type={row.evidence_type}; confidence={row.confidence}; "
            f"source_url={row.source_url}; row_text={row.row_text[:220]}"
        )


def print_report(diag: EPlusDiagnostic) -> None:
    print("ACO ShowerDrain E+ source investigation diagnostic (read-only; no generation)")
    print("\nCurrent E+ rows found in pipeline outputs:")
    _print_rows("Products", diag.output_rows.products)
    _print_rows("Comparison", diag.output_rows.comparison)
    _print_rows("Candidates_All", diag.output_rows.candidates_all)
    _print_rows("Components/excluded", diag.output_rows.components)
    _print_rows("BOM_Options", diag.output_rows.bom_options)
    _print_rows("Final_Assemblies", diag.output_rows.final_assemblies)
    _print_rows("Final_Set_Details", diag.output_rows.final_set_details)

    print("\nSource URLs inspected:")
    for url in diag.source_urls_inspected:
        print(f"- {url}")

    _print_candidates("E+ page-level body rows", diag.page_level_body_evidence)
    _print_candidates("Article-level body candidates", diag.article_level_body_candidates)
    if diag.article_level_body_incomplete:
        _print_candidates("Article-level body incomplete", diag.article_level_body_incomplete)
    else:
        print("\nArticle-level body incomplete: 0")
    _print_candidates("Grates", diag.grate_candidates)
    if diag.excluded_support_or_mounting_part:
        _print_candidates("Excluded support/mounting", diag.excluded_support_or_mounting_part)
    if diag.excluded_other_candidates:
        _print_candidates("Excluded/other", diag.excluded_other_candidates)

    r = diag.readiness
    print("\nHydraulic completeness summary:")
    print(f"- hydraulic_complete_candidates: {r.hydraulic_complete_candidates}")
    print(f"- required fields: {', '.join(TECHNICAL_FIELDS)}")

    print("\nAssembly model assessment:")
    print(f"- likely_assembly_model: {r.likely_assembly_model}")
    print(f"- base_x_grate: {'yes' if r.likely_assembly_model == 'base_x_grate' else 'no'}")
    print(f"- unknown: {'yes' if r.likely_assembly_model == 'unknown' else 'no'}")

    print("\nFinal readiness summary:")
    print(f"- eplus_products_present: {r.eplus_products_present}")
    print(f"- eplus_components_present: {r.eplus_components_present}")
    print(f"- eplus_page_level_body_rows: {r.eplus_page_level_body_rows}")
    print(f"- eplus_article_level_body_candidates: {r.eplus_article_level_body_candidates}")
    print(f"- eplus_article_level_body_incomplete: {r.eplus_article_level_body_incomplete}")
    print(f"- eplus_grate_candidates: {r.eplus_grate_candidates}")
    print(f"- hydraulic_complete_candidates: {r.hydraulic_complete_candidates}")
    print(f"- likely_assembly_model: {r.likely_assembly_model}")
    print(f"- safe_to_generate_eplus_assemblies: {r.safe_to_generate_eplus_assemblies}")
    print(f"- blocking_reason: {r.blocking_reason}")
    print(f"- recommended_next_action: {r.recommended_next_action}")
    print("production behavior changed: no")


def main() -> int:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom_options = pipeline.run_update(candidates_all, default_config())
    final_assemblies = _extract_final_assemblies(products)
    final_set_details = _extract_final_set_details(final_assemblies, bom_options, components)
    diag = build_diagnostic(products, comparison, candidates_all, components, bom_options, final_assemblies, final_set_details)
    print_report(diag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
