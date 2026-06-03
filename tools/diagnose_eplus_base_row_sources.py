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

KNOWN_EPLUS_SOURCE_URLS = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/design-roste-aus-elektropoliertem-edelstahl/",
)
EPLUS_FAMILY = "showerdrain_eplus"
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
    r"(?:einbauh(?:ö|oe)he|oberkante\s+estrich|height)[^\d]{0,40}(?P<min>\d{1,3})\s*[-–]\s*(?P<max>\d{1,3})\s*mm",
    re.IGNORECASE,
)
LENGTH_RE = re.compile(r"(?:\bL1\b|l(?:ä|ae)nge|length|abmessung)[^\d]{0,25}(?P<value>\d{3,4})\s*mm", re.IGNORECASE)
COMPAT_RE = re.compile(r"(?:passend\s+f(?:ü|ue)r|kompatibel(?:\s+mit)?|compatible\s+with)[^.\n|;]{0,180}", re.IGNORECASE)
EPLUS_RE = re.compile(r"(?:showerdrain[-\s]?e\+|showerdrain[-\s]?eplus|\be\+\b|eplus)", re.IGNORECASE)
C_ONLY_RE = re.compile(r"(?:showerdrain[-\s]?c(?:\b|[,\s])|aco-showerdrain-c/|/aco-showerdrain-c/)", re.IGNORECASE)
SUPPORT_ACCESSORY_RE = re.compile(
    r"(?:montagewinkel|zubeh(?:ö|oe)r|accessory|adapter|rahmen|fuss|fuß|showerstep|gef(?:ä|ae)llekeil|brandschutz)",
    re.IGNORECASE,
)
BODY_RE = re.compile(r"(?:rinnenk(?:ö|oe)rper|einbauh(?:ö|oe)he|oberkante\s+estrich|sperrwasserh(?:ö|oe)he|ablaufleistung|abflusswert|ablaufk(?:ö|oe)rper|ablaufstutzen|\bDN\s*\d{2,3}\b)", re.IGNORECASE)
GRATE_RE = re.compile(r"(?:design[-\s]?roste?|designrost|\broste?\b|\bgrate\b)", re.IGNORECASE)


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
    compatible_with: str = ""
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
    excluded_support_accessory_rows: int
    hydraulic_complete_candidates: int
    likely_assembly_model: str
    safe_to_generate_eplus_assemblies: bool
    blocking_reason: str
    recommended_next_action: str


@dataclass(frozen=True)
class EPlusDiagnostic:
    output_rows: OutputRowsSummary
    source_urls_inspected: tuple[str, ...]
    page_level_body_candidates: tuple[EPlusCandidateRow, ...]
    article_level_body_candidates: tuple[EPlusCandidateRow, ...]
    grate_candidates: tuple[EPlusCandidateRow, ...]
    excluded_support_accessory_candidates: tuple[EPlusCandidateRow, ...]
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
            "product_id",
            "product_name",
            "product_family",
            "candidate_type",
            "system_role",
            "option_type",
            "component_id",
            "parent_family",
            "option_family",
            "product_url",
            "source_url",
            "sources",
            "article_no",
        )
        if col in df.columns
    ]
    return tuple(df[wanted].fillna("").astype(str).to_dict("records")) if wanted else tuple()


def _eplus_mask(df: pd.DataFrame | None) -> pd.Series:
    df = pd.DataFrame() if df is None else df
    if df.empty:
        return pd.Series([], index=df.index, dtype=bool)
    cols = [
        col
        for col in (
            "product_id",
            "product_name",
            "product_family",
            "family",
            "candidate_type",
            "system_role",
            "option_type",
            "component_id",
            "parent_family",
            "option_family",
            "product_url",
            "source_url",
            "sources",
            "url",
        )
        if col in df.columns
    ]
    if not cols:
        return pd.Series([False] * len(df), index=df.index)
    text = df[cols].fillna("").astype(str).agg(" ".join, axis=1)
    return text.str.contains(EPLUS_RE, regex=True, na=False)


def collect_eplus_source_urls(*frames: pd.DataFrame) -> tuple[str, ...]:
    urls = {_canonical_url(url) for url in KNOWN_EPLUS_SOURCE_URLS}
    for df in frames:
        df = pd.DataFrame() if df is None else df
        if df.empty:
            continue
        columns = [col for col in ("product_url", "source_url", "sources", "url") if col in df.columns]
        if not columns:
            continue
        mask = _eplus_mask(df)
        for _, row in df[mask].iterrows():
            for col in columns:
                urls.update(_urls_from_value(row.get(col, "")))
    return tuple(sorted(url for url in urls if url))


def summarize_current_eplus_rows(
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    candidates_all: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
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


def _has_explicit_eplus_grate_evidence(source_url: str, title: str, row_text: str, evidence_type: str) -> bool:
    """Return True only for explicit design-grate/rost source evidence.

    A ShowerDrain E+ family URL or generic E+ body/hydraulic text is intentionally
    insufficient. Full page-text fallbacks can contain navigation labels such as
    Design-Roste, so those fallbacks require grate evidence in the URL or title.
    Article/table rows can qualify from row text because they are focused rows.
    """
    if GRATE_RE.search(f"{source_url} {title}"):
        return True
    return evidence_type != "source_page_text" and bool(GRATE_RE.search(row_text))


def _is_cross_family_c_only(source_url: str, row_text: str) -> bool:
    text = f"{source_url} {row_text}"
    return bool(C_ONLY_RE.search(text) and not EPLUS_RE.search(text))


def _classify_candidate_type(source_url: str, title: str, row_text: str, evidence_type: str) -> tuple[str, str, str, str]:
    page_text = f"{source_url} {title}"
    text = f"{page_text} {row_text}"
    lower_text = text.lower()

    if _is_cross_family_c_only(source_url, row_text):
        return "excluded_other", "", "", "low"
    if _has_explicit_eplus_grate_evidence(source_url, title, row_text, evidence_type):
        return "eplus_grate_component", "component", "grate", "high" if GRATE_RE.search(f"{source_url} {title}") else "medium"
    if evidence_type == "source_page_text" and BODY_RE.search(text):
        return "eplus_page_level_body_row", "component", "drain_unit", "medium"
    if SUPPORT_ACCESSORY_RE.search(text):
        return "eplus_excluded_support_accessory", "component", "accessory", "medium"
    if BODY_RE.search(text):
        if ARTICLE_RE.search(row_text):
            return "eplus_article_level_body_candidate", "component", "drain_unit", "medium"
        return "eplus_page_level_body_row", "component", "drain_unit", "medium"
    if EPLUS_RE.search(lower_text):
        return "eplus_excluded_support_accessory", "component", "accessory", "low"
    return "excluded_other", "", "", "low"


def _first_match(regex: re.Pattern[str], text: str, group: str = "value") -> str:
    match = regex.search(text)
    if not match:
        return ""
    return _clean_text(match.group(group)).replace(",", ".")


def _extract_length(row_text: str) -> str:
    match = LENGTH_RE.search(row_text)
    if match:
        return match.group("value")
    l1_match = re.search(r"\bL1\b[^\d]{0,15}(?P<value>\d{3,4})\b", row_text, re.IGNORECASE)
    return l1_match.group("value") if l1_match else ""


def _extract_compat(row_text: str) -> str:
    matches = [_clean_text(match.group(0)) for match in COMPAT_RE.finditer(row_text)]
    if not matches:
        return ""
    with_family = [match for match in matches if EPLUS_RE.search(match)]
    return max(with_family or matches, key=len)


def _extract_height(row_text: str) -> tuple[str, str]:
    match = HEIGHT_RANGE_RE.search(row_text)
    if not match:
        return "", ""
    return match.group("min"), match.group("max")


def _candidate_from_text(source_url: str, title: str, row_text: str, evidence_type: str) -> EPlusCandidateRow | None:
    cleaned = _clean_text(row_text)
    if not cleaned or not (EPLUS_RE.search(f"{source_url} {title} {cleaned}") or ARTICLE_RE.search(cleaned)):
        return None
    candidate_type, base_type, system_role, confidence = _classify_candidate_type(source_url, title, cleaned, evidence_type)
    article_match = ARTICLE_RE.search(cleaned)
    article_number = article_match.group(0) if article_match else ""
    article_digits = _digits_only(article_number)
    proposed_product_id = ""
    if article_digits and candidate_type not in {"excluded_other", "eplus_excluded_support_accessory"}:
        proposed_product_id = aco._stable_aco_id(source_url, EPLUS_FAMILY, system_role or "component", title, article_digits)
    height_min, height_max = _extract_height(cleaned)
    return EPlusCandidateRow(
        article_number=article_number,
        proposed_product_id=proposed_product_id,
        candidate_type=candidate_type,
        product_family=EPLUS_FAMILY if candidate_type not in {"excluded_other", "eplus_excluded_support_accessory"} else "",
        system_role=system_role,
        source_url=source_url,
        row_text=cleaned[:500],
        length_mm=_extract_length(cleaned),
        flow_rate_lps=_first_match(FLOW_RE, cleaned),
        water_seal_mm=_first_match(SEAL_RE, cleaned),
        outlet_dn=f"DN{_first_match(DN_RE, cleaned)}" if _first_match(DN_RE, cleaned) else "",
        height_adj_min_mm=height_min,
        height_adj_max_mm=height_max,
        compatible_with=_extract_compat(cleaned),
        evidence_type=evidence_type,
        confidence=confidence,
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
            if ARTICLE_RE.search(row_text) or EPLUS_RE.search(row_text) or GRATE_RE.search(row_text):
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
    if status != 200 or not html:
        row = EPlusCandidateRow(
            article_number="",
            proposed_product_id="",
            candidate_type="excluded_other",
            product_family="",
            system_role="",
            source_url=final_url,
            row_text=f"fetch failed: status={status}; error={error}",
            evidence_type="fetch_error",
            confidence="low",
        )
        return final_url, (row,)

    soup = BeautifulSoup(html, "lxml")
    title_tag = soup.select_one("h1") or soup.select_one("title")
    title = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else final_url)
    main = soup.select_one("main") or soup
    flat_text = _clean_text(main.get_text(" ", strip=True))

    raw_rows = _table_row_texts(html)
    evidence_type = "source_article_table_row"
    if not raw_rows:
        raw_rows = _article_snippets(flat_text)
        evidence_type = "source_article_snippet"
    if not raw_rows and EPLUS_RE.search(f"{final_url} {title} {flat_text}"):
        raw_rows = [flat_text[:700]]
        evidence_type = "source_page_text"

    candidates: list[EPlusCandidateRow] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for raw_row in raw_rows:
        candidate = _candidate_from_text(final_url, title, raw_row, evidence_type)
        if candidate is None:
            continue
        key = (candidate.article_number, candidate.candidate_type, candidate.row_text)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        candidates.append(candidate)
    return final_url, tuple(candidates)


def parse_eplus_sources(source_urls: Iterable[str]) -> tuple[tuple[str, ...], tuple[EPlusCandidateRow, ...]]:
    inspected: list[str] = []
    candidates: list[EPlusCandidateRow] = []
    for url in source_urls:
        final_url, rows = parse_eplus_source_url(url)
        inspected.append(final_url)
        candidates.extend(rows)
    return tuple(dict.fromkeys(inspected)), tuple(candidates)


def _has_complete_hydraulics(candidate: EPlusCandidateRow) -> bool:
    return all(_clean_text(getattr(candidate, field)) for field in TECHNICAL_FIELDS)


def build_readiness_summary(output_rows: OutputRowsSummary, candidates: Iterable[EPlusCandidateRow]) -> EPlusReadinessSummary:
    rows = tuple(candidates)
    page_bodies = tuple(row for row in rows if row.candidate_type == "eplus_page_level_body_row")
    article_bodies = tuple(row for row in rows if row.candidate_type == "eplus_article_level_body_candidate")
    grates = tuple(row for row in rows if row.candidate_type == "eplus_grate_component")
    support = tuple(row for row in rows if row.candidate_type == "eplus_excluded_support_accessory")
    complete = tuple(row for row in article_bodies if _has_complete_hydraulics(row))
    incomplete_article_bodies = tuple(row for row in article_bodies if not _has_complete_hydraulics(row))

    model = "base_x_grate" if page_bodies and grates else "unknown"
    blocking = (
        "E+ diagnostic found page/family-level body evidence and explicit grate evidence, but no article-level body rows with complete hydraulic data."
        if model == "base_x_grate"
        else "E+ source-backed body/grate relationship and complete hydraulic assembly evidence are incomplete."
    )
    next_action = "keep E+ assembly generation disabled until explicit article-level body and compatibility evidence is found."

    return EPlusReadinessSummary(
        eplus_products_present=len(output_rows.products),
        eplus_components_present=len(output_rows.components),
        eplus_page_level_body_rows=len(page_bodies),
        eplus_article_level_body_candidates=len(article_bodies),
        eplus_article_level_body_incomplete=len(incomplete_article_bodies),
        eplus_grate_candidates=len(grates),
        excluded_support_accessory_rows=len(support),
        hydraulic_complete_candidates=len(complete),
        likely_assembly_model=model,
        safe_to_generate_eplus_assemblies=False,
        blocking_reason=blocking,
        recommended_next_action=next_action,
    )


def build_diagnostic(
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    candidates_all: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
    final_assemblies: pd.DataFrame | None = None,
    final_set_details: pd.DataFrame | None = None,
) -> EPlusDiagnostic:
    output_rows = summarize_current_eplus_rows(
        products,
        comparison,
        candidates_all,
        components,
        bom_options,
        final_assemblies,
        final_set_details,
    )
    source_urls = collect_eplus_source_urls(products, comparison, candidates_all, components, bom_options)
    inspected, source_candidates = parse_eplus_sources(source_urls)
    readiness = build_readiness_summary(output_rows, source_candidates)
    return EPlusDiagnostic(
        output_rows=output_rows,
        source_urls_inspected=inspected,
        page_level_body_candidates=tuple(row for row in source_candidates if row.candidate_type == "eplus_page_level_body_row"),
        article_level_body_candidates=tuple(row for row in source_candidates if row.candidate_type == "eplus_article_level_body_candidate"),
        grate_candidates=tuple(row for row in source_candidates if row.candidate_type == "eplus_grate_component"),
        excluded_support_accessory_candidates=tuple(row for row in source_candidates if row.candidate_type == "eplus_excluded_support_accessory"),
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
            f"compatible_with={row.compatible_with or '(missing)'}; evidence_type={row.evidence_type}; confidence={row.confidence}; "
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

    _print_candidates("E+ page-level body rows", diag.page_level_body_candidates)
    _print_candidates("Article-level body candidates", diag.article_level_body_candidates)
    _print_candidates("Grates", diag.grate_candidates)
    _print_candidates("Excluded support/accessory rows", diag.excluded_support_accessory_candidates)
    if diag.excluded_other_candidates:
        _print_candidates("Excluded/other", diag.excluded_other_candidates)

    r = diag.readiness
    print("\nReadiness summary:")
    print(f"- E+ products present: {r.eplus_products_present}")
    print(f"- E+ components present: {r.eplus_components_present}")
    print(f"- E+ page-level body rows: {r.eplus_page_level_body_rows}")
    print(f"- article-level body candidates: {r.eplus_article_level_body_candidates}")
    print(f"- article-level body incomplete: {r.eplus_article_level_body_incomplete}")
    print(f"- grates: {r.eplus_grate_candidates}")
    print(f"- excluded support/accessory rows: {r.excluded_support_accessory_rows}")
    print(f"- hydraulic_complete_candidates: {r.hydraulic_complete_candidates}")
    print(f"- likely_assembly_model: {r.likely_assembly_model}")
    print(f"- safe_to_generate_eplus_assemblies: {r.safe_to_generate_eplus_assemblies}")
    print(f"- blocking_reason: {r.blocking_reason}")
    print(f"- recommended_next_action: {r.recommended_next_action}")
    print(f"- production_behavior_changed: {diag.production_behavior_changed}")


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
