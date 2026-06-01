from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

from src import pipeline
from src.config import default_config
from src.connectors import aco

BASE_ID = "aco-easyflow-komplettablaeufe-aco-easyflow-dn-50"
ASSEMBLED_IDS = (
    "aco-assembled-easyflow-aco-easyflow-komplettablaeufe-aco-easyflow-dn-50__aco-easyflow-aufsatzstuecke-fuer-designroste",
    "aco-assembled-easyflow-aco-easyflow-komplettablaeufe-aco-easyflow-dn-50__aco-easyflow-grate-design-roste-design-roste",
)
BASE_FACTS = {
    "water_seal_mm": 50,
    "outlet_dn": "DN50",
    "product_family": "easyflow",
    "product_id": BASE_ID,
}

ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
DN_RE = re.compile(r"\bDN\s*([0-9]{2,3})\b", re.IGNORECASE)
FLOW_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
HEIGHT_RANGE_RE = re.compile(r"(?:bauh(?:ö|oe)he|einbauh(?:ö|oe)he|height|h\s*=?)?\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*mm", re.IGNORECASE)
WS_RE = re.compile(r"\bWS\s*([0-9]{2,3})\b|sperrwasserh(?:ö|oe)he\D{0,20}([0-9]{2,3})\s*mm|geruchsverschluss\D{0,20}([0-9]{2,3})\s*mm", re.IGNORECASE)
STANDARD_RE = re.compile(r"\bstandard\b", re.IGNORECASE)
FLATLINE_RE = re.compile(r"\bflatline\b", re.IGNORECASE)
DIMENSION_HINT_RE = re.compile(r"(?:\b\d{1,4}\s*[x×]\s*\d{1,4}(?:\s*[x×]\s*\d{1,4})?\s*mm\b|\bL\s*[x×]\s*B\b|ausspar|cutout|recess|abmess|maß|mass|rost|aufsatz|ablaufk(?:ö|oe)rper|flansch)", re.IGNORECASE)
TECH_HEADER_RE = re.compile(r"artikel|sperrwasser|geruch|dn|abfluss|ablaufleistung|bauh|einbauh|height|ws|flatline|standard|ausspar|abmess|maß|mass|cutout|recess|bezeichnung|beschreibung", re.IGNORECASE)

SOURCE_FIELD_NAMES = ("product_url", "source_url", "sources")


@dataclass(frozen=True)
class ArticleVariantCandidate:
    article_number: str
    source_url: str
    row_description: str
    water_seal_mm: int | None = None
    outlet_dn: str = ""
    flow_rate_lps: float | None = None
    height_adj_min_mm: int | None = None
    height_adj_max_mm: int | None = None
    dimensions_recess_cutout_descriptors: str = ""
    variant_markers: tuple[str, ...] = field(default_factory=tuple)

    @property
    def article_digits(self) -> str:
        return re.sub(r"\D+", "", self.article_number)

    @property
    def height_range(self) -> str:
        if self.height_adj_min_mm is None or self.height_adj_max_mm is None:
            return ""
        return f"{self.height_adj_min_mm}-{self.height_adj_max_mm}"


@dataclass(frozen=True)
class AttributionResult:
    status: str
    matching_candidates: tuple[ArticleVariantCandidate, ...]
    blocking_reason: str
    unique_match_exists: bool


@dataclass(frozen=True)
class DiagnosticReport:
    base_location: str
    base_row: dict[str, str]
    assembled_locations: dict[str, str]
    evidence_rows: list[dict[str, str]]
    inspected_urls: list[str]
    candidates: list[ArticleVariantCandidate]
    attribution: AttributionResult
    counts: dict[str, int]


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(["" for _ in range(0 if df is None else len(df))], index=(None if df is None else df.index), dtype="string")
    return df[col].fillna("").astype(str)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value).strip() == ""


def _display_value(value: Any) -> str:
    return "" if _is_missing(value) else str(value)


def canonical_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw.split("#", 1)[0].split("?", 1)[0])
    path = (parsed.path or "/").replace("//", "/")
    if path != "/":
        path = path.rstrip("/") + "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}" if parsed.scheme and parsed.netloc else raw.split("#", 1)[0].split("?", 1)[0].rstrip("/") + "/"


def _split_urls(value: Any) -> Iterable[str]:
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield str(item)
        return
    text = str(value or "").strip()
    if not text:
        return
    for part in re.split(r"[;,\s]+", text):
        if part.startswith("http"):
            yield part


def urls_from_row(row: pd.Series | dict[str, Any] | None) -> list[str]:
    if row is None:
        return []
    urls: list[str] = []
    for field_name in SOURCE_FIELD_NAMES:
        value = row.get(field_name, "") if hasattr(row, "get") else ""
        for raw_url in _split_urls(value):
            url = canonical_url(raw_url)
            if url and url not in urls:
                urls.append(url)
    return urls


def _is_easyflow_not_plus_text(*values: Any) -> bool:
    text = " ".join(str(v or "") for v in values).lower()
    if "easyflow" not in text:
        return False
    return "easyflowplus" not in text and "easyflow+" not in text and "easyflow-plus" not in text


def locate_product_id(product_id: str, frames: dict[str, pd.DataFrame]) -> tuple[str, pd.Series | None]:
    for name, df in frames.items():
        if df is None or df.empty or "product_id" not in df.columns:
            continue
        matches = df[_norm(df, "product_id") == product_id]
        if not matches.empty:
            return name, matches.iloc[0]
    return "missing", None


def evidence_rows_for_product(evidence: pd.DataFrame, product_id: str) -> list[dict[str, str]]:
    if evidence is None or evidence.empty or "product_id" not in evidence.columns:
        return []
    matches = evidence[_norm(evidence, "product_id") == product_id]
    out: list[dict[str, str]] = []
    for _, row in matches.iterrows():
        out.append({str(k): _display_value(v) for k, v in row.items() if not _is_missing(v)})
    return out


def collect_easyflow_source_urls(registry: pd.DataFrame, debug: Any, rows: Iterable[pd.Series | None]) -> list[str]:
    urls: list[str] = []

    def add(url: str) -> None:
        canon = canonical_url(url)
        if canon and _is_easyflow_not_plus_text(canon) and canon not in urls:
            urls.append(canon)

    for row in rows:
        for url in urls_from_row(row):
            add(url)

    if registry is not None and not registry.empty:
        family = _norm(registry, "product_family").str.lower()
        ids = _norm(registry, "product_id")
        names = _norm(registry, "product_name")
        urls_text = _norm(registry, "product_url") + " " + _norm(registry, "sources")
        text_mask = pd.Series([
            _is_easyflow_not_plus_text(pid, name, url_text)
            for pid, name, url_text in zip(ids.tolist(), names.tolist(), urls_text.tolist())
        ], index=registry.index)
        mask = (family == "easyflow") | text_mask
        for _, row in registry[mask].iterrows():
            for url in urls_from_row(row):
                add(url)

    if isinstance(debug, list):
        for item in debug:
            if not isinstance(item, dict):
                continue
            if not _is_easyflow_not_plus_text(item.get("final_url", ""), item.get("seed_url", ""), item.get("error", "")):
                continue
            for key in ("final_url", "seed_url"):
                add(str(item.get(key, "")))

    return urls


def _to_int(value: str) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _to_float(value: str) -> float | None:
    try:
        return float(value.replace(",", "."))
    except Exception:
        return None


def _extract_ws(text: str) -> int | None:
    for match in WS_RE.finditer(text or ""):
        for group in match.groups():
            if group:
                value = _to_int(group)
                if value is not None and 20 <= value <= 100:
                    return value
    return None


def _extract_dn(text: str) -> str:
    dns = []
    for match in DN_RE.finditer(text or ""):
        value = f"DN{int(match.group(1))}"
        if value not in dns:
            dns.append(value)
    return "/".join(dns)


def _extract_flow(text: str) -> float | None:
    flows = []
    for match in FLOW_RE.finditer(text or ""):
        value = _to_float(match.group(1))
        if value is not None and 0.1 <= value <= 3.0:
            flows.append(value)
    if not flows:
        return None
    return max(flows)


def _extract_height_range(text: str) -> tuple[int | None, int | None]:
    for match in HEIGHT_RANGE_RE.finditer(text or ""):
        low = _to_int(match.group(1))
        high = _to_int(match.group(2))
        if low is None or high is None:
            continue
        low, high = (low, high) if low <= high else (high, low)
        if 0 <= low <= 300 and 0 <= high <= 300:
            return low, high
    return None, None


def _variant_markers(text: str, water_seal_mm: int | None) -> tuple[str, ...]:
    markers: list[str] = []
    if STANDARD_RE.search(text or ""):
        markers.append("Standard")
    if FLATLINE_RE.search(text or ""):
        markers.append("Flatline")
    ws = water_seal_mm or _extract_ws(text)
    if ws is not None:
        markers.append(f"WS{ws}")
    return tuple(dict.fromkeys(markers))


def _descriptor_text(cells: list[str], headers: list[str], row_text: str) -> str:
    parts: list[str] = []
    for header, cell in zip(headers, cells):
        if not cell or ARTICLE_RE.fullmatch(cell):
            continue
        combo = f"{header}: {cell}" if header else cell
        if DIMENSION_HINT_RE.search(combo) or not TECH_HEADER_RE.search(header):
            parts.append(combo)
    if not parts and DIMENSION_HINT_RE.search(row_text):
        parts.append(row_text)
    return " ; ".join(dict.fromkeys(_clean_text(p) for p in parts if _clean_text(p)))


def _table_headers(table: Any) -> list[str]:
    header_row = table.select_one("thead tr") or table.select_one("tr")
    if not header_row:
        return []
    return [_clean_text(cell.get_text(" ", strip=True)) for cell in header_row.select("th,td")]


def parse_article_variant_candidates(html: str, source_url: str) -> list[ArticleVariantCandidate]:
    soup = BeautifulSoup(html or "", "lxml")
    page_text = _clean_text(soup.get_text(" ", strip=True))
    page_markers_text = " ".join([_clean_text(x.get_text(" ", strip=True)) for x in soup.select("h1,h2,h3")])
    candidates: list[ArticleVariantCandidate] = []
    seen: set[tuple[str, str]] = set()

    for table in soup.select("table"):
        headers = _table_headers(table)
        header_text = " ".join(headers)
        if headers and not TECH_HEADER_RE.search(header_text):
            continue
        for tr in table.select("tr"):
            cells = [_clean_text(cell.get_text(" ", strip=True)) for cell in tr.select("th,td")]
            row_text = _clean_text(tr.get_text(" ", strip=True))
            article_match = ARTICLE_RE.search(row_text)
            if not article_match:
                continue
            article_number = article_match.group(0)
            key = (re.sub(r"\D+", "", article_number), row_text)
            if key in seen:
                continue
            seen.add(key)
            candidate_text = " ".join([page_markers_text, header_text, row_text])
            ws = _extract_ws(candidate_text)
            dn = _extract_dn(candidate_text)
            flow = _extract_flow(candidate_text)
            height_min, height_max = _extract_height_range(candidate_text)
            candidates.append(ArticleVariantCandidate(
                article_number=article_number,
                source_url=canonical_url(source_url),
                row_description=row_text,
                water_seal_mm=ws,
                outlet_dn=dn,
                flow_rate_lps=flow,
                height_adj_min_mm=height_min,
                height_adj_max_mm=height_max,
                dimensions_recess_cutout_descriptors=_descriptor_text(cells, headers, row_text),
                variant_markers=_variant_markers(candidate_text, ws),
            ))

    if candidates:
        return candidates

    # Last-resort parser for article-like rows in flattened text. It is intentionally diagnostic-only.
    for article_match in ARTICLE_RE.finditer(page_text):
        start = max(0, article_match.start() - 220)
        end = min(len(page_text), article_match.end() + 220)
        context = _clean_text(page_text[start:end])
        key = (re.sub(r"\D+", "", article_match.group(0)), context)
        if key in seen:
            continue
        seen.add(key)
        ws = _extract_ws(context)
        candidates.append(ArticleVariantCandidate(
            article_number=article_match.group(0),
            source_url=canonical_url(source_url),
            row_description=context,
            water_seal_mm=ws,
            outlet_dn=_extract_dn(context),
            flow_rate_lps=_extract_flow(context),
            height_adj_min_mm=_extract_height_range(context)[0],
            height_adj_max_mm=_extract_height_range(context)[1],
            dimensions_recess_cutout_descriptors=context if DIMENSION_HINT_RE.search(context) else "",
            variant_markers=_variant_markers(context, ws),
        ))
    return candidates


def fetch_article_variant_candidates(urls: Iterable[str]) -> tuple[list[str], list[ArticleVariantCandidate]]:
    inspected: list[str] = []
    candidates: list[ArticleVariantCandidate] = []
    seen_candidates: set[tuple[str, str, str]] = set()
    for url in urls:
        status, final_url, html, _err = aco._safe_get_text(url, timeout=35)
        final = canonical_url(final_url or url)
        if final not in inspected:
            inspected.append(final)
        if status != 200 or not html:
            continue
        if not _is_easyflow_not_plus_text(final, html[:4000]):
            continue
        for candidate in parse_article_variant_candidates(html, final):
            key = (candidate.article_digits, candidate.source_url, candidate.row_description)
            if key not in seen_candidates:
                seen_candidates.add(key)
                candidates.append(candidate)
    return inspected, candidates


def candidate_matches_base(candidate: ArticleVariantCandidate, base_facts: dict[str, Any] | None = None) -> bool:
    facts = base_facts or BASE_FACTS
    if candidate.water_seal_mm != int(facts["water_seal_mm"]):
        return False
    return str(facts["outlet_dn"]).upper() in (candidate.outlet_dn or "").upper().split("/")


def classify_attribution(candidates: list[ArticleVariantCandidate], base_row: pd.Series | None = None) -> AttributionResult:
    if not candidates:
        return AttributionResult("parser_insufficient", tuple(), "No Easyflow article/variant rows could be parsed from inspected source pages.", False)

    matching = tuple(candidate for candidate in candidates if candidate_matches_base(candidate))
    if len(matching) == 1:
        return AttributionResult("exact_article_match", matching, "A single parsed article row matches Easyflow WS50/DN50 base facts.", True)
    if len(matching) > 1:
        articles = ", ".join(candidate.article_number for candidate in matching)
        flows = ", ".join(sorted({"" if c.flow_rate_lps is None else str(c.flow_rate_lps) for c in matching if c.flow_rate_lps is not None})) or "unknown"
        heights = ", ".join(sorted({c.height_range for c in matching if c.height_range})) or "unknown"
        return AttributionResult(
            "multiple_candidate_articles",
            matching,
            f"Multiple Easyflow WS50/DN50 article rows match the current family-level base row ({articles}); candidate flow values={flows}, height ranges={heights}. Do not infer one article at base-row granularity.",
            False,
        )

    has_partial_easyflow_rows = any((candidate.water_seal_mm == 50 or "DN50" in (candidate.outlet_dn or "")) for candidate in candidates)
    if has_partial_easyflow_rows:
        return AttributionResult("family_level_only", tuple(), "Parsed Easyflow rows are only partially comparable to WS50/DN50 base facts; no complete article-level match is proven.", False)
    return AttributionResult("no_article_match", tuple(), "Parsed article rows exist, but none match both WS50 and DN50 base facts.", False)


def _row_summary(row: pd.Series | None) -> dict[str, str]:
    if row is None:
        return {}
    fields = ["product_id", "product_name", "product_family", "water_seal_mm", "outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm", "product_url", "sources"]
    return {field: _display_value(row.get(field, "")) for field in fields if field in row.index and not _is_missing(row.get(field, ""))}


def build_diagnostic_report() -> DiagnosticReport:
    registry_rows, debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

    frames = {"Products": products, "Comparison": comparison, "Candidates_All": registry, "Components": excluded}
    base_location, base_row = locate_product_id(BASE_ID, frames)
    assembled_locations: dict[str, str] = {}
    assembled_rows: list[pd.Series | None] = []
    for product_id in ASSEMBLED_IDS:
        location, row = locate_product_id(product_id, frames)
        assembled_locations[product_id] = location
        assembled_rows.append(row)

    source_urls = collect_easyflow_source_urls(registry, debug, [base_row, *assembled_rows])
    inspected_urls, candidates = fetch_article_variant_candidates(source_urls)
    attribution = classify_attribution(candidates, base_row)

    counts = {
        "Products": len(products),
        "Comparison": len(comparison),
        "Candidates_All": len(registry),
        "Components": len(excluded),
        "BOM_Options": len(bom),
        "Easyflow article candidates": len(candidates),
        "Easyflow WS50/DN50 matching candidates": len(attribution.matching_candidates),
    }

    return DiagnosticReport(
        base_location=base_location,
        base_row=_row_summary(base_row),
        assembled_locations=assembled_locations,
        evidence_rows=evidence_rows_for_product(evidence, BASE_ID),
        inspected_urls=inspected_urls,
        candidates=candidates,
        attribution=attribution,
        counts=counts,
    )


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def print_report(report: DiagnosticReport) -> None:
    print("Easyflow article/variant attribution diagnostic")
    print("\nCounts:")
    for key, value in report.counts.items():
        print(f"- {key}: {value}")

    print("\nBase row:")
    print(f"- location: {report.base_location}")
    for key, value in report.base_row.items():
        print(f"- {key}: {value}")

    print("\nAssembled rows:")
    for product_id, location in report.assembled_locations.items():
        print(f"- {product_id}: {location}")

    print("\nSource URLs inspected:")
    if report.inspected_urls:
        for url in report.inspected_urls:
            print(f"- {url}")
    else:
        print("- none")

    print("\nExtracted Easyflow article/variant candidates:")
    if report.candidates:
        print("| article | WS | DN | flow_lps | height_mm | markers | source_url |")
        print("|---|---:|---|---:|---|---|---|")
        for candidate in report.candidates:
            print(
                "| "
                + " | ".join([
                    candidate.article_number,
                    _fmt(candidate.water_seal_mm),
                    candidate.outlet_dn,
                    _fmt(candidate.flow_rate_lps),
                    candidate.height_range,
                    ", ".join(candidate.variant_markers),
                    candidate.source_url,
                ])
                + " |"
            )
            if candidate.dimensions_recess_cutout_descriptors:
                print(f"  descriptors: {candidate.dimensions_recess_cutout_descriptors}")
            print(f"  row: {candidate.row_description}")
    else:
        print("- none")

    print("\nAttribution classification:")
    print(f"- status: {report.attribution.status}")
    print(f"- unique_article_variant_match_exists: {report.attribution.unique_match_exists}")
    print(f"- blocking_reason: {report.attribution.blocking_reason}")

    print("\nProduction behavior:")
    print("- diagnostic-only; no Products, Comparison, Final_Assemblies, BOM_Options, validator baseline, extraction, discovery, or scoring behavior is modified by this script")
    print("- flow_rate_lps and height adjustment values are not written by this diagnostic")


def main() -> int:
    report = build_diagnostic_report()
    print_report(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
