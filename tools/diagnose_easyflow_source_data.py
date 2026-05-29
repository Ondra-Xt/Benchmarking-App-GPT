from __future__ import annotations

import json
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
TECH_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
SOURCE_FIELD_NAMES = ("product_url", "source_url", "sources")
SEARCH_TERMS = (
    "Ablaufleistung",
    "Abflussleistung",
    "flow",
    "l/s",
    "lps",
    "Bauhöhe",
    "height",
    "Einbauhöhe",
    "Sperrwasserhöhe",
    "DN50",
)
FLOW_CONTEXT_RE = re.compile(r"ablaufleistung|abflussleistung|abflusswert|flow", re.IGNORECASE)
FLOW_VALUE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
HEIGHT_CONTEXT_RE = re.compile(r"bauh(?:ö|oe)he|einbauh(?:ö|oe)he|height", re.IGNORECASE)
HEIGHT_RANGE_RE = re.compile(r"(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm", re.IGNORECASE)


@dataclass
class SourceHit:
    url: str
    term: str
    snippet: str
    structured: bool = False


@dataclass
class CandidateEvidence:
    field: str
    value: str
    url: str
    snippet: str
    structured: bool = False
    source: str = "source_scan"


@dataclass
class FieldAssessment:
    field: str
    verdict: str
    candidates: list[CandidateEvidence] = field(default_factory=list)
    term_hits: list[SourceHit] = field(default_factory=list)


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _snippet(text: str, start: int, end: int, pad: int = 90) -> str:
    return _clean_text(text[max(0, start - pad): min(len(text), end + pad)])


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


def _canonical_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw.split("#", 1)[0].split("?", 1)[0])
        path = (parsed.path or "/").replace("//", "/")
        if path != "/":
            path = path.rstrip("/") + "/"
        return f"{parsed.scheme}://{parsed.netloc}{path}"
    except Exception:
        return raw.split("#", 1)[0].split("?", 1)[0].rstrip("/") + "/"


def _maybe_json_urls(value: Any) -> Iterable[str]:
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield str(item)
        return
    text = str(value or "").strip()
    if not text:
        return
    if text.startswith("["):
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                for item in loaded:
                    yield str(item)
                return
        except Exception:
            pass
    for part in re.split(r"[;,\s]+", text):
        if part.startswith("http"):
            yield part


def urls_from_row(row: pd.Series | dict[str, Any] | None) -> list[str]:
    if row is None:
        return []
    urls: list[str] = []
    for field_name in SOURCE_FIELD_NAMES:
        getter = row.get if hasattr(row, "get") else None
        value = getter(field_name, "") if getter else ""
        for url in _maybe_json_urls(value):
            canon = _canonical_url(url)
            if canon and canon not in urls:
                urls.append(canon)
    return urls


def locate_product_id(product_id: str, frames: dict[str, pd.DataFrame]) -> tuple[str, pd.Series | None]:
    for name, df in frames.items():
        if df is None or df.empty:
            continue
        matches = df[_norm(df, "product_id") == product_id]
        if not matches.empty:
            return name, matches.iloc[0]
    return "missing", None


def current_values(row: pd.Series | None) -> dict[str, str]:
    return {field_name: _display_value(row.get(field_name, "") if row is not None else "") for field_name in TECH_FIELDS}


def diagnostic_row_fields(row: pd.Series | None) -> dict[str, str]:
    if row is None:
        return {}
    interesting: dict[str, str] = {}
    key_tokens = ("source", "evidence", "raw", "detail", "url")
    for key, value in row.items():
        if key in SOURCE_FIELD_NAMES or any(token in str(key).lower() for token in key_tokens):
            if not _is_missing(value):
                interesting[str(key)] = _display_value(value)
    return interesting


def evidence_rows_for_product(evidence: pd.DataFrame, product_id: str) -> list[dict[str, str]]:
    if evidence is None or evidence.empty or "product_id" not in evidence.columns:
        return []
    matches = evidence[_norm(evidence, "product_id") == product_id]
    out: list[dict[str, str]] = []
    for _, row in matches.iterrows():
        out.append({str(k): _display_value(v) for k, v in row.items() if not _is_missing(v)})
    return out


def easyflow_source_urls(registry: pd.DataFrame, debug: Any, rows: Iterable[pd.Series | None]) -> list[str]:
    urls: list[str] = []
    for row in rows:
        for url in urls_from_row(row):
            if url not in urls:
                urls.append(url)

    if registry is not None and not registry.empty:
        family = _norm(registry, "product_family").str.lower()
        ids = _norm(registry, "product_id").str.lower()
        names = _norm(registry, "product_name").str.lower()
        mask = family.str.contains("easyflow", na=False) | ids.str.contains("easyflow", na=False) | names.str.contains("easyflow", na=False)
        for _, row in registry[mask].iterrows():
            for url in urls_from_row(row):
                if url not in urls:
                    urls.append(url)

    if isinstance(debug, list):
        for item in debug:
            if not isinstance(item, dict):
                continue
            text = " ".join(str(item.get(k, "")) for k in ("final_url", "seed_url", "error"))
            if "easyflow" not in text.lower():
                continue
            for key in ("final_url", "seed_url"):
                url = _canonical_url(str(item.get(key, "")))
                if url.startswith("http") and url not in urls:
                    urls.append(url)

    return urls


def _table_texts(html: str) -> list[str]:
    soup = BeautifulSoup(html or "", "lxml")
    texts: list[str] = []
    for table in soup.select("table"):
        table_text = _clean_text(table.get_text(" ", strip=True))
        if table_text:
            texts.append(table_text)
        for tr in table.select("tr"):
            row_text = _clean_text(tr.get_text(" ", strip=True))
            if row_text:
                texts.append(row_text)
    return texts


def _main_text(html: str) -> str:
    if hasattr(aco, "_main_flat_text_from_html"):
        return aco._main_flat_text_from_html(html)  # type: ignore[attr-defined]
    soup = BeautifulSoup(html or "", "lxml")
    return _clean_text(soup.get_text(" ", strip=True))


def _scan_terms(url: str, html: str) -> list[SourceHit]:
    hits: list[SourceHit] = []
    flat = _main_text(html)
    table_text = " ".join(_table_texts(html))
    for term in SEARCH_TERMS:
        pattern = re.compile(re.escape(term), re.IGNORECASE)
        for match in pattern.finditer(flat):
            structured = bool(pattern.search(table_text)) and pattern.search(table_text).group(0).lower() == match.group(0).lower()
            hits.append(SourceHit(url=url, term=term, snippet=_snippet(flat, match.start(), match.end()), structured=structured))
            break
    return hits


def _scan_candidate_values(url: str, html: str) -> list[CandidateEvidence]:
    candidates: list[CandidateEvidence] = []
    flat = _main_text(html)
    table_texts = _table_texts(html)

    for text, structured in [(t, True) for t in table_texts] + [(flat, False)]:
        for match in FLOW_VALUE_RE.finditer(text):
            context = _snippet(text, match.start(), match.end())
            if not FLOW_CONTEXT_RE.search(context):
                continue
            try:
                value = str(float(match.group(1).replace(",", ".")))
            except Exception:
                value = match.group(1).replace(",", ".")
            candidates.append(CandidateEvidence("flow_rate_lps", value, url, context, structured))
        for match in HEIGHT_RANGE_RE.finditer(text):
            context = _snippet(text, match.start(), match.end())
            if not HEIGHT_CONTEXT_RE.search(context):
                continue
            lo = int(match.group(1))
            hi = int(match.group(2))
            if lo > hi:
                lo, hi = hi, lo
            candidates.append(CandidateEvidence("height_adj_min_mm", str(lo), url, context, structured))
            candidates.append(CandidateEvidence("height_adj_max_mm", str(hi), url, context, structured))

    # Prefer parser evidence when the current connector can already extract something from a source.
    parsed = aco.extract_parameters(url) if hasattr(aco, "extract_parameters") else {}
    for field_name in ("flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"):
        value = parsed.get(field_name) if isinstance(parsed, dict) else None
        if _is_missing(value):
            continue
        snippet = ""
        for label, evidence_snippet, evidence_url in parsed.get("evidence", []):
            if field_name.startswith("flow") and "flow" in str(label).lower():
                snippet = str(evidence_snippet)
                break
            if field_name.startswith("height") and "einbau" in str(label).lower():
                snippet = str(evidence_snippet)
                break
        candidates.append(CandidateEvidence(field_name, _display_value(value), url, snippet or f"extract_parameters value={value}", True, "extract_parameters"))
    return _dedupe_candidates(candidates)


def _dedupe_candidates(candidates: Iterable[CandidateEvidence]) -> list[CandidateEvidence]:
    out: list[CandidateEvidence] = []
    seen: set[tuple[str, str, str, str]] = set()
    for candidate in candidates:
        key = (candidate.field, candidate.value, candidate.url, candidate.snippet[:160])
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def scan_easyflow_sources(urls: Iterable[str]) -> tuple[list[str], list[SourceHit], list[CandidateEvidence]]:
    inspected: list[str] = []
    term_hits: list[SourceHit] = []
    candidates: list[CandidateEvidence] = []
    for url in urls:
        if not url.startswith("http") or url in inspected:
            continue
        status, final, html, err = aco._safe_get_text(url, timeout=35)  # type: ignore[attr-defined]
        final_url = _canonical_url(final or url)
        inspected.append(final_url)
        if status != 200 or not html:
            term_hits.append(SourceHit(final_url, "FETCH", f"status={status} err={err}"))
            continue
        term_hits.extend(_scan_terms(final_url, html))
        candidates.extend(_scan_candidate_values(final_url, html))
    return inspected, term_hits, _dedupe_candidates(candidates)


def assess_missing_fields(missing_fields: Iterable[str], term_hits: list[SourceHit], candidates: list[CandidateEvidence]) -> dict[str, FieldAssessment]:
    assessments: dict[str, FieldAssessment] = {}
    for field_name in missing_fields:
        field_candidates = [candidate for candidate in candidates if candidate.field == field_name]
        if field_name.startswith("height_adj"):
            field_terms = [hit for hit in term_hits if hit.term.lower() in {"bauhöhe", "height", "einbauhöhe"}]
        elif field_name == "flow_rate_lps":
            field_terms = [hit for hit in term_hits if hit.term.lower() in {"ablaufleistung", "abflussleistung", "flow", "l/s", "lps"}]
        else:
            field_terms = []

        distinct_values = {candidate.value for candidate in field_candidates if not _is_missing(candidate.value)}
        if len(distinct_values) > 1:
            verdict = "mentioned but ambiguous"
        elif any(candidate.structured or candidate.source == "extract_parameters" for candidate in field_candidates):
            verdict = "source-backed and extractable"
        elif field_candidates:
            verdict = "present only in non-structured text"
        elif field_terms:
            verdict = "mentioned but ambiguous"
        else:
            verdict = "absent"
        assessments[field_name] = FieldAssessment(field_name, verdict, field_candidates, field_terms)
    return assessments


def print_report(
    counts: dict[str, int],
    base_location: str,
    base_values: dict[str, str],
    assembled_values: dict[str, dict[str, str]],
    diagnostic_fields: dict[str, str],
    evidence_rows: list[dict[str, str]],
    inspected_urls: list[str],
    assessments: dict[str, FieldAssessment],
) -> None:
    print("Easyflow source-backed missing-field diagnostic")
    print("\nCounts:")
    for key, value in counts.items():
        print(f"- {key}: {value}")

    print(f"\nBase row: {BASE_ID}")
    print(f"- location: {base_location}")
    print("- current values:")
    for field_name in TECH_FIELDS:
        print(f"  - {field_name}: {base_values.get(field_name, '')}")

    print("\nAssembled rows current values:")
    for product_id in ASSEMBLED_IDS:
        print(f"- {product_id}:")
        for field_name in TECH_FIELDS:
            print(f"  - {field_name}: {assembled_values.get(product_id, {}).get(field_name, '')}")

    print("\nAvailable base candidate/source/evidence/raw/detail fields:")
    if diagnostic_fields:
        for key, value in diagnostic_fields.items():
            print(f"- {key}: {value}")
    else:
        print("- none")

    print("\nPipeline evidence rows for base row:")
    if evidence_rows:
        for row in evidence_rows:
            source_url = row.get("source_url", "")
            field_name = row.get("field", row.get("field_name", ""))
            value = row.get("extracted_value", row.get("value", ""))
            snippet = row.get("snippet", row.get("source_note", ""))
            print(f"- field={field_name} value={value} source_url={source_url} snippet={snippet}")
    else:
        print("- none")

    print("\nEasyflow source URLs inspected:")
    for url in inspected_urls:
        print(f"- {url}")

    print("\nMissing-field source assessment:")
    for field_name in ("flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"):
        assessment = assessments[field_name]
        print(f"- {field_name}: {assessment.verdict}")
        if assessment.candidates:
            print("  - candidate evidence:")
            for candidate in assessment.candidates:
                kind = "structured" if candidate.structured else "non-structured"
                print(f"    - value={candidate.value} source={candidate.source} kind={kind} url={candidate.url} snippet={candidate.snippet}")
        elif assessment.term_hits:
            print("  - term evidence:")
            for hit in assessment.term_hits[:5]:
                kind = "structured" if hit.structured else "non-structured"
                print(f"    - term={hit.term} kind={kind} url={hit.url} snippet={hit.snippet}")
        else:
            print("  - evidence: none found in inspected Easyflow sources")

    print("\nProduction behavior changed: no (diagnostic script only; Products are not modified)")


def main() -> int:
    registry_rows, debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

    frames = {
        "Products": products,
        "Comparison": comparison,
        "Candidates_All": registry,
        "Components": excluded,
    }
    base_location, base_row = locate_product_id(BASE_ID, frames)
    assembled_rows: dict[str, pd.Series | None] = {}
    assembled_values: dict[str, dict[str, str]] = {}
    for product_id in ASSEMBLED_IDS:
        _, row = locate_product_id(product_id, frames)
        assembled_rows[product_id] = row
        assembled_values[product_id] = current_values(row)

    base_values = current_values(base_row)
    missing_fields = [field_name for field_name in ("flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm") if _is_missing(base_values.get(field_name))]
    urls = easyflow_source_urls(registry, debug, [base_row, *assembled_rows.values()])
    inspected_urls, term_hits, candidates = scan_easyflow_sources(urls)
    assessments = assess_missing_fields(missing_fields or ("flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"), term_hits, candidates)

    counts = {
        "Products": len(products),
        "Comparison": len(comparison),
        "Candidates_All": len(registry),
        "Components": len(excluded),
        "BOM_Options": len(bom),
        "total assembled rows": int(_norm(products, "product_id").str.startswith("aco-assembled-").sum()) if not products.empty else 0,
        "easyflow assembled": int(_norm(products, "product_id").str.startswith("aco-assembled-easyflow-").sum()) if not products.empty else 0,
    }
    print_report(
        counts,
        base_location,
        base_values,
        assembled_values,
        diagnostic_row_fields(base_row),
        evidence_rows_for_product(evidence, BASE_ID),
        inspected_urls,
        assessments,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
