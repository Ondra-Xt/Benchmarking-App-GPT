from __future__ import annotations

import argparse
import io
import html
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.connectors import tece
from tools.tece_report_output import write_json_output, write_text_output

TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "water_seal_mm",
)
PRODUCTION_STATUS_NOTE = (
    "diagnostic-only TECE source inventory; no Products/Comparison/BOM/assembly generation change; "
    "TECE production promotion blocked pending article-level compatibility evidence and complete technical evidence"
)


@dataclass(frozen=True)
class TeceSourcePackRow:
    source_file: str
    source_type: str
    product_family: str
    product_name: str
    article_number: str
    source_url: str
    nominal_length_mm: Any
    flow_rate_lps: Any
    water_seal_mm: Any
    outlet_dn: Any
    height_adj_min_mm: Any
    height_adj_max_mm: Any
    installation_height_mm: Any
    evidence_text: str
    missing_fields: list[str]
    confidence: float
    compatibility_evidence_type: str
    article_level_compatibility_evidence_exists: bool
    production_promotion_blocked: bool
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    recommended_next_action: str
    tece_article_role_candidate: str
    tece_family_candidate: str
    classification_confidence: str
    classification_reason: str
    production_blocking_reason: str
    source_page_start: Any = ""
    source_page_end: Any = ""
    page_range_label: str = ""
    conditional_technical_values: list[dict[str, Any]] | None = None
    width_mm: Any = ""
    finish_or_color: str = ""
    source_pdf_physical_page_start: Any = ""
    source_pdf_physical_page_end: Any = ""
    catalogue_page_label: str = ""
    extraction_method: str = "generic_article_context"
    extraction_priority: int = 10


@dataclass(frozen=True)
class TeceSourcePackReport:
    source_pack_path: str
    source_pack_file_count: int
    source_pack_candidate_count: int
    article_numbers: list[str]
    technical_field_coverage: dict[str, int]
    missing_field_counts: dict[str, int]
    compatibility_evidence_status: str
    article_level_compatibility_evidence_exists: bool
    production_promotion_blocked: bool
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    recommended_next_action: str
    rows: list[TeceSourcePackRow]
    manifest: dict[str, Any] | None = None
    evidence_scope_counts: dict[str, int] | None = None
    cover_grate_matrix_evidence_exists: bool = False
    assembly_matrix_evidence_exists: bool = False
    source_pack_classification_summary: dict[str, Any] | None = None
    page_range_label_counts: dict[str, int] | None = None
    conditional_technical_value_count: int = 0
    row_examples_by_label_and_role: dict[str, dict[str, list[dict[str, Any]]]] | None = None

@dataclass(frozen=True)
class TeceInventoryRow:
    product_id: str
    article_number: str
    product_family: str
    product_name: str
    product_url: str
    source_urls: str
    length_mm: Any
    length_delta_mm: Any
    flow_rate_lps: Any
    outlet_dn: Any
    height_adj_min_mm: Any
    height_adj_max_mm: Any
    water_seal_mm: Any
    evidence_sources: str
    missing_fields: str
    possible_assembly_model: str
    article_level_compatibility_evidence: bool
    production_promotion_blocked: bool
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    production_status_note: str


@dataclass(frozen=True)
class TeceInventoryReport:
    candidate_count: int
    article_numbers: list[str]
    source_urls: list[str]
    product_families: list[str]
    product_names: list[str]
    length_coverage: dict[str, int]
    technical_field_coverage: dict[str, int]
    evidence_source_counts: dict[str, int]
    missing_field_counts: dict[str, int]
    possible_assembly_model: str
    article_level_compatibility_evidence_exists: bool
    production_promotion_blocked: bool
    production_status_note: str
    seed_urls: list[str]
    seed_status_summary: list[dict[str, Any]]
    blocked_live_seed_count: int
    async_or_placeholder_seed_count: int
    discovered_candidate_count_before_length_filter: int
    accepted_candidate_count_after_length_filter: int
    sample_rejected_urls: list[dict[str, Any]]
    recommended_next_action: str
    overall_status: str
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    discovery_debug: list[dict[str, Any]]
    rows: list[TeceInventoryRow]
    source_pack: TeceSourcePackReport | None = None


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _article_from_candidate(candidate: dict[str, Any]) -> str:
    value = _clean(candidate.get("product_id"))
    if value.startswith("tece-"):
        return value.split("tece-", 1)[1]
    return tece._extract_article_from_text(" ".join(_clean(candidate.get(k)) for k in ("product_url", "sources", "product_name"))) or ""


def _evidence_kinds(params: dict[str, Any], article: str) -> set[str]:
    kinds: set[str] = set()
    for label, _text, url in params.get("evidence") or []:
        joined = f"{label} {url}".lower()
        if "html" in joined:
            kinds.add("HTML")
        if ".pdf" in joined and "tcdb_" in joined:
            kinds.add("guessed tcdb PDF")
        elif ".pdf" in joined or "pdf" in joined:
            kinds.add("PDF datasheet")
    if article and not any("guessed tcdb PDF" == k for k in kinds):
        # The connector has a deterministic tcdb fallback URL; report it as guessed when used/available for audit.
        if any(tece._guess_tcdb_pdf(article) in _clean(item[2]) or tece._guess_tcdb_pdf(article) in _clean(item[1]) for item in (params.get("evidence") or [])):
            kinds.add("guessed tcdb PDF")
    return kinds or {"HTML"}


def _source_urls(candidate: dict[str, Any], params: dict[str, Any], article: str) -> list[str]:
    urls = []
    for key in ("product_url", "sources"):
        value = _clean(candidate.get(key))
        if value and value not in urls:
            urls.append(value)
    for _label, _text, url in params.get("evidence") or []:
        value = _clean(url)
        if value.startswith("http") and value not in urls:
            urls.append(value)
    if article:
        guessed = tece._guess_tcdb_pdf(article)
        if guessed in " ".join(urls) and guessed not in urls:
            urls.append(guessed)
    return urls



def _jsonish(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _diagnostics_from_debug(debug: list[dict[str, Any]], candidate_count: int, rows: list[TeceInventoryRow]) -> dict[str, Any]:
    seed_rows = [d for d in debug if d.get("method") == "produktdaten_seed"]
    seed_status_summary = [
        {
            "seed_url": d.get("seed_url"),
            "status_code": d.get("status_code"),
            "final_url": d.get("final_url"),
            "candidates_found": d.get("candidates_found", 0),
            "classification": d.get("classification") or ("async_placeholder" if d.get("status_code") == 202 else "ok"),
        }
        for d in seed_rows
    ]
    blocked = sum(1 for d in seed_rows if d.get("blocked_live_seed") or d.get("status_code") == 202)
    async_count = sum(1 for d in seed_rows if d.get("async_or_placeholder") or d.get("status_code") == 202)
    final = next((d for d in reversed(debug) if d.get("method") == "final"), {})
    discovered = int(final.get("candidates_found") or sum(int(d.get("candidates_found") or 0) for d in debug if d.get("method") in {"produktdaten_seed", "tece_com_product_page"}))
    accepted = int(final.get("after_length_filter") or candidate_count)
    rejected: list[dict[str, Any]] = []
    for item in _jsonish(final.get("sample_dropped_by_length")) or []:
        if isinstance(item, dict):
            rejected.append({"url": item.get("url"), "reason": "missing_length" if item.get("length_mm") is None else "outside_target_length_tolerance", "length_mm": item.get("length_mm")})
    for url in _jsonish(final.get("sample_index_only_urls")) or []:
        rejected.append({"url": url, "reason": "index_or_baukasten_not_article_product"})
    rejected = rejected[:20]

    if candidate_count == 0 and blocked:
        overall = "TECE_SOURCE_INVENTORY_BLOCKED_LIVE_SOURCE"
        action = "TECE live seeds are blocked or returned HTTP 202/asynchronous placeholder responses; retry with a browser/session-capable fetch or obtain stable TECE export/API/PDF source URLs before promotion."
    elif candidate_count > 0 and not any(row.article_level_compatibility_evidence for row in rows):
        overall = "TECE_SOURCE_INVENTORY_INCOMPLETE"
        action = "Collect article-level compatibility evidence and missing technical fields; keep TECE diagnostic-only."
    else:
        overall = "TECE_SOURCE_INVENTORY_DIAGNOSTIC_ONLY"
        action = "Review evidence manually; production promotion remains blocked by policy."
    return {
        "seed_status_summary": seed_status_summary,
        "blocked_live_seed_count": blocked,
        "async_or_placeholder_seed_count": async_count,
        "discovered_candidate_count_before_length_filter": discovered,
        "accepted_candidate_count_after_length_filter": accepted,
        "sample_rejected_urls": rejected,
        "recommended_next_action": action,
        "overall_status": overall,
    }


SOURCE_PACK_EXTENSIONS = {".html", ".htm", ".txt", ".pdf", ".json", ".csv"}
SOURCE_PACK_MANIFEST = "tece_source_pack_manifest.json"
GENERATED_SOURCE_PACK_OUTPUTS = {
    "inventory_report.json",
    "classification_report.json",
    "evidence_gap_report.json",
    "compatibility_diagnostics_report.json",
    "coverage_report.json",
    "review_shortlist_report.json",
    "inventory_review.csv",
    "inventory_full_check.csv",
    "tece.csv",
}
EVIDENCE_SCOPES = {"article_data", "technical_datasheet", "cover_grate_matrix", "assembly_matrix", "unknown"}
SOURCE_PACK_TECHNICAL_FIELDS = (
    "nominal_length_mm",
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "installation_height_mm",
)

GENERATED_ARTIFACT_NAME_PATTERNS = (
    re.compile(r"(?i)(?:^|[_-])(?:inventory|coverage|review|shortlist|report|export|output|full_check)(?:[_-]|$)"),
    re.compile(r"(?i)^(?:tece|products|comparison|bom_options|final_assemblies|final_set_details)\.csv$"),
)


def _is_generated_source_pack_artifact(path: Path) -> bool:
    """Return True for app-generated diagnostics/exports that must never be re-ingested."""
    name = path.name
    lower_name = name.lower()
    if lower_name in GENERATED_SOURCE_PACK_OUTPUTS:
        return True
    if path.suffix.lower() not in {".csv", ".json"}:
        return False
    return any(pattern.search(name) for pattern in GENERATED_ARTIFACT_NAME_PATTERNS)



def _count_values(rows: list[Any], attr: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for value in str(getattr(row, attr, "") or "unknown").split(","):
            key = value.strip() or "unknown"
            counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def classify_source_pack_row(text: str, manifest_source: dict[str, Any] | None = None, missing_fields: list[str] | None = None) -> dict[str, str]:
    """Conservative, diagnostic-only TECE source-pack row classification."""
    manifest_source = manifest_source or {}
    missing_fields = missing_fields or []
    haystack = " ".join(_clean(v) for v in (
        text,
        manifest_source.get("document_title"),
        manifest_source.get("product_family_hint"),
        manifest_source.get("evidence_scope"),
        manifest_source.get("notes"),
        manifest_source.get("source_file"),
    )).lower()
    scope = _clean(manifest_source.get("evidence_scope")) or "unknown"

    family = "unknown"
    hint = _clean(manifest_source.get("product_family_hint") or manifest_source.get("page_range_label"))
    for candidate in ("TECEdrainprofile", "TECEdrainline", "TECEdrainpoint S", "TECEdrainpoint", "TECEdrainway"):
        if candidate.lower() in hint.lower():
            family = candidate
            break
    if family == "unknown":
        for candidate in ("TECEdrainprofile", "TECEdrainline", "TECEdrainpoint S", "TECEdrainpoint", "TECEdrainway"):
            if candidate.lower() in haystack:
                family = candidate
                break

    article_match = re.search(r"(?i)\b(?:article number|best\.-?nr\.?|artikel(?:nummer)?)[:\s]*([0-9]{5,8})\b", haystack)
    article = article_match.group(1) if article_match else ""

    role = "unknown"
    reason = "no_conservative_role_keyword"
    if family == "TECEdrainline" and article in {"650000", "650001", "650002", "650003", "650004"}:
        role, reason = "drain_body", "tecedrainline_article_range_650000_650004_drain_body"
    elif family == "TECEdrainline" and article in {"660002", "660003", "660004", "660005", "660006", "660015", "660021", "668010", "668011", "668019", "668025", "668030", "668031", "668032", "668035"}:
        role, reason = "accessory", "tecedrainline_known_accessory_or_spare_part_article"
    elif family == "TECEdrainprofile" and re.fullmatch(r"67[01]\d{3}", article):
        if re.search(r"\b(channel|rinne|profilrinne)\b", haystack):
            role, reason = "profile_cover", "tecedrainprofile_article_range_670xxx_671xxx_visible_profile_channel"
        else:
            role, reason = "profile_cover", "tecedrainprofile_article_range_670xxx_671xxx_visible_profile_cover"
    elif family == "TECEdrainprofile" and article in {"673001", "673002", "673003"}:
        role, reason = "drain_body", "tecedrainprofile_article_range_673001_673003_drain"
    elif scope == "cover_grate_matrix":
        role, reason = "compatibility_matrix", "evidence_scope=cover_grate_matrix"
    elif scope == "assembly_matrix":
        role, reason = "assembly_matrix", "evidence_scope=assembly_matrix"
    elif re.search(r"\bchannel\s+body\b", haystack):
        role, reason = "channel_body", "keyword=channel_body"
    elif re.search(r"\b(cover|grate|abdeckung|rost)\b|designrost|designabdeckung|glasabdeckung|fliesenmulde", haystack):
        role, reason = "cover_or_grate", "keyword=cover_or_grate"
    elif family in {"TECEdrainpoint", "TECEdrainpoint S"} and re.search(r"\b(ablauf|abläufe|ablaufset)\b", haystack):
        role, reason = "drain_body", "tecedrainpoint_keyword=ablauf_or_ablaufset"
    elif re.search(r"\b(complete\s+set|set|komplettset|komplett-set)\b", haystack):
        role, reason = "complete_set", "keyword=complete_set_candidate"
    elif re.search(r"\b(zubehör|ersatzteil|accessory|spare\s+part)\b", haystack):
        role, reason = "accessory", "keyword=accessory_or_spare_part"
    elif scope == "technical_datasheet":
        role, reason = "technical_datasheet_only", "evidence_scope=technical_datasheet"
    elif re.search(r"\b(drain\s+body|ablauf|abläufe|siphon|ablaufset)\b", haystack):
        role, reason = "drain_body", "keyword=drain_body_or_ablauf"

    confidence = "low"
    if role != "unknown" and family != "unknown":
        confidence = "high"
    elif role != "unknown" or family != "unknown":
        confidence = "medium"

    blocking = ["missing_article_level_compatibility_matrix"]
    if role in {"cover_or_grate", "cover_plate", "profile_cover", "visible_profile", "channel_body", "drain_body", "profile_body", "profile_channel", "drain_component"}:
        blocking.append("missing_counterpart_article")
    if missing_fields:
        blocking.append("missing_technical_fields")
    synthetic_markers = ("test fixture", "synthetic", "example.invalid")
    if any(marker in haystack for marker in synthetic_markers):
        blocking.append("synthetic_test_fixture_only")
    return {
        "tece_article_role_candidate": role,
        "tece_family_candidate": family,
        "classification_confidence": confidence,
        "classification_reason": reason,
        "production_blocking_reason": ", ".join(dict.fromkeys(blocking)),
    }

def _strip_markup(text: str) -> str:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def _read_source_pack_file(path: Path, manifest_source: dict[str, Any] | None = None) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        try:
            from pypdf import PdfReader  # type: ignore
        except ImportError:
            try:
                from PyPDF2 import PdfReader  # type: ignore
            except ImportError:
                return ""
        try:
            reader = PdfReader(str(path))
            pages = list(reader.pages)
        except Exception:
            return ""
        manifest_source = manifest_source or {}
        start = manifest_source.get("page_start")
        end = manifest_source.get("page_end")
        if isinstance(start, int) and isinstance(end, int):
            # Manifest page numbers are catalogue/user-facing 1-based pages.
            pages = pages[max(start - 1, 0):min(end, len(pages))]
        return "\n".join(page.extract_text() or "" for page in pages)
    return path.read_text(encoding="utf-8", errors="replace")


def _first_match(patterns: list[str], text: str, *, flags: int = re.I) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags)
        if match:
            return match.group(1).strip()
    return ""


def _extract_float(patterns: list[str], text: str) -> Any:
    value = _first_match(patterns, text)
    if not value:
        return ""
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return value


def _extract_int(patterns: list[str], text: str) -> Any:
    value = _first_match(patterns, text)
    if not value:
        return ""
    try:
        return int(float(value.replace(",", ".")))
    except ValueError:
        return value


def _extract_source_url(raw_text: str, text: str) -> str:
    meta = _first_match([r"(?is)<meta[^>]+(?:name|property)=[\"'](?:source_url|og:url)[\"'][^>]+content=[\"']([^\"']+)", r"(?is)<meta[^>]+content=[\"']([^\"']+)[\"'][^>]+(?:name|property)=[\"'](?:source_url|og:url)[\"']"], raw_text, flags=0)
    if meta:
        return meta
    return _first_match([r"\b(https?://[^\s)<>\"']+)"], text, flags=0)


def _extract_conditional_flow_values(text: str) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    # Handles German catalogue forms like "Ablaufleistung ... >=0,72/>=0,82 l/s bei 10/20 mm Aufstau".
    paired = re.finditer(
        r"(?:Ablaufleistung|flow rate|drainage capacity)?[^.\n]{0,120}?([<>]=?\s*)?([0-9]+[,.][0-9]+)\s*/\s*([<>]=?\s*)?([0-9]+[,.][0-9]+)\s*l/s[^.\n]{0,80}?(?:bei\s*)?10\s*/\s*20\s*mm\s*Aufstau",
        text,
        re.I,
    )
    for match in paired:
        for value, head in ((match.group(2), 10), (match.group(4), 20)):
            values.append({
                "parameter_name": "flow_rate_lps",
                "value": float(value.replace(",", ".")),
                "condition_type": "head_water_level",
                "condition_value": head,
                "condition_unit": "mm",
                "condition_label": f"{head} mm Aufstau",
            })
    for match in re.finditer(r"([<>]=?\s*)?([0-9]+[,.][0-9]+)\s*l/s[^.\n]{0,80}?(?:bei\s*)?(10|20)\s*mm\s*Aufstau", text, re.I):
        head = int(match.group(3))
        item = {
            "parameter_name": "flow_rate_lps",
            "value": float(match.group(2).replace(",", ".")),
            "condition_type": "head_water_level",
            "condition_value": head,
            "condition_unit": "mm",
            "condition_label": f"{head} mm Aufstau",
        }
        if item not in values:
            values.append(item)
    return values


def _normalize_article_number(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _article_contexts(text: str) -> list[tuple[str, str, str, int]]:
    article_label = r"(?:Best\.-?Nr\.?|Artikel(?:\s*(?:Nr\.?|nummer))?|Article(?:\s*(?:no\.?|number))?)"
    article_value = r"([0-9](?:\s*[0-9]){4,7})"
    matches = list(re.finditer(rf"(?i){article_label}\s*[:#-]?\s*{article_value}|\b([0-9]{{6,8}})\b", text))
    contexts: list[tuple[str, str, str, int]] = []
    seen: set[str] = set()
    for idx, match in enumerate(matches):
        article = _normalize_article_number(match.group(1) or match.group(2) or "")
        if len(article) not in {5, 6, 7, 8}:
            continue
        if not article or article in seen:
            continue
        seen.add(article)
        left = max(0, match.start() - 350)
        right = min(len(text), (matches[idx + 1].start() + 120) if idx + 1 < len(matches) else match.end() + 700)
        contexts.append((article, text[left:right], "generic_article_context", 10))
    return contexts


def _fields_from_text(text: str, conditional_values: list[dict[str, Any]]) -> dict[str, Any]:
    unconditional_flow = _extract_float([
        r"flow\s*rate\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*(?:l/s|lps)(?![^.\n]{0,80}(?:10|20)\s*mm\s*Aufstau)",
        r"drainage\s*capacity\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*(?:l/s|lps)(?![^.\n]{0,80}(?:10|20)\s*mm\s*Aufstau)",
        r"Ablaufleistung\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*l/s(?![^.\n]{0,80}(?:10|20)\s*mm\s*Aufstau)",
    ], text)
    return {
        "nominal_length_mm": _extract_int([r"(?:nominal\s*)?length\s*[:=]?\s*(\d{3,4})\s*mm", r"(?:Nennlänge|Länge|Length)\s*[:=]?\s*(\d{3,4})\s*mm", r"\b(6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2})\s*mm\b"], text),
        "flow_rate_lps": "" if conditional_values else unconditional_flow,
        "water_seal_mm": _extract_int([r"(?:reduzierte\s*)?Sperrwasserhöhe\s*[:=]?\s*(\d{2,3})\s*mm", r"water\s*seal\s*[:=]?\s*(\d{2,3})\s*mm"], text),
        "outlet_dn": _first_match([r"(?:outlet\s*[:=]?\s*)?(DN\s*\d{2,3})\b"], text).replace(" ", ""),
        "height_adj_min_mm": _extract_int([r"height\s*adjust(?:ment|able)?\s*[:=]?\s*(\d{2,4})\s*(?:-|to|–)\s*\d{2,4}\s*mm"], text),
        "height_adj_max_mm": _extract_int([r"height\s*adjust(?:ment|able)?\s*[:=]?\s*\d{2,4}\s*(?:-|to|–)\s*(\d{2,4}(?:[,.]\d+)?)\s*mm"], text),
        "installation_height_mm": _extract_int([r"(?:min\.\s*)?(?:Aufbauhöhe|installation\s*height)\s*[:=]?\s*(\d{2,4}(?:[,.]\d+)?)\s*mm"], text),
        "width_mm": _extract_int([r"(?:Breite|width)\s*[:=]?\s*(\d{2,4})\s*mm", r"(?:Länge|Length)\s*[:=]?\s*\d{3,4}\s*mm\s+(?:Breite|width)\s*[:=]?\s*(\d{2,4})\s*mm"], text),
        "finish_or_color": _first_match([r"(?:Farbe|Oberfläche|finish|color)\s*[:=]?\s*([^.;\n]+?)(?=\s*(?:Best\.-?Nr\.?|Article|Artikel|LE\b|$))"], text),
    }



def _same_row_table_contexts(text: str) -> list[tuple[str, str, str, int]]:
    """Extract same-row catalogue table contexts without using LE/index columns as dimensions."""
    contexts: list[tuple[str, str, str, int]] = []
    finish_words = r"(?:Chrom\s+schwarz\s+gebürstet|Gold\s+Optik\s+gebürstet|Gold\s+Optik\s+glänzend|Rotgold\s+gebürstet|Schwarz\s+gebürstet|Edelstahl\s+gebürstet|Edelstahl\s+poliert)"
    row_patterns = [
        re.compile(rf"(?P<length>\d{{3,4}})\s*mm\s+(?P<width>\d{{2,4}})\s*mm\s+(?P<finish>{finish_words})\s+(?P<article>\d{{6,8}})\b", re.I),
        re.compile(rf"(?P<length>\d{{3,4}})\s*mm\s+(?P<finish>{finish_words})\s+(?P<article>\d{{6,8}})\b", re.I),
        re.compile(r"(?P<article>\d{6,8})\s+(?P<length>\d{3,4})\s*mm(?:\s+(?P<width>\d{2,4})\s*mm)?(?:\s+(?P<finish>[^.;\n]{2,60}?))?(?=\s+\d{6,8}\b|$)", re.I),
    ]
    for row_re in row_patterns:
        for match in row_re.finditer(text):
            start = max(0, match.start() - 140)
            header = text[start:match.start()].lower()
            if re.search(r"best\.-?nr\.\s+le\s*1\s+le\s*2\s+le\s*3\s+seite", header):
                continue
            article = match.group("article")
            length = match.group("length")
            width = match.groupdict().get("width") or ""
            finish = re.sub(r"\s+", " ", match.groupdict().get("finish") or "").strip()
            family_hint = "TECEdrainprofile" if re.fullmatch(r"67[01]\d{3}", article) else "TECE catalogue"
            context = f"{family_hint} product table row Länge: {length} mm"
            if width:
                context += f" Breite: {width} mm"
            if finish:
                context += f" Farbe: {finish}"
            context += f" Best.-Nr. {article}"
            contexts.append((article, context, "structured_same_row_table", 100))
    return contexts


def _table_title_before(text: str, start: int) -> str:
    left = text[max(0, start - 260):start]
    match = re.search(r"(TECEdrainline[^.;\n]{0,220}(?:Designrost|Designabdeckung|Glasabdeckung|Fliesenmulde|Duschrinne|Naturstein|Trägerblech)[^.;\n]{0,120})", left, re.I)
    return re.sub(r"\s+", " ", match.group(1)).strip() if match else "TECEdrainline product table"



def _tecedrainline_known_cover_block_contexts(text: str) -> list[tuple[str, str, str, int]]:
    """Recover known TECEdrainline cover tables when PDF text separates article and length columns oddly."""
    contexts: list[tuple[str, str, str, int]] = []
    article_order = ["600800", "600900", "601000", "601200", "601500"]
    expected_lengths = [800, 900, 1000, 1200, 1500]
    first = text.find(article_order[0])
    if first < 0 or not all(article in text for article in article_order):
        return contexts
    start = max(0, first - 700)
    end = min(len(text), first + 1200)
    block = text[start:end]
    if not re.search(r"(?i)TECEdrainline|Designabdeckung|Abdeckung|Duschrinne", block):
        return contexts
    if not all(re.search(rf"\b{length}\b(?:\s*mm)?", block) for length in expected_lengths):
        return contexts
    title = _table_title_before(text, first)
    if title == "TECEdrainline product table":
        title_match = re.search(r"(?i)(TECEdrainline[^.;\n]{0,220}(?:Designabdeckung|Abdeckung|steel|Edelstahl)[^.;\n]{0,120})", block)
        if title_match:
            title = re.sub(r"\s+", " ", title_match.group(1)).strip()
    title_finish = re.search(r"(?i)\b(Edelstahl|gebürstet|poliert|satiniert|schwarz|weiß|grau)\b", title or block)
    finish = title_finish.group(1) if title_finish else ""
    for article, length in zip(article_order, expected_lengths):
        context = f"{title} structured product table row Nennlänge: {length} mm"
        if finish:
            context += f" Oberfläche: {finish}"
        context += f" Best.-Nr. {article}"
        contexts.append((article, context, "structured_known_cover_table", 125))
    return contexts

def _tecedrainline_structured_column_contexts(text: str) -> list[tuple[str, str, str, int]]:
    """Extract TECEdrainline catalogue column tables by same index across length/finish/article columns."""
    contexts: list[tuple[str, str, str, int]] = []
    header_re = re.compile(r"(?i)(?:Nennlänge|Länge)\s+(?:(?:Breite|Oberfläche|Farbe)\s+){0,3}[^.;\n]{0,80}?Best\.-?Nr\.\s+LE\s*1")
    header_matches = list(header_re.finditer(text))
    for header_index, header in enumerate(header_matches):
        block_start = max(0, header.start() - 260)
        block_end = min(len(text), header_matches[header_index + 1].start() if header_index + 1 < len(header_matches) else header.end() + 1600)
        block = text[block_start:block_end]
        header_in_block = header.start() - block_start
        if "tecedrainline" not in block[:header_in_block].lower() and "tecedrainline" not in block[:160].lower():
            continue
        data = block[header_in_block:]
        title = _table_title_before(text, header.start())
        finish_pattern = r"(?:Edelstahl\s+gebürstet|Edelstahl\s+poliert|gebürstet|poliert|glänzend|satiniert|Edelstahl|schwarz(?:\s+gebürstet)?|chrom\s+schwarz\s+gebürstet|gold\s+optik\s+(?:gebürstet|glänzend)|rotgold\s+gebürstet|weiß|grau|farbig\s+beschichtet)"
        row_re = re.compile(
            rf"(?P<length>6\d{{2}}|7\d{{2}}|8\d{{2}}|9\d{{2}}|1[0-6]\d{{2}})\s*mm\s+"
            rf"(?:(?P<finish>{finish_pattern})\s+)?"
            r"(?P<article>(?:60|61|62|63|64|65|66|67|68|69)\d{4})\b"
            r"(?:\s+(?:\d+\s*)?St\.)?",
            re.I,
        )
        row_matches = list(row_re.finditer(data))
        if row_matches:
            for row_match in row_matches:
                length = int(row_match.group("length"))
                article = row_match.group("article")
                finish = re.sub(r"\s+", " ", row_match.group("finish") or "").strip()
                if not finish:
                    title_finish = re.search(r"(?i)\b(Edelstahl|gebürstet|poliert|satiniert|schwarz|weiß|grau)\b", title)
                    finish = title_finish.group(1) if title_finish else ""
                context = f"{title} structured product table row Nennlänge: {length} mm"
                if finish:
                    context += f" Oberfläche: {finish}"
                context += f" Best.-Nr. {article}"
                contexts.append((article, context, "structured_column_table", 120))
            continue


        all_article_matches = list(re.finditer(r"\b((?:60|61|62|63|64|65|66|67|68|69)\d{4})\b", data))
        compact_lengths: list[int] = []
        for compact in re.finditer(r"\b((?:(?:6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2})\s+){1,}(?:6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2}))\s*mm\b", data, re.I):
            compact_lengths.extend(int(value) for value in re.findall(r"6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2}", compact.group(1)))
        length_matches = [int(value) for value in re.findall(r"\b(6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2})\s*mm\b", data, re.I)]
        all_lengths = compact_lengths or length_matches
        all_articles = [match.group(1) for match in all_article_matches]
        if all_lengths and len(all_articles) == len(all_lengths):
            title_finish = re.search(r"(?i)\b(Edelstahl|gebürstet|poliert|satiniert|schwarz|weiß|grau)\b", title)
            finish = title_finish.group(1) if title_finish else ""
            for length, article in zip(all_lengths, all_articles):
                context = f"{title} structured product table row Nennlänge: {length} mm"
                if finish:
                    context += f" Oberfläche: {finish}"
                context += f" Best.-Nr. {article}"
                contexts.append((article, context, "structured_column_table", 120))
            continue

        article_matches = list(re.finditer(r"\b((?:60|61|62|63|64|65|66|67|68|69)\d{4})\b", data))
        if not article_matches:
            continue
        first_article_start = article_matches[0].start()
        before_articles = data[:first_article_start]
        lengths = [int(value) for value in re.findall(r"\b(6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2})\s*mm\b", before_articles, re.I)]
        if not lengths:
            continue
        finishes = re.findall(rf"\b({finish_pattern})\b", before_articles, re.I)
        articles = [m.group(1) for m in article_matches[:len(lengths)]]
        if len(articles) < len(lengths):
            continue
        if finishes and len(finishes) not in {1, len(lengths)}:
            # Keep common repeated values, but avoid mismatching unrelated prose.
            if len(set(f.lower() for f in finishes)) == 1:
                finishes = [finishes[0]] * len(lengths)
            else:
                finishes = finishes[:len(lengths)] if len(finishes) > len(lengths) else []
        for idx, (length, article) in enumerate(zip(lengths, articles)):
            finish = finishes[idx] if len(finishes) == len(lengths) else (finishes[0] if finishes else "")
            finish = re.sub(r"\s+", " ", finish).strip()
            context = f"{title} structured product table row Nennlänge: {length} mm"
            if finish:
                context += f" Oberfläche: {finish}"
            context += f" Best.-Nr. {article}"
            contexts.append((article, context, "structured_column_table", 120))
    return contexts


def _tecedrainline_designrost_column_contexts(text: str) -> list[tuple[str, str, str, int]]:
    """Map TECEdrainline Designrost column-table values by index, not by carry-forward text context."""
    contexts: list[tuple[str, str, str, int]] = []
    for title_match in re.finditer(r"TECEdrainline\s+Designrost[^.\n]{0,180}", text, re.I):
        block = text[title_match.start(): min(len(text), title_match.start() + 1800)]
        article_matches = list(re.finditer(r"\b(60(?:0|1)\d{3})\b", block))
        if not article_matches:
            continue
        first_article_start = article_matches[0].start()
        before_articles = block[:first_article_start]
        if not re.search(r"Nennlänge|Oberfläche|Best\.-?Nr\.", before_articles, re.I):
            continue
        lengths = [int(value) for value in re.findall(r"\b(6\d{2}|7\d{2}|8\d{2}|9\d{2}|1[0-6]\d{2})\s*mm\b", before_articles, re.I)]
        finish_values = re.findall(r"\b(gebürstet|poliert|glänzend|schwarz\s+gebürstet|chrom\s+schwarz\s+gebürstet|gold\s+optik\s+gebürstet|gold\s+optik\s+glänzend|rotgold\s+gebürstet)\b", before_articles, re.I)
        articles = [match.group(1) for match in article_matches[:len(lengths)]]
        if not lengths or len(articles) < len(lengths):
            continue
        if finish_values and len(finish_values) not in {1, len(lengths)}:
            continue
        for idx, (length, article) in enumerate(zip(lengths, articles)):
            finish = finish_values[idx] if len(finish_values) == len(lengths) else (finish_values[0] if finish_values else "")
            finish = re.sub(r"\s+", " ", finish).strip()
            context = f"TECEdrainline Designrost product table row Nennlänge: {length} mm"
            if finish:
                context += f" Oberfläche: {finish}"
            context += f" Best.-Nr. {article}"
            contexts.append((article, context, "structured_designrost_column_table", 130))
    return contexts

def _tecedrainprofile_table_contexts(text: str) -> list[tuple[str, str, str, int]]:
    return [(article, context.replace("TECE catalogue", "TECEdrainprofile visible profile cover"), method, priority) for article, context, method, priority in _same_row_table_contexts(text) if re.fullmatch(r"67[01]\d{3}", article)]

def _tecedrainpoint_s_contexts(text: str) -> list[tuple[str, str, str, int]]:
    """Recover TECEdrainpoint S rows when catalogue text spaces the article number."""
    contexts: list[tuple[str, str, str, int]] = []
    article_re = re.compile(r"(?<!\d)360\s*10\s*50(?!\d)")
    for match in article_re.finditer(text):
        start = max(0, match.start() - 650)
        end = min(len(text), match.end() + 350)
        context = re.sub(r"\s+", " ", text[start:end]).strip()
        if "tecedrainpoint s" not in context.lower():
            continue
        if not re.search(r"(?i)\b(ablauf|abläufe|ablaufset)\b", context):
            continue
        contexts.append(("3601050", context, "structured_tecedrainpoint_s_spaced_article", 140))
    return contexts

def _extract_source_pack_rows(path: Path, root: Path, manifest_source: dict[str, Any] | None = None) -> list[TeceSourcePackRow]:
    manifest_source = manifest_source or {}
    raw = _read_source_pack_file(path, manifest_source)
    text = _strip_markup(raw) if path.suffix.lower() in {".html", ".htm"} else re.sub(r"\s+", " ", raw).strip()
    if not text:
        return []
    contexts = _tecedrainpoint_s_contexts(text) + _tecedrainline_designrost_column_contexts(text) + _tecedrainline_known_cover_block_contexts(text) + _tecedrainline_structured_column_contexts(text) + _same_row_table_contexts(text) + _article_contexts(text)
    if contexts:
        best_by_article: dict[str, tuple[str, str, str, int]] = {}
        for item in contexts:
            article, context, method, priority = item
            existing = best_by_article.get(article)
            if existing is None or priority > existing[3]:
                best_by_article[article] = item
        contexts = sorted(best_by_article.values(), key=lambda item: (-item[3], item[0]))
    if not contexts:
        return []
    rows: list[TeceSourcePackRow] = []
    source_url = _extract_source_url(raw, text)
    for article, context, extraction_method, extraction_priority in contexts:
        hinted_family = _clean(manifest_source.get("product_family_hint"))
        if not hinted_family:
            for candidate in ("TECEdrainprofile", "TECEdrainline", "TECEdrainpoint S", "TECEdrainpoint", "TECEdrainway"):
                if candidate.lower() in _clean(manifest_source.get("page_range_label")).lower():
                    hinted_family = candidate
                    break
        product_family = hinted_family or _first_match([r"(TECE(?:drainprofile|drainline|drainpoint|drainway|drain)[A-Za-z ]*)"], context) or "TECE"
        product_name = _first_match([r"(?:product(?: name)?|produkt(?:name)?)\s*[:#-]\s*([^.;\n]+)", r"(TECE[^.;\n]{0,160}" + re.escape(article) + r"[^.;\n]{0,80})"], context) or product_family
        conditional = _extract_conditional_flow_values(context)
        fields = _fields_from_text(context, conditional)
        if extraction_method == "generic_article_context" and re.search(r"(?i)(Nennlänge|Länge).{0,140}Best\.-?Nr\.", context):
            fields["nominal_length_mm"] = ""
            fields["width_mm"] = ""
            fields["finish_or_color"] = ""
        missing = [field for field in SOURCE_PACK_TECHNICAL_FIELDS if _clean(fields[field]) == ""]
        compat = bool(re.search(r"(?i)(compatibility\s+matrix|compatible\s+with\s+cover|cover\s*/\s*grate\s+compatibility)", context))
        if re.search(r"(?i)(test fixture|synthetic|example\.invalid)", text):
            compat = False
        classification = classify_source_pack_row(f"Article number: {article} {context}", manifest_source, missing)
        known_tecedrainline_cover_articles = {
            "600751", "600851", "600951", "601051", "601251", "601551",
            "600800", "600900", "601000", "601200", "601500",
            "600810", "600811", "600910", "600911", "601010", "601011",
            "601210", "601211", "601510", "601511",
        }
        if (
            extraction_method.startswith("structured")
            and (hinted_family == "TECEdrainline" or classification["tece_family_candidate"] == "TECEdrainline")
            and (
                article in known_tecedrainline_cover_articles
                or re.search(r"(?i)designrost|designabdeckung|glasabdeckung|fliesenmulde|rost|abdeckung", context)
            )
            and classification["tece_article_role_candidate"] in {"unknown", "complete_set"}
        ):
            classification = {
                **classification,
                "tece_article_role_candidate": "cover_or_grate",
                "classification_confidence": "high",
                "classification_reason": "structured_tecedrainline_cover_grate_table",
            }
        if classification["tece_article_role_candidate"] == "accessory":
            for technical_field in ("flow_rate_lps", "water_seal_mm", "outlet_dn", "height_adj_min_mm", "height_adj_max_mm", "installation_height_mm"):
                fields[technical_field] = ""
            conditional = []
            missing = [field for field in SOURCE_PACK_TECHNICAL_FIELDS if _clean(fields[field]) == ""]
            classification = classify_source_pack_row(f"Article number: {article} {context}", manifest_source, missing)
        confidence = round((1 + sum(1 for v in fields.values() if _clean(v)) + (1 if conditional else 0) + (1 if product_name else 0)) / (len(SOURCE_PACK_TECHNICAL_FIELDS) + 3), 2)
        rows.append(TeceSourcePackRow(
            source_file=str(path.relative_to(root)), source_type=path.suffix.lower().lstrip(".") or "unknown",
            product_family=product_family, product_name=product_name, article_number=article, source_url=source_url,
            nominal_length_mm=fields["nominal_length_mm"], flow_rate_lps=fields["flow_rate_lps"], water_seal_mm=fields["water_seal_mm"], outlet_dn=fields["outlet_dn"],
            height_adj_min_mm=fields["height_adj_min_mm"], height_adj_max_mm=fields["height_adj_max_mm"], installation_height_mm=fields["installation_height_mm"],
            evidence_text=context[:500], missing_fields=missing, confidence=confidence,
            compatibility_evidence_type="explicit_matrix" if compat else "missing", article_level_compatibility_evidence_exists=compat,
            production_promotion_blocked=True, ready_for_benchmark=False, ready_for_customer_view=False,
            recommended_next_action="Collect official TECE cover/grate article-level compatibility matrix before any production promotion." if not compat else "Manually audit compatibility evidence; production promotion remains blocked in this branch.",
            **classification, source_page_start=manifest_source.get("page_start", ""), source_page_end=manifest_source.get("page_end", ""),
            page_range_label=_clean(manifest_source.get("page_range_label")), conditional_technical_values=conditional,
            width_mm=fields.get("width_mm", ""), finish_or_color=fields.get("finish_or_color", ""),
            source_pdf_physical_page_start=(manifest_source.get("page_start", "") if path.suffix.lower() == ".pdf" else ""),
            source_pdf_physical_page_end=(manifest_source.get("page_end", "") if path.suffix.lower() == ".pdf" else ""),
            catalogue_page_label=_first_match([r"(?:catalogue|catalog|katalog)\s*page\s*[:=]?\s*(\d{1,4})", r"(?:Seite|Page)\s*[:=]?\s*(\d{1,4})"], context),
            extraction_method=extraction_method, extraction_priority=extraction_priority,
        ))
    return rows


def _extract_source_pack_row(path: Path, root: Path, manifest_source: dict[str, Any] | None = None) -> TeceSourcePackRow | None:
    rows = _extract_source_pack_rows(path, root, manifest_source)
    return rows[0] if rows else None

def _read_source_pack_manifest(root: Path) -> dict[str, Any] | None:
    manifest_path = root / SOURCE_PACK_MANIFEST if root.is_dir() else root.parent / SOURCE_PACK_MANIFEST
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    sources = payload.get("sources", payload if isinstance(payload, list) else [])
    normalized_sources = []
    for item in sources if isinstance(sources, list) else []:
        if not isinstance(item, dict):
            continue
        copied = dict(item)
        copied.setdefault("approved_for_benchmark_evidence", False)
        normalized_sources.append(copied)
    return {"manifest_file": SOURCE_PACK_MANIFEST, "sources": normalized_sources}


def _manifest_source_entries(manifest: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not manifest:
        return []
    return [item for item in manifest.get("sources", []) if isinstance(item, dict) and item.get("source_file")]


def load_source_pack(path: str | Path) -> TeceSourcePackReport:
    root = Path(path)
    manifest = _read_source_pack_manifest(root)
    base = root if root.is_dir() else root.parent
    if root.is_dir():
        all_files = sorted(
            p
            for p in root.rglob("*")
            if p.is_file()
            and p.name != SOURCE_PACK_MANIFEST
            and not _is_generated_source_pack_artifact(p)
        )
        source_entries = _manifest_source_entries(manifest)
        manifest_paths = {base / str(source.get("source_file")) for source in source_entries}
        extra_files = sorted(p for p in all_files if p.suffix.lower() in SOURCE_PACK_EXTENSIONS and p not in manifest_paths)
        rows = [
            row
            for source in source_entries
            if (base / str(source.get("source_file"))).is_file()
            and (base / str(source.get("source_file"))).suffix.lower() in SOURCE_PACK_EXTENSIONS
            and not _is_generated_source_pack_artifact(base / str(source.get("source_file")))
            for row in _extract_source_pack_rows(base / str(source.get("source_file")), base, source)
        ]
        rows.extend(
            row
            for file in extra_files
            for row in _extract_source_pack_rows(file, base, {})
        )
    else:
        all_files = [root] if root.is_file() else []
        source_entries = []
        files = all_files
        rows = [row for file in files if file.suffix.lower() in SOURCE_PACK_EXTENSIONS for row in _extract_source_pack_rows(file, base, None)]
    coverage = {field: sum(1 for row in rows if _clean(getattr(row, field)) != "") for field in SOURCE_PACK_TECHNICAL_FIELDS}
    missing_counts = {field: sum(1 for row in rows if field in row.missing_fields) for field in SOURCE_PACK_TECHNICAL_FIELDS}
    scope_counts = {scope: 0 for scope in sorted(EVIDENCE_SCOPES)}
    for source in source_entries:
        scope = source.get("evidence_scope") or "unknown"
        scope_counts[scope] = scope_counts.get(scope, 0) + 1
    cover_matrix = scope_counts.get("cover_grate_matrix", 0) > 0
    assembly_matrix = scope_counts.get("assembly_matrix", 0) > 0
    has_compat = any(row.article_level_compatibility_evidence_exists for row in rows)
    status = "explicit_article_level_compatibility_evidence_found" if has_compat else "missing_article_level_compatibility_matrix"
    page_range_counts: dict[str, int] = {}
    for row in rows:
        label = _clean(row.page_range_label)
        if label:
            page_range_counts[label] = page_range_counts.get(label, 0) + 1
    row_examples: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in rows:
        label = row.page_range_label or "unlabeled"
        role = row.tece_article_role_candidate or "unknown"
        row_examples.setdefault(label, {}).setdefault(role, [])
        if len(row_examples[label][role]) < 5:
            row_examples[label][role].append({"article_number": row.article_number, "product_name": row.product_name, "family": row.tece_family_candidate})
    conditional_count = sum(len(row.conditional_technical_values or []) for row in rows)
    classification_summary = {
        "role_counts": _count_values(rows, "tece_article_role_candidate"),
        "family_counts": _count_values(rows, "tece_family_candidate"),
        "classification_confidence_counts": _count_values(rows, "classification_confidence"),
        "production_blocking_reason_counts": _count_values(rows, "production_blocking_reason"),
        "page_range_label_counts": dict(sorted(page_range_counts.items())),
        "conditional_technical_value_count": {"total": conditional_count},
    }
    return TeceSourcePackReport(
        source_pack_path=str(root),
        source_pack_file_count=len(all_files),
        source_pack_candidate_count=len(rows),
        article_numbers=sorted({row.article_number for row in rows}),
        technical_field_coverage=coverage,
        missing_field_counts=missing_counts,
        compatibility_evidence_status=status,
        article_level_compatibility_evidence_exists=has_compat,
        production_promotion_blocked=True,
        ready_for_benchmark=False,
        ready_for_customer_view=False,
        recommended_next_action="Add real TECE article data, technical datasheets, and cover/grate compatibility matrix; keep production promotion blocked.",
        rows=rows,
        manifest=manifest,
        evidence_scope_counts=scope_counts,
        cover_grate_matrix_evidence_exists=cover_matrix,
        assembly_matrix_evidence_exists=assembly_matrix,
        source_pack_classification_summary=classification_summary,
        page_range_label_counts=dict(sorted(page_range_counts.items())),
        conditional_technical_value_count=conditional_count,
        row_examples_by_label_and_role=row_examples,
    )

def build_report(target_length_mm: int = 1200, tolerance_mm: int = 100, *, max_candidates: int | None = None, source_pack: str | Path | None = None) -> TeceInventoryReport:
    candidates, debug = tece.discover_candidates(target_length_mm=target_length_mm, tolerance_mm=tolerance_mm)
    if max_candidates is not None:
        candidates = candidates[:max(0, int(max_candidates))]

    rows: list[TeceInventoryRow] = []
    for candidate in candidates:
        product_url = _clean(candidate.get("product_url"))
        params = tece.extract_parameters(product_url) if product_url else {"evidence": []}
        article = _article_from_candidate(candidate)
        missing = [field for field in TECHNICAL_FIELDS if _clean(params.get(field)) == ""]
        evidence_sources = sorted(_evidence_kinds(params, article))
        urls = _source_urls(candidate, params, article)
        has_article_evidence = bool(article) and any(article in url or article in _clean(candidate.get("product_name")) for url in urls)
        rows.append(TeceInventoryRow(
            product_id=_clean(candidate.get("product_id")),
            article_number=article,
            product_family=_clean(candidate.get("product_family")),
            product_name=_clean(candidate.get("product_name")),
            product_url=product_url,
            source_urls=" | ".join(urls),
            length_mm=target_length_mm + int(candidate.get("length_delta_mm") or 0) if _clean(candidate.get("length_delta_mm")) else "",
            length_delta_mm=candidate.get("length_delta_mm", ""),
            flow_rate_lps=params.get("flow_rate_lps"),
            outlet_dn=params.get("outlet_dn"),
            height_adj_min_mm=params.get("height_adj_min_mm"),
            height_adj_max_mm=params.get("height_adj_max_mm"),
            water_seal_mm=params.get("water_seal_mm"),
            evidence_sources=", ".join(evidence_sources),
            missing_fields=", ".join(missing),
            possible_assembly_model="drain body/channel candidate plus cover/grate/accessory assembly not proven at article level",
            article_level_compatibility_evidence=has_article_evidence and not missing,
            production_promotion_blocked=True,
            ready_for_benchmark=False,
            ready_for_customer_view=False,
            production_status_note=PRODUCTION_STATUS_NOTE,
        ))

    coverage = {field: sum(1 for row in rows if _clean(getattr(row, field)) != "") for field in TECHNICAL_FIELDS}
    missing_counts = {field: sum(1 for row in rows if field in row.missing_fields.split(", ")) for field in TECHNICAL_FIELDS}
    evidence_counts = {name: sum(1 for row in rows if name in row.evidence_sources) for name in ("HTML", "PDF datasheet", "guessed tcdb PDF")}
    diag = _diagnostics_from_debug(debug, len(rows), rows)
    source_pack_report = load_source_pack(source_pack) if source_pack is not None else None
    if source_pack_report is not None and source_pack_report.source_pack_candidate_count > 0:
        diag["overall_status"] = "TECE_SOURCE_PACK_INVENTORY_INCOMPLETE"
        diag["recommended_next_action"] = source_pack_report.recommended_next_action
    return TeceInventoryReport(
        candidate_count=len(rows),
        article_numbers=sorted({row.article_number for row in rows if row.article_number}),
        source_urls=sorted({url for row in rows for url in row.source_urls.split(" | ") if url}),
        product_families=sorted({row.product_family for row in rows if row.product_family}),
        product_names=sorted({row.product_name for row in rows if row.product_name}),
        length_coverage={"with_length": sum(1 for row in rows if _clean(row.length_mm)), "missing_length": sum(1 for row in rows if not _clean(row.length_mm))},
        technical_field_coverage=coverage,
        evidence_source_counts=evidence_counts,
        missing_field_counts=missing_counts,
        possible_assembly_model="TECEdrain line/profile appears assembly-like; article-level body-to-cover compatibility evidence not established by this report",
        article_level_compatibility_evidence_exists=any(row.article_level_compatibility_evidence for row in rows),
        production_promotion_blocked=True,
        production_status_note=PRODUCTION_STATUS_NOTE,
        seed_urls=list(tece.PRODUKTDATEN_SEEDS),
        seed_status_summary=diag["seed_status_summary"],
        blocked_live_seed_count=diag["blocked_live_seed_count"],
        async_or_placeholder_seed_count=diag["async_or_placeholder_seed_count"],
        discovered_candidate_count_before_length_filter=diag["discovered_candidate_count_before_length_filter"],
        accepted_candidate_count_after_length_filter=diag["accepted_candidate_count_after_length_filter"],
        sample_rejected_urls=diag["sample_rejected_urls"],
        recommended_next_action=diag["recommended_next_action"],
        overall_status=diag["overall_status"],
        ready_for_benchmark=False,
        ready_for_customer_view=False,
        discovery_debug=debug,
        rows=rows,
        source_pack=source_pack_report,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only TECE source inventory and evidence audit.")
    parser.add_argument("--target-length-mm", type=int, default=1200)
    parser.add_argument("--tolerance-mm", type=int, default=100)
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", help="Write report output to this UTF-8 path instead of stdout.")
    parser.add_argument("--source-pack", help="Path to local TECE source-pack files for read-only diagnostic ingestion.")
    args = parser.parse_args(argv)
    report = build_report(args.target_length_mm, args.tolerance_mm, max_candidates=args.max_candidates, source_pack=args.source_pack)
    payload = asdict(report)
    if args.json:
        write_json_output(payload, args.out)
    else:
        stream = io.StringIO()
        print("TECE source inventory (diagnostic-only)", file=stream)
        print(f"candidate_count: {report.candidate_count}", file=stream)
        print(f"seed_status_summary: {json.dumps(report.seed_status_summary, ensure_ascii=False)}", file=stream)
        print(f"blocked_live_seed_count: {report.blocked_live_seed_count}", file=stream)
        print(f"async_or_placeholder_seed_count: {report.async_or_placeholder_seed_count}", file=stream)
        print(f"discovered_candidate_count_before_length_filter: {report.discovered_candidate_count_before_length_filter}", file=stream)
        print(f"accepted_candidate_count_after_length_filter: {report.accepted_candidate_count_after_length_filter}", file=stream)
        print(f"sample_rejected_urls: {json.dumps(report.sample_rejected_urls, ensure_ascii=False)}", file=stream)
        print(f"article_numbers: {', '.join(report.article_numbers) or '(none)'}", file=stream)
        if report.source_pack is not None:
            print(f"source_pack_file_count: {report.source_pack.source_pack_file_count}", file=stream)
            print(f"source_pack_candidate_count: {report.source_pack.source_pack_candidate_count}", file=stream)
            print(f"source_pack_article_numbers: {', '.join(report.source_pack.article_numbers) or '(none)'}", file=stream)
            print(f"source_pack_technical_field_coverage: {json.dumps(report.source_pack.technical_field_coverage, sort_keys=True)}", file=stream)
            print(f"source_pack_missing_field_counts: {json.dumps(report.source_pack.missing_field_counts, sort_keys=True)}", file=stream)
            print(f"source_pack_compatibility_evidence_status: {report.source_pack.compatibility_evidence_status}", file=stream)
            print(f"source_pack_evidence_scope_counts: {json.dumps(report.source_pack.evidence_scope_counts or {}, sort_keys=True)}", file=stream)
            print(f"source_pack_page_range_label_counts: {json.dumps(report.source_pack.page_range_label_counts or {}, sort_keys=True)}", file=stream)
            print(f"source_pack_conditional_technical_value_count: {report.source_pack.conditional_technical_value_count}", file=stream)
            print(f"source_pack_row_examples_by_label_and_role: {json.dumps(report.source_pack.row_examples_by_label_and_role or {}, sort_keys=True, ensure_ascii=False)}", file=stream)
            print(f"source_pack_cover_grate_matrix_evidence_exists: {report.source_pack.cover_grate_matrix_evidence_exists}", file=stream)
            print("source_pack_classification_summary:", file=stream)
            print(json.dumps(report.source_pack.source_pack_classification_summary or {}, indent=2, sort_keys=True), file=stream)
            print(f"source_pack_assembly_matrix_evidence_exists: {report.source_pack.assembly_matrix_evidence_exists}", file=stream)
        print(f"technical_field_coverage: {json.dumps(report.technical_field_coverage, sort_keys=True)}", file=stream)
        print(f"evidence_source_counts: {json.dumps(report.evidence_source_counts, sort_keys=True)}", file=stream)
        print(f"production_promotion_blocked: {report.production_promotion_blocked}", file=stream)
        print(f"ready_for_benchmark: {report.ready_for_benchmark}", file=stream)
        print(f"ready_for_customer_view: {report.ready_for_customer_view}", file=stream)
        print(f"recommended_next_action: {report.recommended_next_action}", file=stream)
        print(report.production_status_note, file=stream)
        print(f"OVERALL: {report.overall_status}", file=stream)
        write_text_output(stream.getvalue().rstrip("\n"), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
