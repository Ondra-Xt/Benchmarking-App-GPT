from __future__ import annotations

import argparse
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
    for candidate in ("TECEdrainprofile", "TECEdrainline", "TECEdrainpoint", "TECEdrainway"):
        if candidate.lower() in hint.lower():
            family = candidate
            break
    if family == "unknown":
        for candidate in ("TECEdrainprofile", "TECEdrainline", "TECEdrainpoint", "TECEdrainway"):
            if candidate.lower() in haystack:
                family = candidate
                break

    role = "unknown"
    reason = "no_conservative_role_keyword"
    if scope == "cover_grate_matrix":
        role, reason = "compatibility_matrix", "evidence_scope=cover_grate_matrix"
    elif scope == "assembly_matrix":
        role, reason = "assembly_matrix", "evidence_scope=assembly_matrix"
    elif re.search(r"\bchannel\s+body\b", haystack):
        role, reason = "channel_body", "keyword=channel_body"
    elif re.search(r"\b(cover|grate|abdeckung|rost)\b", haystack):
        role, reason = "cover_or_grate", "keyword=cover_or_grate"
    elif re.search(r"\b(complete\s+set|set|komplettset|komplett-set)\b", haystack):
        role, reason = "complete_set", "keyword=complete_set_candidate"
    elif scope == "technical_datasheet":
        role, reason = "technical_datasheet_only", "evidence_scope=technical_datasheet"
    elif re.search(r"\b(drain\s+body|ablauf|abläufe)\b", haystack):
        role, reason = "drain_body", "keyword=drain_body_or_ablauf"

    confidence = "low"
    if role != "unknown" and family != "unknown":
        confidence = "high"
    elif role != "unknown" or family != "unknown":
        confidence = "medium"

    blocking = ["missing_article_level_compatibility_matrix"]
    if role in {"cover_or_grate", "channel_body", "drain_body"}:
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


def _article_contexts(text: str) -> list[tuple[str, str]]:
    matches = list(re.finditer(r"(?i)(?:Best\.-?Nr\.?|Artikel(?:\s*(?:Nr\.?|nummer))?|Article(?:\s*(?:no\.?|number))?)\s*[:#-]?\s*([0-9]{5,8})|\b([0-9]{6})\b", text))
    contexts: list[tuple[str, str]] = []
    seen: set[str] = set()
    for idx, match in enumerate(matches):
        article = match.group(1) or match.group(2)
        if not article or article in seen:
            continue
        seen.add(article)
        left = max(0, match.start() - 350)
        right = min(len(text), (matches[idx + 1].start() + 120) if idx + 1 < len(matches) else match.end() + 700)
        contexts.append((article, text[left:right]))
    return contexts


def _fields_from_text(text: str, conditional_values: list[dict[str, Any]]) -> dict[str, Any]:
    unconditional_flow = _extract_float([
        r"flow\s*rate\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*(?:l/s|lps)(?![^.\n]{0,80}(?:10|20)\s*mm\s*Aufstau)",
        r"drainage\s*capacity\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*(?:l/s|lps)(?![^.\n]{0,80}(?:10|20)\s*mm\s*Aufstau)",
        r"Ablaufleistung\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*l/s(?![^.\n]{0,80}(?:10|20)\s*mm\s*Aufstau)",
    ], text)
    return {
        "nominal_length_mm": _extract_int([r"(?:nominal\s*)?length\s*[:=]?\s*(\d{3,4})\s*mm", r"(?:Nennlänge|Länge|Length)\s*[:=]?\s*(\d{3,4})\s*mm", r"\b(7\d{2}|8\d{2}|9\d{2}|1[0-5]\d{2})\s*mm\b"], text),
        "flow_rate_lps": "" if conditional_values else unconditional_flow,
        "water_seal_mm": _extract_int([r"(?:reduzierte\s*)?Sperrwasserhöhe\s*[:=]?\s*(\d{2,3})\s*mm", r"water\s*seal\s*[:=]?\s*(\d{2,3})\s*mm"], text),
        "outlet_dn": _first_match([r"(?:outlet\s*[:=]?\s*)?(DN\s*\d{2,3})\b"], text).replace(" ", ""),
        "height_adj_min_mm": _extract_int([r"height\s*adjust(?:ment|able)?\s*[:=]?\s*(\d{2,4})\s*(?:-|to|–)\s*\d{2,4}\s*mm"], text),
        "height_adj_max_mm": _extract_int([r"height\s*adjust(?:ment|able)?\s*[:=]?\s*\d{2,4}\s*(?:-|to|–)\s*(\d{2,4}(?:[,.]\d+)?)\s*mm"], text),
        "installation_height_mm": _extract_int([r"(?:min\.\s*)?(?:Aufbauhöhe|installation\s*height)\s*[:=]?\s*(\d{2,4}(?:[,.]\d+)?)\s*mm"], text),
    }


def _extract_source_pack_rows(path: Path, root: Path, manifest_source: dict[str, Any] | None = None) -> list[TeceSourcePackRow]:
    manifest_source = manifest_source or {}
    raw = _read_source_pack_file(path, manifest_source)
    text = _strip_markup(raw) if path.suffix.lower() in {".html", ".htm"} else re.sub(r"\s+", " ", raw).strip()
    if not text:
        return []
    contexts = _article_contexts(text)
    if not contexts:
        return []
    rows: list[TeceSourcePackRow] = []
    source_url = _extract_source_url(raw, text)
    for article, context in contexts:
        hinted_family = _clean(manifest_source.get("product_family_hint"))
        if not hinted_family:
            for candidate in ("TECEdrainprofile", "TECEdrainline", "TECEdrainpoint", "TECEdrainway"):
                if candidate.lower() in _clean(manifest_source.get("page_range_label")).lower():
                    hinted_family = candidate
                    break
        product_family = hinted_family or _first_match([r"(TECE(?:drainprofile|drainline|drainpoint|drainway|drain)[A-Za-z ]*)"], context) or "TECE"
        product_name = _first_match([r"(?:product(?: name)?|produkt(?:name)?)\s*[:#-]\s*([^.;\n]+)", r"(TECE[^.;\n]{0,160}" + re.escape(article) + r"[^.;\n]{0,80})"], context) or product_family
        conditional = _extract_conditional_flow_values(context)
        fields = _fields_from_text(context, conditional)
        missing = [field for field in SOURCE_PACK_TECHNICAL_FIELDS if _clean(fields[field]) == ""]
        compat = bool(re.search(r"(?i)(compatibility\s+matrix|compatible\s+with\s+cover|cover\s*/\s*grate\s+compatibility)", context))
        if re.search(r"(?i)(test fixture|synthetic|example\.invalid)", text):
            compat = False
        classification = classify_source_pack_row(context, manifest_source, missing)
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
        all_files = sorted(p for p in root.rglob("*") if p.is_file() and p.name != SOURCE_PACK_MANIFEST and p.name not in GENERATED_SOURCE_PACK_OUTPUTS)
        source_entries = _manifest_source_entries(manifest)
        files = [base / str(source.get("source_file")) for source in source_entries]
        rows = [
            row
            for source in source_entries
            if (base / str(source.get("source_file"))).is_file()
            and (base / str(source.get("source_file"))).suffix.lower() in SOURCE_PACK_EXTENSIONS
            for row in _extract_source_pack_rows(base / str(source.get("source_file")), base, source)
        ]
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
    parser.add_argument("--source-pack", help="Path to local TECE source-pack files for read-only diagnostic ingestion.")
    args = parser.parse_args(argv)
    report = build_report(args.target_length_mm, args.tolerance_mm, max_candidates=args.max_candidates, source_pack=args.source_pack)
    payload = asdict(report)
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print("TECE source inventory (diagnostic-only)")
        print(f"candidate_count: {report.candidate_count}")
        print(f"seed_status_summary: {json.dumps(report.seed_status_summary, ensure_ascii=False)}")
        print(f"blocked_live_seed_count: {report.blocked_live_seed_count}")
        print(f"async_or_placeholder_seed_count: {report.async_or_placeholder_seed_count}")
        print(f"discovered_candidate_count_before_length_filter: {report.discovered_candidate_count_before_length_filter}")
        print(f"accepted_candidate_count_after_length_filter: {report.accepted_candidate_count_after_length_filter}")
        print(f"sample_rejected_urls: {json.dumps(report.sample_rejected_urls, ensure_ascii=False)}")
        print(f"article_numbers: {', '.join(report.article_numbers) or '(none)'}")
        if report.source_pack is not None:
            print(f"source_pack_file_count: {report.source_pack.source_pack_file_count}")
            print(f"source_pack_candidate_count: {report.source_pack.source_pack_candidate_count}")
            print(f"source_pack_article_numbers: {', '.join(report.source_pack.article_numbers) or '(none)'}")
            print(f"source_pack_technical_field_coverage: {json.dumps(report.source_pack.technical_field_coverage, sort_keys=True)}")
            print(f"source_pack_missing_field_counts: {json.dumps(report.source_pack.missing_field_counts, sort_keys=True)}")
            print(f"source_pack_compatibility_evidence_status: {report.source_pack.compatibility_evidence_status}")
            print(f"source_pack_evidence_scope_counts: {json.dumps(report.source_pack.evidence_scope_counts or {}, sort_keys=True)}")
            print(f"source_pack_page_range_label_counts: {json.dumps(report.source_pack.page_range_label_counts or {}, sort_keys=True)}")
            print(f"source_pack_conditional_technical_value_count: {report.source_pack.conditional_technical_value_count}")
            print(f"source_pack_row_examples_by_label_and_role: {json.dumps(report.source_pack.row_examples_by_label_and_role or {}, sort_keys=True, ensure_ascii=False)}")
            print(f"source_pack_cover_grate_matrix_evidence_exists: {report.source_pack.cover_grate_matrix_evidence_exists}")
            print("source_pack_classification_summary:")
            print(json.dumps(report.source_pack.source_pack_classification_summary or {}, indent=2, sort_keys=True))
            print(f"source_pack_assembly_matrix_evidence_exists: {report.source_pack.assembly_matrix_evidence_exists}")
        print(f"technical_field_coverage: {json.dumps(report.technical_field_coverage, sort_keys=True)}")
        print(f"evidence_source_counts: {json.dumps(report.evidence_source_counts, sort_keys=True)}")
        print(f"production_promotion_blocked: {report.production_promotion_blocked}")
        print(f"ready_for_benchmark: {report.ready_for_benchmark}")
        print(f"ready_for_customer_view: {report.ready_for_customer_view}")
        print(f"recommended_next_action: {report.recommended_next_action}")
        print(report.production_status_note)
        print(f"OVERALL: {report.overall_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
