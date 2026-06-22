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


def _strip_markup(text: str) -> str:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def _read_source_pack_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        try:
            from pypdf import PdfReader  # type: ignore
        except ImportError:
            try:
                from PyPDF2 import PdfReader  # type: ignore
            except ImportError:
                return ""
        reader = PdfReader(str(path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
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


def _extract_source_pack_row(path: Path, root: Path) -> TeceSourcePackRow | None:
    raw = _read_source_pack_file(path)
    text = _strip_markup(raw) if path.suffix.lower() in {".html", ".htm"} else re.sub(r"\s+", " ", raw).strip()
    if not text:
        return None
    article = _first_match([r"(?:article|artikel|item)(?:\s*(?:no\.?|number|nr\.?|#))?\s*[:#-]?\s*([0-9]{5,8})", r"\b([0-9]{6})\b"], text)
    if not article:
        return None
    product_family = _first_match([r"(TECE(?:drain|drainline|line|profile)[A-Za-z ]*)"], text) or "TECE"
    product_name = _first_match([
        r"(?:product(?: name)?|produkt(?:name)?)\s*[:#-]\s*([^.;\n]+)",
        r"(TECE[^.;\n]*" + re.escape(article) + r"[^.;\n]*)",
    ], text) or product_family
    fields = {
        "nominal_length_mm": _extract_int([r"(?:nominal\s*)?length\s*[:=]?\s*(\d{3,4})\s*mm", r"L(?:änge|ength)?\s*[:=]?\s*(\d{3,4})\s*mm"], text),
        "flow_rate_lps": _extract_float([r"flow\s*rate\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*(?:l/s|lps)", r"drainage\s*capacity\s*[:=]?\s*([0-9]+[,.]?[0-9]*)\s*(?:l/s|lps)"], text),
        "water_seal_mm": _extract_int([r"water\s*seal\s*[:=]?\s*(\d{2,3})\s*mm"], text),
        "outlet_dn": _first_match([r"outlet\s*[:=]?\s*(DN\s*\d{2,3})", r"\b(DN\s*\d{2,3})\b"], text).replace(" ", ""),
        "height_adj_min_mm": _extract_int([r"height\s*adjust(?:ment|able)?\s*[:=]?\s*(\d{2,4})\s*(?:-|to|–)\s*\d{2,4}\s*mm"], text),
        "height_adj_max_mm": _extract_int([r"height\s*adjust(?:ment|able)?\s*[:=]?\s*\d{2,4}\s*(?:-|to|–)\s*(\d{2,4})\s*mm"], text),
        "installation_height_mm": _extract_int([r"installation\s*height\s*[:=]?\s*(\d{2,4})\s*mm"], text),
    }
    missing = [field for field in SOURCE_PACK_TECHNICAL_FIELDS if _clean(fields[field]) == ""]
    compat = bool(re.search(r"(?i)(compatibility\s+matrix|compatible\s+with\s+cover|cover\s*/\s*grate\s+compatibility)", text))
    evidence = text[:500]
    confidence = round((1 + sum(1 for v in fields.values() if _clean(v)) + (1 if product_name else 0)) / (len(SOURCE_PACK_TECHNICAL_FIELDS) + 2), 2)
    return TeceSourcePackRow(
        source_file=str(path.relative_to(root)),
        source_type=path.suffix.lower().lstrip(".") or "unknown",
        product_family=product_family,
        product_name=product_name,
        article_number=article,
        source_url=_extract_source_url(raw, text),
        nominal_length_mm=fields["nominal_length_mm"],
        flow_rate_lps=fields["flow_rate_lps"],
        water_seal_mm=fields["water_seal_mm"],
        outlet_dn=fields["outlet_dn"],
        height_adj_min_mm=fields["height_adj_min_mm"],
        height_adj_max_mm=fields["height_adj_max_mm"],
        installation_height_mm=fields["installation_height_mm"],
        evidence_text=evidence,
        missing_fields=missing,
        confidence=confidence,
        compatibility_evidence_type="explicit_matrix" if compat else "missing",
        article_level_compatibility_evidence_exists=compat,
        production_promotion_blocked=True,
        ready_for_benchmark=False,
        ready_for_customer_view=False,
        recommended_next_action="Collect official TECE cover/grate article-level compatibility matrix before any production promotion." if not compat else "Manually audit compatibility evidence; production promotion remains blocked in this branch.",
    )


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


def _manifest_sources_by_file(manifest: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not manifest:
        return {}
    return {str(item.get("source_file")): item for item in manifest.get("sources", []) if item.get("source_file")}


def load_source_pack(path: str | Path) -> TeceSourcePackReport:
    root = Path(path)
    manifest = _read_source_pack_manifest(root)
    files = sorted(p for p in (root.rglob("*") if root.is_dir() else [root]) if p.is_file() and p.name != SOURCE_PACK_MANIFEST and p.suffix.lower() in SOURCE_PACK_EXTENSIONS)
    rows = [row for file in files if (row := _extract_source_pack_row(file, root if root.is_dir() else root.parent)) is not None]
    coverage = {field: sum(1 for row in rows if _clean(getattr(row, field)) != "") for field in SOURCE_PACK_TECHNICAL_FIELDS}
    missing_counts = {field: sum(1 for row in rows if field in row.missing_fields) for field in SOURCE_PACK_TECHNICAL_FIELDS}
    manifest_by_file = _manifest_sources_by_file(manifest)
    scope_counts = {scope: 0 for scope in sorted(EVIDENCE_SCOPES)}
    for source in manifest_by_file.values():
        scope = source.get("evidence_scope") or "unknown"
        scope_counts[scope] = scope_counts.get(scope, 0) + 1
    cover_matrix = scope_counts.get("cover_grate_matrix", 0) > 0
    assembly_matrix = scope_counts.get("assembly_matrix", 0) > 0
    has_compat = cover_matrix or assembly_matrix or any(row.article_level_compatibility_evidence_exists for row in rows)
    status = "explicit_article_level_compatibility_evidence_found" if has_compat else "missing_article_level_compatibility_matrix"
    return TeceSourcePackReport(
        source_pack_path=str(root),
        source_pack_file_count=len(files),
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
            print(f"source_pack_cover_grate_matrix_evidence_exists: {report.source_pack.cover_grate_matrix_evidence_exists}")
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
