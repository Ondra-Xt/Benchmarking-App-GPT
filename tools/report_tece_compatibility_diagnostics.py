from __future__ import annotations

import argparse
import io
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import TeceSourcePackRow, load_source_pack
from tools.tece_report_output import write_json_output, write_text_output

BODY_ROLES = {"channel_body", "drain_body", "complete_set", "technical_datasheet_only"}
COVER_ROLES = {"cover_or_grate"}
HIGH_CONFIDENCE = {"explicit_article_level_matrix", "explicit_text_pairing"}


@dataclass(frozen=True)
class TeceCompatibilityPairing:
    family: str
    body_or_drain_article: str
    cover_grate_plate_article: str
    nominal_length_mm: Any
    evidence_type: str
    evidence_level: str
    role_pair: str
    evidence_confidence: str
    diagnostic_only: bool
    production_safe: bool
    source_file: str
    source_page_start: Any
    source_page_end: Any
    page_range_label: str
    evidence_text_or_reason: str
    production_promotion_blocked: bool = True
    ready_for_benchmark: bool = False
    ready_for_customer_view: bool = False
    requires_manual_review: bool = True
    why_not_production_safe: str = "TECE production promotion is blocked pending manual review of explicit article-level compatibility evidence and production criteria; diagnostic candidates are not production-safe."


@dataclass(frozen=True)
class TeceCompatibilityFamilyDiagnostic:
    product_family: str
    candidate_body_channel_drain_articles: list[dict[str, Any]]
    candidate_cover_grate_plate_articles: list[dict[str, Any]]
    complete_set_articles: list[dict[str, Any]]
    possible_pairings: list[TeceCompatibilityPairing]
    production_promotion_blocked: bool = True
    ready_for_benchmark: bool = False
    ready_for_customer_view: bool = False
    requires_manual_review: bool = True
    why_not_production_safe: str = "TECE production promotion is blocked pending manual review of explicit article-level compatibility evidence and production criteria; diagnostic candidates are not production-safe."


@dataclass(frozen=True)
class TeceCompatibilityDiagnosticsReport:
    source_pack_path: str
    compatibility_diagnostic_available: bool
    compatibility_candidate_count: int
    evidence_level_counts: dict[str, int]
    evidence_type_counts: dict[str, int]
    evidence_confidence_counts: dict[str, int]
    production_safe_candidate_count: int
    diagnostic_only_candidate_count: int
    family_candidate_counts: dict[str, int]
    family_evidence_level_counts: dict[str, dict[str, int]]
    explicit_article_level_compatibility_evidence_exists: bool
    production_promotion_blocked: bool
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    families: list[TeceCompatibilityFamilyDiagnostic]


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _family(row: TeceSourcePackRow) -> str:
    return _clean(row.tece_family_candidate) or _clean(row.product_family) or "unknown"


def _row_dict(row: TeceSourcePackRow) -> dict[str, Any]:
    return {
        "article_number": row.article_number,
        "role_candidate": row.tece_article_role_candidate,
        "nominal_length_mm": row.nominal_length_mm,
        "product_name": row.product_name,
        "source_file": row.source_file,
        "source_page_start": row.source_page_start,
        "source_page_end": row.source_page_end,
        "page_range_label": row.page_range_label,
        "conditional_technical_values": row.conditional_technical_values or [],
    }


def _role_bucket(row: TeceSourcePackRow) -> str:
    role = row.tece_article_role_candidate
    haystack = f"{row.product_name} {row.evidence_text} {row.article_number}".lower()
    article = _clean(row.article_number)
    if article in {"650000", "650001", "650002", "650003", "650004", "673001", "673002", "673003"}:
        return "body"
    if re.match(r"60[01]\d{3}$", article) or re.match(r"67[01]\d{3}$", article):
        return "cover"
    if role in BODY_ROLES:
        return "body"
    if role in COVER_ROLES or re.search(r"\b(cover|grate|abdeckung|rost|plate|designrost)\b", haystack):
        return "cover"
    if role == "complete_set" or re.search(r"\b(complete set|komplettset|set)\b", haystack):
        return "set"
    if re.match(r"67[013]\d{3}$", article):
        return "body" if article in {"673001", "673002", "673003"} else "cover"
    if re.match(r"60[01]\d{3}$", article) or re.match(r"65\d{4}$", article):
        return "cover" if "65000" not in article else "body"
    return "body" if role != "unknown" else "unknown"


EXPLICIT_TEXT_SIGNALS = (
    r"passend\s+zu",
    r"kombinierbar\s+mit",
    r"bestehend\s+aus",
    r"für\s+[^.!?;]{0,80}\bRinne\b",
    r"für\s+[^.!?;]{0,80}\bAblauf\b",
    r"suitable\s+for",
    r"compatible\s+with",
    r"consists\s+of",
)
_EXPLICIT_TEXT_SIGNAL_RE = re.compile("(?i)(" + "|".join(EXPLICIT_TEXT_SIGNALS) + ")")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?;])\s+")


def _sentences(text: str) -> list[str]:
    return [part.strip() for part in _SENTENCE_SPLIT_RE.split(text or "") if part.strip()]


def _context_has_pairing(row: TeceSourcePackRow, a: str, b: str) -> bool:
    return any(a in sentence and b in sentence and _EXPLICIT_TEXT_SIGNAL_RE.search(sentence) for sentence in _sentences(row.evidence_text or ""))


def _context_has_matrix_pairing(row: TeceSourcePackRow, a: str, b: str) -> bool:
    text = row.evidence_text or ""
    if a not in text or b not in text:
        return False
    return bool(re.search(r"(?i)(matrix|table|tabelle)", text)) and _context_has_pairing(row, a, b)


def _pairing_for(body: TeceSourcePackRow, cover: TeceSourcePackRow) -> TeceCompatibilityPairing:
    rows = [body, cover]
    evidence_type = "insufficient_evidence"
    reason = "No explicit article-level pairing text or reliable matrix evidence found."
    source = body
    if any(r.compatibility_evidence_type == "explicit_matrix" and _context_has_matrix_pairing(r, body.article_number, cover.article_number) for r in rows):
        evidence_type = "explicit_article_level_matrix"
        source = next(r for r in rows if _context_has_matrix_pairing(r, body.article_number, cover.article_number))
        reason = source.evidence_text
    elif any(_context_has_pairing(r, body.article_number, cover.article_number) for r in rows):
        evidence_type = "explicit_text_pairing"
        source = next(r for r in rows if _context_has_pairing(r, body.article_number, cover.article_number))
        reason = source.evidence_text
    elif body.page_range_label and body.page_range_label == cover.page_range_label and _family(body) == _family(cover):
        evidence_type = "explicit_section_pairing"
        source = body
        reason = "Articles appear in the same TECE family catalogue section/page range only; diagnostic proposal, not production evidence."
    if _clean(body.nominal_length_mm) and _clean(body.nominal_length_mm) == _clean(cover.nominal_length_mm) and _family(body) == _family(cover) and evidence_type in {"insufficient_evidence", "explicit_section_pairing"}:
        evidence_type = "same_length_same_family_candidate"
        source = body
        reason = "Same nominal length and family candidate only; this is not production compatibility evidence."
    confidence = "high" if evidence_type in HIGH_CONFIDENCE else ("medium" if evidence_type == "explicit_section_pairing" else "low")
    return TeceCompatibilityPairing(
        family=_family(body), body_or_drain_article=body.article_number, cover_grate_plate_article=cover.article_number,
        nominal_length_mm=body.nominal_length_mm or cover.nominal_length_mm, evidence_type=evidence_type,
        evidence_level=evidence_type, role_pair=f"{body.tece_article_role_candidate or _role_bucket(body)}_to_{cover.tece_article_role_candidate or _role_bucket(cover)}",
        evidence_confidence=confidence, diagnostic_only=True, production_safe=False,
        source_file=source.source_file, source_page_start=source.source_page_start, source_page_end=source.source_page_end,
        page_range_label=source.page_range_label, evidence_text_or_reason=reason,
    )



def _sorted_counts(values: list[str]) -> dict[str, int]:
    return dict(sorted(Counter(values).items()))


def _family_pairing_counts(
    family_reports: list[TeceCompatibilityFamilyDiagnostic],
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    family_candidate_counts: dict[str, int] = {}
    family_evidence_level_counts: dict[str, dict[str, int]] = {}
    for family_report in family_reports:
        pairings = family_report.possible_pairings
        family_candidate_counts[family_report.product_family] = len(pairings)
        family_evidence_level_counts[family_report.product_family] = _sorted_counts([p.evidence_type for p in pairings])
    return dict(sorted(family_candidate_counts.items())), dict(sorted(family_evidence_level_counts.items()))

def build_compatibility_diagnostics_report(source_pack: str | Path) -> TeceCompatibilityDiagnosticsReport:
    report = load_source_pack(source_pack)
    families = sorted({_family(row) for row in report.rows})
    family_reports: list[TeceCompatibilityFamilyDiagnostic] = []
    all_pairings: list[TeceCompatibilityPairing] = []
    for family in families:
        rows = [row for row in report.rows if _family(row) == family]
        bodies = [row for row in rows if _role_bucket(row) == "body"]
        covers = [row for row in rows if _role_bucket(row) == "cover"]
        sets = [row for row in rows if _role_bucket(row) == "set"]
        pairings = [_pairing_for(body, cover) for body in bodies for cover in covers]
        all_pairings.extend(pairings)
        family_reports.append(TeceCompatibilityFamilyDiagnostic(
            product_family=family,
            candidate_body_channel_drain_articles=[_row_dict(row) for row in bodies],
            candidate_cover_grate_plate_articles=[_row_dict(row) for row in covers],
            complete_set_articles=[_row_dict(row) for row in sets],
            possible_pairings=pairings,
        ))
    explicit = any(p.evidence_type in HIGH_CONFIDENCE for p in all_pairings)
    evidence_level_counts = _sorted_counts([p.evidence_type for p in all_pairings])
    evidence_confidence_counts = _sorted_counts([p.evidence_confidence for p in all_pairings])
    family_candidate_counts, family_evidence_level_counts = _family_pairing_counts(family_reports)
    return TeceCompatibilityDiagnosticsReport(
        source_pack_path=report.source_pack_path,
        compatibility_diagnostic_available=bool(report.rows),
        compatibility_candidate_count=len(all_pairings),
        evidence_level_counts=evidence_level_counts,
        evidence_type_counts=evidence_level_counts,
        evidence_confidence_counts=evidence_confidence_counts,
        production_safe_candidate_count=sum(1 for p in all_pairings if p.production_safe),
        diagnostic_only_candidate_count=sum(1 for p in all_pairings if p.diagnostic_only),
        family_candidate_counts=family_candidate_counts,
        family_evidence_level_counts=family_evidence_level_counts,
        explicit_article_level_compatibility_evidence_exists=explicit,
        production_promotion_blocked=True,
        ready_for_benchmark=False,
        ready_for_customer_view=False,
        families=family_reports,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnostic-only TECE compatibility/assembly report.")
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    report = build_compatibility_diagnostics_report(args.source_pack)
    payload = asdict(report)
    if args.json:
        write_json_output(payload, args.out)
    else:
        stream = io.StringIO()
        print("TECE compatibility diagnostics (diagnostic-only; production promotion blocked)", file=stream)
        for key in (
            "compatibility_candidate_count",
            "evidence_level_counts",
            "production_safe_candidate_count",
            "diagnostic_only_candidate_count",
            "family_evidence_level_counts",
            "production_promotion_blocked",
            "ready_for_benchmark",
            "ready_for_customer_view",
        ):
            print(f"{key}: {json.dumps(payload[key], ensure_ascii=False, sort_keys=True)}", file=stream)
        write_text_output(stream.getvalue().rstrip("\n"), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
