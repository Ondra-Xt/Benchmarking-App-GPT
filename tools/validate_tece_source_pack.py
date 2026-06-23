from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import EVIDENCE_SCOPES, SOURCE_PACK_EXTENSIONS, SOURCE_PACK_MANIFEST

SOURCE_ORIGINS = {"public_url", "manual_download", "supplier_export", "unknown"}
REQUIRED_FIELDS = {"source_file", "source_type", "evidence_scope"}
PRODUCTION_NAMES = {"Products", "Comparison", "BOM_Options", "Final_Assemblies", "Final_Set_Details"}

@dataclass(frozen=True)
class ValidationResult:
    source_pack_path: str
    manifest_path: str
    valid: bool
    errors: list[str]
    warnings: list[str]
    manifest_file_count: int
    filesystem_file_count: int
    unknown_files: list[str]
    evidence_scope_counts: dict[str, int]
    cover_grate_matrix_evidence_exists: bool
    assembly_matrix_evidence_exists: bool
    production_promotion_blocked: bool = True
    page_range_label_counts: dict[str, int] | None = None


def _load_manifest(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload, []
    if isinstance(payload, dict) and isinstance(payload.get("sources"), list):
        return payload["sources"], []
    return [], ["manifest must be a JSON list or an object with a 'sources' list"]


def validate_source_pack(source_pack: str | Path) -> ValidationResult:
    root = Path(source_pack)
    errors: list[str] = []
    warnings: list[str] = []
    manifest_path = root / SOURCE_PACK_MANIFEST
    if not root.exists():
        errors.append(f"source pack path does not exist: {root}")
    if not manifest_path.exists():
        errors.append(f"manifest missing: {SOURCE_PACK_MANIFEST}")
        sources: list[dict[str, Any]] = []
    else:
        try:
            sources, load_errors = _load_manifest(manifest_path)
            errors.extend(load_errors)
        except json.JSONDecodeError as exc:
            sources = []
            errors.append(f"manifest is not valid JSON: {exc}")

    listed: set[str] = set()
    scope_counts = {scope: 0 for scope in sorted(EVIDENCE_SCOPES)}
    page_range_label_counts: dict[str, int] = {}
    for idx, item in enumerate(sources):
        prefix = f"sources[{idx}]"
        if not isinstance(item, dict):
            errors.append(f"{prefix} must be an object")
            continue
        missing = sorted(REQUIRED_FIELDS - set(item))
        if missing:
            errors.append(f"{prefix} missing required fields: {', '.join(missing)}")
        source_file = str(item.get("source_file", ""))
        if source_file:
            listed.add(source_file)
            source_path = root / source_file
            if not source_path.exists():
                errors.append(f"listed source file missing: {source_file}")
            if source_path.suffix.lower() not in SOURCE_PACK_EXTENSIONS:
                errors.append(f"unsupported extension for {source_file}: {source_path.suffix.lower() or '(none)'}")
            if source_path.name == SOURCE_PACK_MANIFEST:
                errors.append("manifest cannot list itself as a source file")
        source_type = item.get("source_type")
        if not source_type:
            errors.append(f"{prefix} missing source_type")
        scope = item.get("evidence_scope")
        if not scope:
            errors.append(f"{prefix} missing evidence_scope")
            scope = "unknown"
        elif scope not in EVIDENCE_SCOPES:
            errors.append(f"{prefix} has unsupported evidence_scope: {scope}")
        scope_counts[str(scope)] = scope_counts.get(str(scope), 0) + 1
        origin = item.get("source_origin", "unknown")
        if origin not in SOURCE_ORIGINS:
            errors.append(f"{prefix} has unsupported source_origin: {origin}")
        if item.get("approved_for_benchmark_evidence") is not False:
            errors.append(f"{prefix} approved_for_benchmark_evidence must be false for intake")
        if any(item.get(flag) is True for flag in ("ready_for_benchmark", "ready_for_customer_view", "production_promoted")):
            errors.append(f"{prefix} contains production promotion/readiness flag")
        lower_file = source_file.lower()
        if any(name.lower() in lower_file for name in PRODUCTION_NAMES):
            errors.append(f"{prefix} appears to target production export: {source_file}")
        has_page_start = "page_start" in item
        has_page_end = "page_end" in item
        page_start = item.get("page_start")
        page_end = item.get("page_end")
        if has_page_start or has_page_end:
            if not (has_page_start and has_page_end):
                errors.append(f"{prefix} page range requires both page_start and page_end")
            if has_page_start and not isinstance(page_start, int):
                errors.append(f"{prefix} page_start must be an integer when provided")
            if has_page_end and not isinstance(page_end, int):
                errors.append(f"{prefix} page_end must be an integer when provided")
            if isinstance(page_start, int) and isinstance(page_end, int) and page_start > page_end:
                errors.append(f"{prefix} page_start must be <= page_end")
            declared_type = str(source_type or "").lower()
            if not (declared_type == "pdf" and lower_file.endswith(".pdf")):
                errors.append(f"{prefix} page ranges are only supported for PDF sources: {source_file}")
        label = item.get("page_range_label")
        if label is not None:
            if not isinstance(label, str) or not label.strip():
                errors.append(f"{prefix} page_range_label must be a non-empty string when provided")
            else:
                page_range_label_counts[label.strip()] = page_range_label_counts.get(label.strip(), 0) + 1
    if sources and not (scope_counts.get("cover_grate_matrix", 0) or scope_counts.get("assembly_matrix", 0)):
        warnings.append("no compatibility matrix evidence identified; TECE promotion remains blocked")

    filesystem_files = []
    if root.exists():
        filesystem_files = [p for p in root.rglob("*") if p.is_file() and p.name != SOURCE_PACK_MANIFEST]
        for file in filesystem_files:
            rel = str(file.relative_to(root))
            if rel not in listed:
                errors.append(f"unknown unlisted file: {rel}")
            if file.suffix.lower() not in SOURCE_PACK_EXTENSIONS:
                errors.append(f"unsupported extension for {rel}: {file.suffix.lower() or '(none)'}")
    unknown_files = sorted(str(p.relative_to(root)) for p in filesystem_files if str(p.relative_to(root)) not in listed) if root.exists() else []
    return ValidationResult(
        source_pack_path=str(root), manifest_path=str(manifest_path), valid=not errors,
        errors=errors, warnings=warnings, manifest_file_count=len(sources), filesystem_file_count=len(filesystem_files),
        unknown_files=unknown_files, evidence_scope_counts=scope_counts,
        cover_grate_matrix_evidence_exists=scope_counts.get("cover_grate_matrix", 0) > 0,
        assembly_matrix_evidence_exists=scope_counts.get("assembly_matrix", 0) > 0,
        page_range_label_counts=dict(sorted(page_range_label_counts.items())),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate diagnostic-only TECE source-pack intake manifests.")
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = validate_source_pack(args.source_pack)
    if args.json:
        print(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    else:
        print(f"TECE source-pack validation: {'PASS' if result.valid else 'FAIL'}")
        for error in result.errors:
            print(f"ERROR: {error}")
        for warning in result.warnings:
            print(f"WARNING: {warning}")
        print(f"page_range_label_counts: {json.dumps(result.page_range_label_counts or {}, sort_keys=True)}")
        print(f"production_promotion_blocked: {result.production_promotion_blocked}")
    return 0 if result.valid else 1

if __name__ == "__main__":
    raise SystemExit(main())
