from __future__ import annotations

import json
import shutil
from pathlib import Path

from tools.validate_tece_source_pack import main, validate_source_pack

FIXTURE = Path("tests/fixtures/tece/source_pack")


def _copy_fixture(tmp_path: Path) -> Path:
    dst = tmp_path / "source_pack"
    shutil.copytree(FIXTURE, dst)
    return dst


def _write_manifest(root: Path, sources: list[dict]) -> None:
    (root / "tece_source_pack_manifest.json").write_text(json.dumps({"sources": sources}, indent=2), encoding="utf-8")


def _sources(root: Path) -> list[dict]:
    return json.loads((root / "tece_source_pack_manifest.json").read_text(encoding="utf-8"))["sources"]


def test_valid_manifest_passes(tmp_path):
    root = _copy_fixture(tmp_path)
    result = validate_source_pack(root)
    assert result.valid is True
    assert result.production_promotion_blocked is True
    assert result.manifest_file_count >= 2


def test_missing_file_fails(tmp_path):
    root = _copy_fixture(tmp_path)
    (root / "tece_test_notes.txt").unlink()
    result = validate_source_pack(root)
    assert result.valid is False
    assert any("listed source file missing: tece_test_notes.txt" in e for e in result.errors)


def test_unsupported_extension_fails(tmp_path):
    root = _copy_fixture(tmp_path)
    bad = root / "tece_bad.exe"
    bad.write_text("not allowed", encoding="utf-8")
    sources = _sources(root)
    sources.append({
        "source_file": "tece_bad.exe", "source_type": "exe", "source_origin": "unknown",
        "evidence_scope": "article_data", "approved_for_benchmark_evidence": False,
    })
    _write_manifest(root, sources)
    result = validate_source_pack(root)
    assert result.valid is False
    assert any("unsupported extension" in e and "tece_bad.exe" in e for e in result.errors)


def test_unknown_unlisted_file_is_reported(tmp_path):
    root = _copy_fixture(tmp_path)
    (root / "unlisted.txt").write_text("unknown", encoding="utf-8")
    result = validate_source_pack(root)
    assert result.valid is False
    assert "unlisted.txt" in result.unknown_files
    assert any("unknown unlisted file: unlisted.txt" in e for e in result.errors)


def test_missing_evidence_scope_fails_clearly(tmp_path):
    root = _copy_fixture(tmp_path)
    sources = _sources(root)
    sources[0].pop("evidence_scope")
    _write_manifest(root, sources)
    result = validate_source_pack(root)
    assert result.valid is False
    assert any("missing evidence_scope" in e for e in result.errors)


def test_approved_for_benchmark_evidence_defaults_false_in_report(tmp_path):
    root = _copy_fixture(tmp_path)
    sources = _sources(root)
    sources[0].pop("approved_for_benchmark_evidence")
    _write_manifest(root, sources)
    result = validate_source_pack(root)
    assert result.valid is False
    assert any("approved_for_benchmark_evidence must be false" in e for e in result.errors)


def test_production_promotion_markers_fail(tmp_path):
    root = _copy_fixture(tmp_path)
    sources = _sources(root)
    sources[0]["ready_for_benchmark"] = True
    _write_manifest(root, sources)
    result = validate_source_pack(root)
    assert result.valid is False
    assert result.production_promotion_blocked is True
    assert any("readiness flag" in e for e in result.errors)


def test_cli_returns_success_for_fixture(capsys):
    assert main(["--source-pack", str(FIXTURE)]) == 0
    assert "PASS" in capsys.readouterr().out


def test_pdf_page_range_is_accepted_and_counted(tmp_path):
    root = tmp_path
    (root / "catalog.pdf").write_bytes(b"%PDF-1.4\n% synthetic placeholder")
    _write_manifest(root, [{
        "source_file": "catalog.pdf",
        "source_type": "pdf",
        "source_origin": "manual_download",
        "evidence_scope": "article_data",
        "page_start": 2,
        "page_end": 4,
        "page_range_label": "TECEdrainline drainage section",
        "approved_for_benchmark_evidence": False,
    }])

    result = validate_source_pack(root)

    assert result.valid is True
    assert result.page_range_label_counts == {"TECEdrainline drainage section": 1}


def test_invalid_page_range_fails(tmp_path):
    root = tmp_path
    (root / "catalog.pdf").write_bytes(b"%PDF-1.4\n% synthetic placeholder")
    _write_manifest(root, [{
        "source_file": "catalog.pdf",
        "source_type": "pdf",
        "source_origin": "manual_download",
        "evidence_scope": "article_data",
        "page_start": 10,
        "page_end": 4,
        "approved_for_benchmark_evidence": False,
    }])

    result = validate_source_pack(root)

    assert result.valid is False
    assert any("page_start must be <= page_end" in error for error in result.errors)


def test_page_range_for_non_pdf_fails_clearly(tmp_path):
    root = tmp_path
    (root / "notes.txt").write_text("Article number: 600100", encoding="utf-8")
    _write_manifest(root, [{
        "source_file": "notes.txt",
        "source_type": "txt",
        "source_origin": "unknown",
        "evidence_scope": "article_data",
        "page_start": 1,
        "page_end": 2,
        "approved_for_benchmark_evidence": False,
    }])

    result = validate_source_pack(root)

    assert result.valid is False
    assert any("page ranges are only supported for PDF sources" in error for error in result.errors)
