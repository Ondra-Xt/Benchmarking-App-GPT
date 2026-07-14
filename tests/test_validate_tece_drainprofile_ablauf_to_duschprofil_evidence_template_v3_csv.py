from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import validate_tece_drainprofile_ablauf_to_duschprofil_evidence_template_v3_csv as mod


class Row(SimpleNamespace):
    pass


def _source_rows(role_counts=None):
    role_counts = role_counts or mod.EXPECTED_MACHINE_ROLE_COUNTS
    roles = []
    for role, count in role_counts.items():
        roles += [role] * count
    articles = mod.RETAINED_DRAIN_BODY_ARTICLES + mod.INSTALLABLE_DUSCHPROFIL_ARTICLES + mod.FORBIDDEN_SPARE_COVER_ARTICLES
    rows = [
        Row(
            tece_family_candidate=mod.EXPECTED_FAMILY,
            product_family=mod.EXPECTED_FAMILY,
            tece_article_role_candidate=role,
            article_number=articles[i] if i < len(articles) else f"67{i:04d}",
        )
        for i, role in enumerate(roles)
    ]
    rows += [
        Row(tece_family_candidate="TECEdrainline", product_family="TECEdrainline", tece_article_role_candidate="unknown", article_number=f"60{i:04d}")
        for i in range(436 - len(rows))
    ]
    return rows


@pytest.fixture
def fake_source_pack(monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=_source_rows()))


@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    sp = tmp_path / "source_pack"
    sp.mkdir()
    (sp / "keep.txt").write_text("unchanged", encoding="utf-8")
    csv_path = tmp_path / "tecedrainprofile_ablauf_to_duschprofil_evidence_template_v3.csv"
    _write_csv(csv_path, _valid_rows())
    return csv_path, sp


def _valid_rows():
    rows = []
    areas = ["ablauf_to_duschprofil_interface_statement"] * 3 + ["duschprofil_installable_profile_scope_statement"] + ["installable_duschprofil_article_scope_and_length_evidence"] * 30
    targets = ["retained_drain_body_article"] * 3 + ["generic_duschprofil_scope"] + ["installable_duschprofil_article"] * 30
    decisions = ["accepted_ablauf_to_duschprofil_interface"] * 3 + ["accepted_generic_duschprofil_scope"] + ["accepted_installable_duschprofil_article"] * 30
    for i, (article, area, target, decision) in enumerate(zip(mod.EXPECTED_ARTICLE_ORDER, areas, targets, decisions), 1):
        row = {c: "" for c in mod.REQUIRED_COLUMNS}
        row.update(
            {
                "template_v3_row_id": f"{mod.ROW_ID_PREFIX}-{i:06d}",
                "family": mod.EXPECTED_FAMILY,
                "evidence_collection_area": area,
                "evidence_target_type": target,
                "article_number": article,
                "source_document_name": "TECE catalogue",
                "source_document_version": "2024",
                "source_page_or_section": "p.1",
                "source_url_or_path": "https://example.test/tece",
                "source_text_excerpt": "Official TECE evidence for diagnostic review only.",
                "reviewed_evidence_summary": f"Reviewed official evidence for {article or 'generic Duschprofil installable profile scope'}.",
                "reviewer_decision": decision,
                "reviewer_notes": "Diagnostic-only; no generation, mutation, promotion, benchmark readiness, or customer-view readiness.",
                "evidence_acceptance_allowed": "true",
                "evidence_complete": "true",
                "ready_for_future_diagnostic_design": "true",
                "diagnostic_only": "true",
            }
        )
        for f in mod.BLOCK_FALSE_FIELDS:
            row[f] = "false"
        rows.append(row)
    return rows


def _write_csv(path, rows, columns=None):
    columns = columns or list(rows[0].keys())
    with Path(path).open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _run(artifacts, **kw):
    csv_path, sp = artifacts
    return mod.validate(kw.get("evidence_csv", csv_path), kw.get("source_pack", sp), kw.get("family", mod.EXPECTED_FAMILY))


def _mut(artifacts, fn):
    csv_path, _ = artifacts
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8-sig")))
    fn(rows)
    _write_csv(csv_path, rows)


def test_valid_reviewed_v3_evidence_csv_passes(artifacts):
    r = _run(artifacts)
    assert r["valid"] is True
    assert r["errors"] == []
    assert r["input_inventory_row_count"] == 436
    assert r["family_inventory_row_count"] == 76
    assert r["current_machine_role_counts"] == mod.EXPECTED_MACHINE_ROLE_COUNTS
    assert r["evidence_csv_row_count"] == 34
    assert r["evidence_collection_area_counts"] == mod.EXPECTED_AREA_COUNTS
    assert r["evidence_target_type_counts"] == mod.EXPECTED_TARGET_TYPE_COUNTS
    assert r["retained_drain_body_articles"] == mod.RETAINED_DRAIN_BODY_ARTICLES
    assert r["installable_duschprofil_articles"] == mod.INSTALLABLE_DUSCHPROFIL_ARTICLES
    assert r["forbidden_spare_cover_articles_in_installable_scope"] == []
    assert r["accepted_ablauf_to_duschprofil_interface_count"] == 3
    assert r["accepted_generic_duschprofil_scope_count"] == 1
    assert r["accepted_installable_duschprofil_article_count"] == 30
    assert r["accepted_v3_evidence_row_count"] == 34
    assert r["reviewed_evidence_complete_row_count"] == 34
    assert r["ready_for_future_diagnostic_design_count"] == 34
    assert r["production_safe_candidate_count"] == 0
    assert r["production_promotion_blocked"] is True
    assert r["ready_for_benchmark"] is False
    assert r["ready_for_customer_view"] is False
    assert r["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE


def test_requires_tecedrainprofile_family(artifacts):
    assert _run(artifacts, family="TECEdrainline")["valid"] is False


def test_source_pack_baseline_counts_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _run(artifacts)["valid"] is False


def test_required_columns_enforced(artifacts):
    rows = _valid_rows()
    _write_csv(artifacts[0], rows, [c for c in rows[0] if c != "reviewer_notes"])
    assert "reviewer_notes" in _run(artifacts)["required_columns_missing"]


def test_exactly_34_rows_enforced(artifacts):
    _mut(artifacts, lambda rows: rows.pop())
    assert _run(artifacts)["valid"] is False


def test_required_row_ids_enforced(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(template_v3_row_id="bad"))
    assert _run(artifacts)["invalid_template_v3_rows"]


def test_duplicate_row_ids_fail(artifacts):
    _mut(artifacts, lambda rows: rows[1].update(template_v3_row_id=rows[0]["template_v3_row_id"]))
    assert _run(artifacts)["duplicate_template_v3_row_ids"]


def test_duplicate_article_rows_fail(artifacts):
    _mut(artifacts, lambda rows: rows[1].update(article_number=rows[0]["article_number"]))
    assert _run(artifacts)["duplicate_article_rows"]


def test_evidence_area_counts_enforced(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(evidence_collection_area="installable_duschprofil_article_scope_and_length_evidence"))
    assert _run(artifacts)["valid"] is False


def test_target_type_counts_enforced(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(evidence_target_type="installable_duschprofil_article"))
    assert _run(artifacts)["valid"] is False


def test_article_order_enforced(artifacts):
    _mut(artifacts, lambda rows: (rows[0].update(article_number="673002"), rows[1].update(article_number="673001")))
    assert _run(artifacts)["valid"] is False


@pytest.mark.parametrize("article", mod.FORBIDDEN_SPARE_COVER_ARTICLES)
def test_675xxx_spare_cover_articles_forbidden_as_installable_scope(artifacts, article):
    _mut(artifacts, lambda rows: rows[4].update(article_number=article))
    r = _run(artifacts)
    assert r["valid"] is False
    assert article in r["forbidden_spare_cover_articles_in_installable_scope"]


def test_wrong_reviewer_decision_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_decision="accepted_installable_duschprofil_article"))
    assert _run(artifacts)["invalid_manual_decision_rows"]


def test_accepted_decision_with_blank_source_fields_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(source_text_excerpt=""))
    assert _run(artifacts)["invalid_manual_evidence_rows"]


@pytest.mark.parametrize("field", mod.ALLOWED_TRUE_FIELDS)
def test_allowed_diagnostic_true_fields_must_be_true_for_all_rows(artifacts, field):
    _mut(artifacts, lambda rows: rows[0].update({field: "false"}))
    assert _run(artifacts)["valid"] is False


@pytest.mark.parametrize("field", mod.BLOCK_FALSE_FIELDS)
def test_blocking_flags_true_fail(artifacts, field):
    _mut(artifacts, lambda rows: rows[0].update({field: "true"}))
    r = _run(artifacts)
    assert r["valid"] is False
    assert r[f"{field}_count"] == 1


def test_diagnostic_only_true_required(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(diagnostic_only="false"))
    assert _run(artifacts)["diagnostic_only_leakage_rows"]


@pytest.mark.parametrize("field", mod.ZERO_COUNTER_FIELDS)
def test_generated_or_production_counts_always_zero(artifacts, field):
    assert _run(artifacts)[field] == 0


def _hash_tree(p: Path):
    return {str(x.relative_to(p)): hashlib.sha256(x.read_bytes()).hexdigest() for x in sorted(p.rglob("*")) if x.is_file()}


def test_source_pack_input_immutability(artifacts):
    before = _hash_tree(artifacts[1])
    _run(artifacts)
    assert _hash_tree(artifacts[1]) == before


def test_cli_json_stdout_and_json_out(artifacts, capsys, tmp_path):
    out = tmp_path / "report.json"
    assert mod.main(["--evidence-csv", str(artifacts[0]), "--source-pack", str(artifacts[1]), "--json-out", str(out), "--json"]) == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    stdout = json.loads(capsys.readouterr().out)
    assert data["valid"] is True
    assert stdout["valid"] is True
