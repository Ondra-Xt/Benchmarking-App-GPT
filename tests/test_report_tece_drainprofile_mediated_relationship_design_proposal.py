from __future__ import annotations

import csv, hashlib, json, subprocess, sys
from pathlib import Path
from types import SimpleNamespace
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import report_tece_drainprofile_mediated_relationship_design_proposal as mod
from tools import (
    validate_tece_drainprofile_mediated_relationship_evidence_template_v2_csv as validator,
)


class Row(SimpleNamespace):
    pass


def _source_rows(role_counts=None):
    role_counts = role_counts or validator.EXPECTED_MACHINE_ROLE_COUNTS
    roles = [role for role, count in role_counts.items() for _ in range(count)]
    arts = (
        validator.RETAINED_DRAIN_BODY_ARTICLES
        + validator.PROPOSED_PROFILE_COVER_ARTICLES
    )
    rows = [
        Row(
            tece_family_candidate=validator.EXPECTED_FAMILY,
            product_family=validator.EXPECTED_FAMILY,
            tece_article_role_candidate=role,
            article_number=arts[i] if i < len(arts) else f"67{i:04d}",
        )
        for i, role in enumerate(roles)
    ]
    rows += [
        Row(
            tece_family_candidate="TECEdrainline",
            product_family="TECEdrainline",
            tece_article_role_candidate="unknown",
            article_number=f"60{i:04d}",
        )
        for i in range(436 - len(rows))
    ]
    return rows


@pytest.fixture
def fake_source_pack(monkeypatch):
    monkeypatch.setattr(
        validator, "load_source_pack", lambda _p: SimpleNamespace(rows=_source_rows())
    )


def _valid_evidence_rows():
    rows = []
    areas = (
        ["drain_body_to_duschprofil_system_relationship"] * 3
        + ["duschprofil_profile_cover_scope_confirmation"]
        + ["proposed_profile_cover_article_scope"] * 11
    )
    targets = (
        ["retained_drain_body_article"] * 3
        + ["duschprofil_profile_cover_scope"]
        + ["proposed_profile_cover_article"] * 11
    )
    for i, (article, area, target) in enumerate(
        zip(validator.EXPECTED_ARTICLE_ORDER, areas, targets), 1
    ):
        notes = "Reviewed diagnostic-only evidence; no direct matrix and no production readiness."
        if article in {"675019", "675010"}:
            notes += " Date scope bis 06/2023."
        if article in {"675024", "675025"}:
            notes += " Date scope ab 07/2023."
        row = {c: "" for c in validator.REQUIRED_COLUMNS}
        row.update(
            {
                "template_v2_row_id": f"{validator.ROW_ID_PREFIX}-{i:06d}",
                "family": validator.EXPECTED_FAMILY,
                "evidence_collection_area": area,
                "evidence_target_type": target,
                "article_number": article,
                "source_document_name": "TECE catalogue",
                "source_document_version": "2024",
                "source_page_or_section": "p.1",
                "source_url_or_path": "https://example.test/tece",
                "source_text_excerpt": "Official diagnostic scope evidence only.",
                "reviewed_evidence_summary": f"Official TECE evidence for {article or 'Duschprofil scope'}.",
                "reviewer_decision": next(iter(validator.ALLOWED_DECISIONS[area])),
                "reviewer_notes": notes,
                "safe_to_use_for_future_diagnostic_design": "true",
                "diagnostic_only": "true",
            }
        )
        for f in validator.BLOCK_FALSE_FIELDS:
            row[f] = "false"
        rows.append(row)
    return rows


def _write_csv(path: Path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    a = SimpleNamespace()
    a.validation = (
        tmp_path
        / "tecedrainprofile_mediated_relationship_evidence_template_v2_REVIEWED_validation.json"
    )
    a.template = (
        tmp_path
        / "tecedrainprofile_mediated_relationship_evidence_template_v2_report.json"
    )
    a.evidence = (
        tmp_path
        / "tecedrainprofile_mediated_relationship_evidence_template_v2_REVIEWED.csv"
    )
    a.qa_report = (
        tmp_path
        / "tecedrainprofile_mediated_relationship_evidence_template_v2_QA_report.json"
    )
    a.qa_csv = (
        tmp_path / "tecedrainprofile_mediated_relationship_evidence_template_v2_QA.csv"
    )
    a.source_pack = tmp_path / "source_pack"
    a.source_pack.mkdir()
    (a.source_pack / "keep.txt").write_text("unchanged")
    a.validation.write_text(json.dumps({"valid": True, "errors": []}))
    a.template.write_text(json.dumps({"valid": True}))
    a.qa_report.write_text(
        json.dumps({"valid": True, "future_diagnostic_design_ready_count": 15})
    )
    _write_csv(a.evidence, _valid_evidence_rows())
    _write_csv(
        a.qa_csv,
        [{"qa_row_id": f"QA-{i:06d}", "qa_status": "pass"} for i in range(1, 16)],
    )
    a.out = tmp_path / "proposal.csv"
    a.json_out = tmp_path / "proposal.json"
    return a


def _report(a):
    return mod.generate_report(
        a.qa_report, a.qa_csv, a.validation, a.evidence, a.template, a.source_pack
    )


def _mut_csv(a, fn):
    rows = list(csv.DictReader(a.evidence.open(encoding="utf-8-sig")))
    fn(rows)
    _write_csv(a.evidence, rows)


def test_valid_design_proposal_generation_passes(artifacts):
    report, rows = _report(artifacts)
    assert report["valid"] is True and report["errors"] == []
    assert len(rows) == 7 and all(r["diagnostic_only"] == "true" for r in rows)


def test_cli_writes_csv_and_json(artifacts, capsys):
    rc = mod.main(
        [
            "--qa-report",
            str(artifacts.qa_report),
            "--qa-csv",
            str(artifacts.qa_csv),
            "--validation-report",
            str(artifacts.validation),
            "--evidence-csv",
            str(artifacts.evidence),
            "--template-v2-report",
            str(artifacts.template),
            "--source-pack",
            str(artifacts.source_pack),
            "--out",
            str(artifacts.out),
            "--json-out",
            str(artifacts.json_out),
            "--json",
        ]
    )
    assert rc == 0 and artifacts.out.exists() and artifacts.json_out.exists()
    assert json.loads(artifacts.json_out.read_text())["design_proposal_row_count"] == 7
    assert '"valid": true' in capsys.readouterr().out


@pytest.mark.parametrize("attr", ["qa_report", "validation", "template"])
def test_input_report_valid_false_fails(artifacts, attr):
    getattr(artifacts, attr).write_text(json.dumps({"valid": False, "errors": []}))
    assert _report(artifacts)[0]["valid"] is False


def test_wrong_family_fails(artifacts):
    assert (
        mod.generate_report(
            artifacts.qa_report,
            artifacts.qa_csv,
            artifacts.validation,
            artifacts.evidence,
            artifacts.template,
            artifacts.source_pack,
            "Wrong",
        )[0]["valid"]
        is False
    )


def test_source_pack_baseline_mismatch_fails(artifacts, monkeypatch):
    monkeypatch.setattr(
        validator, "load_source_pack", lambda _p: SimpleNamespace(rows=[])
    )
    assert _report(artifacts)[0]["valid"] is False


@pytest.mark.parametrize("target", ["evidence", "qa_csv"])
def test_input_row_count_mismatch_fails(artifacts, target):
    path = getattr(artifacts, target)
    rows = list(csv.DictReader(path.open(encoding="utf-8-sig")))
    rows.pop()
    _write_csv(path, rows)
    assert _report(artifacts)[0]["valid"] is False


def test_proposal_row_count_exactly_7_and_ids_deterministic_unique(artifacts):
    report, rows = _report(artifacts)
    assert report["design_proposal_row_count"] == 7
    assert report["design_proposal_row_ids"] == mod.EXPECTED_ROW_IDS
    assert [r["proposal_row_id"] for r in rows] == mod.EXPECTED_ROW_IDS
    assert len(set(report["design_proposal_row_ids"])) == 7


@pytest.mark.parametrize(
    "name,value",
    [
        ("RETAINED_DRAIN_BODY_ARTICLES", ["673001"]),
        ("PROPOSED_PROFILE_COVER_ARTICLES", ["675000"]),
    ],
)
def test_article_lists_exact_enforced(artifacts, monkeypatch, name, value):
    monkeypatch.setattr(mod, name, value)
    assert _report(artifacts)[0]["valid"] is False


def test_intermediate_system_object_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "INTERMEDIATE_SYSTEM_OBJECT", "Wrong")
    assert _report(artifacts)[0]["valid"] is False


@pytest.mark.parametrize(
    "area", ["mediated_relationship_topology", "direct_pairing_model_rejection"]
)
def test_mediated_and_direct_rejection_enforced(artifacts, monkeypatch, area):
    orig = mod.build_design_proposal_rows

    def bad(family=mod.EXPECTED_FAMILY):
        rows = orig(family)
        for r in rows:
            if r["proposal_area"] == area:
                r["relationship_model"] = "broken"
        return rows

    monkeypatch.setattr(mod, "build_design_proposal_rows", bad)
    assert _report(artifacts)[0]["valid"] is False


def test_direct_pairing_claim_leakage_fails(artifacts):
    _mut_csv(
        artifacts,
        lambda rows: rows[0].update(reviewer_notes="675001 is compatible with 673001"),
    )
    assert _report(artifacts)[0]["valid"] is False


@pytest.mark.parametrize("field", mod.FALSE_ALLOWED_FIELDS)
def test_generation_mutation_promotion_readiness_flags_fail(
    artifacts, monkeypatch, field
):
    orig = mod.build_design_proposal_rows

    def bad(family=mod.EXPECTED_FAMILY):
        rows = orig(family)
        rows[0][field] = "true"
        return rows

    monkeypatch.setattr(mod, "build_design_proposal_rows", bad)
    report, _ = _report(artifacts)
    assert report["valid"] is False and report[f"{field}_count"] == 1


@pytest.mark.parametrize(
    "counter", mod.GENERATED_COUNTERS + ["production_safe_candidate_count"]
)
def test_generated_counters_and_production_safe_nonzero_fail(artifacts, counter):
    artifacts.template.write_text(json.dumps({"valid": True, counter: 1}))
    assert _report(artifacts)[0]["valid"] is False


@pytest.mark.parametrize(
    "key,bad_value",
    [
        ("production_promotion_blocked", False),
        ("ready_for_benchmark", True),
        ("ready_for_customer_view", True),
    ],
)
def test_fixed_production_and_readiness_values_fail_if_mutated(
    artifacts, key, bad_value
):
    report, rows = _report(artifacts)
    report[key] = bad_value
    checked = mod.validate_design_proposal_report(
        report,
        rows,
        list(csv.DictReader(artifacts.evidence.open(encoding="utf-8-sig"))),
        list(csv.DictReader(artifacts.qa_csv.open(encoding="utf-8-sig"))),
        mod.EXPECTED_FAMILY,
        [],
    )
    assert checked["valid"] is False


def test_diagnostic_only_false_fails(artifacts, monkeypatch):
    orig = mod.build_design_proposal_rows

    def bad(family=mod.EXPECTED_FAMILY):
        rows = orig(family)
        rows[0]["diagnostic_only"] = "false"
        return rows

    monkeypatch.setattr(mod, "build_design_proposal_rows", bad)
    assert _report(artifacts)[0]["valid"] is False


def test_source_pack_input_immutability(artifacts):
    files = [
        artifacts.validation,
        artifacts.evidence,
        artifacts.template,
        artifacts.qa_report,
        artifacts.qa_csv,
        artifacts.source_pack / "keep.txt",
    ]
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    mod.main(
        [
            "--qa-report",
            str(artifacts.qa_report),
            "--qa-csv",
            str(artifacts.qa_csv),
            "--validation-report",
            str(artifacts.validation),
            "--evidence-csv",
            str(artifacts.evidence),
            "--template-v2-report",
            str(artifacts.template),
            "--source-pack",
            str(artifacts.source_pack),
            "--out",
            str(artifacts.out),
            "--json-out",
            str(artifacts.json_out),
        ]
    )
    assert before == {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}


def test_diagnostic_only_note_present(artifacts):
    assert _report(artifacts)[0]["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_remains_pass_stable():
    result = subprocess.run(
        [
            sys.executable,
            "tools/report_aco_final_baseline.py",
            "--workbook",
            "nonexistent.xlsx",
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
    )
    assert (
        result.returncode != 0
        or "OVERALL: ACO_BASELINE_STABLE" in result.stdout
        or "PASS" in result.stdout
    )
