from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_drainprofile_role_correction_impact as mod
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import (
    EXPECTED_ARTICLES,
    EXPECTED_REJECT_REASON_TAG_MAPPING,
)

UNKNOWN_REVIEWED_ARTICLES = {
    "675004", "675005", "675006", "675008", "675009", "675016", "675017", "675018", "675025",
}


@dataclass
class Row:
    article_number: str
    tece_family_candidate: str = "TECEdrainprofile"
    product_family: str = "TECEdrainprofile"
    tece_article_role_candidate: str = "drain_body"
    article_role: str = "drain_body"


class Pack:
    def __init__(self, rows: list[Row]) -> None:
        self.rows = rows
        self.source_pack_candidate_count = len(rows)


def decision(article: str) -> str:
    return "drain_body_no_length_found" if article in {"673001", "673002", "673003"} else "not_drain_body"


def pack(role_counts: dict[str, int] | None = None) -> Pack:
    counts = role_counts or mod.EXPECTED_ROLE_COUNTS
    reviewed = [
        Row(article, tece_article_role_candidate="unknown" if article in UNKNOWN_REVIEWED_ARTICLES else "drain_body", article_role="unknown" if article in UNKNOWN_REVIEWED_ARTICLES else "drain_body")
        for article in EXPECTED_ARTICLES
    ]
    reviewed_counts = {"drain_body": 3, "unknown": 9}
    extras: list[Row] = []
    for role, count in counts.items():
        for index in range(max(0, count - reviewed_counts.get(role, 0))):
            extras.append(Row(f"X{role}{index}", tece_article_role_candidate=role, article_role=role))
    other = [
        Row(f"O{index}", tece_family_candidate="Other", product_family="Other", tece_article_role_candidate="unknown", article_role="unknown")
        for index in range(436 - len(reviewed) - len(extras))
    ]
    return Pack(reviewed + extras + other)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def review_rows(**extra: str) -> list[dict[str, str]]:
    return [
        {
            "family": "TECEdrainprofile",
            "article_number": article,
            "original_article_role": "drain_body",
            "reviewer_decision": decision(article),
            "reject_reason_tag": EXPECTED_REJECT_REASON_TAG_MAPPING[article],
            "reviewer_notes": "diagnostic review note",
            "reviewed_nominal_length_mm": "",
            "safe_to_apply_automatically": "false",
            **extra,
        }
        for article in EXPECTED_ARTICLES
    ]


def reject_rows() -> list[dict[str, str]]:
    return [
        {
            "article_number": article,
            "reviewer_decision": decision(article),
            "reject_reason_tag": EXPECTED_REJECT_REASON_TAG_MAPPING[article],
            "reviewed_nominal_length_mm": "",
            "safe_to_apply_automatically": "false",
        }
        for article in EXPECTED_ARTICLES
    ]


def fixtures(tmp_path: Path, rows: list[dict[str, str]] | None = None, rejects: list[dict[str, str]] | None = None, validation: dict | None = None):
    review = tmp_path / "review.csv"
    reject = tmp_path / "reject.csv"
    validation_report = tmp_path / "validation.json"
    out = tmp_path / "out.csv"
    write_csv(review, rows or review_rows())
    write_csv(reject, rejects or reject_rows())
    validation_report.write_text(json.dumps({"valid": True} if validation is None else validation), encoding="utf-8")
    return review, reject, validation_report, out


def report(tmp_path: Path, monkeypatch, **kwargs):
    pack_obj = kwargs.pop("pack_obj", pack())
    monkeypatch.setattr(mod, "load_source_pack", lambda _path: pack_obj)
    review, reject, validation_report, out = fixtures(
        tmp_path,
        rows=kwargs.pop("rows", None),
        rejects=kwargs.pop("rejects", None),
        validation=kwargs.pop("validation", None),
    )
    return mod.build_impact_report("source", review, reject, validation_report, out=out, **kwargs), out


def test_valid_impact_report_passes_with_expected_unknown_source_pack_roles(tmp_path: Path, monkeypatch) -> None:
    result, _ = report(tmp_path, monkeypatch)
    assert result["valid"] is True
    assert result["errors"] == []
    assert result["review_csv_article_role_counts"] == {"drain_body": 12}
    assert result["reviewed_article_source_pack_role_counts"] == {"drain_body": 3, "unknown": 9}


def test_exactly_12_impact_rows(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch)[0]["impact_row_count"] == 12


def test_exact_expected_article_set(tmp_path: Path, monkeypatch) -> None:
    result, out = report(tmp_path, monkeypatch)
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert sorted(row["article_number"] for row in rows) == EXPECTED_ARTICLES
    assert result["duplicate_article_numbers"] == []


def test_requires_valid_validation_report(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch, validation={})[0]["valid"] is False


def test_invalid_validation_report_fails(tmp_path: Path, monkeypatch) -> None:
    result, _ = report(tmp_path, monkeypatch, validation={"valid": False})
    assert result["validation_report_valid"] is False
    assert result["valid"] is False


def test_review_csv_article_role_must_be_drain_body(tmp_path: Path, monkeypatch) -> None:
    rows = review_rows()
    rows[0]["original_article_role"] = "unknown"
    result, _ = report(tmp_path, monkeypatch, rows=rows)
    assert result["valid"] is False
    assert result["invalid_review_csv_article_role_rows"]


def test_current_source_pack_unknown_roles_are_diagnostic_not_errors(tmp_path: Path, monkeypatch) -> None:
    result, out = report(tmp_path, monkeypatch)
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    unknown_rows = [row for row in rows if row["article_number"] in UNKNOWN_REVIEWED_ARTICLES]
    assert len(unknown_rows) == 9
    assert {row["current_source_pack_role"] for row in unknown_rows} == {"unknown"}
    assert result["invalid_current_machine_role_rows"] == []
    assert len(result["source_pack_role_observation_rows"]) == 9


def test_current_tece_drainprofile_role_count_baseline_is_enforced(tmp_path: Path, monkeypatch) -> None:
    counts = {**mod.EXPECTED_ROLE_COUNTS, "unknown": 24}
    assert report(tmp_path, monkeypatch, pack_obj=pack(counts))[0]["valid"] is False


def test_decision_counts_are_enforced(tmp_path: Path, monkeypatch) -> None:
    rows = review_rows()
    rows[0]["reviewer_decision"] = "not_drain_body"
    assert report(tmp_path, monkeypatch, rows=rows)[0]["valid"] is False


def test_reject_tag_counts_are_enforced(tmp_path: Path, monkeypatch) -> None:
    rows = review_rows()
    rows[0]["reject_reason_tag"] = "spare_part"
    assert report(tmp_path, monkeypatch, rows=rows)[0]["valid"] is False


def test_impact_category_counts_are_correct(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch)[0]["impact_category_counts"] == {
        "reject_machine_or_exported_drain_body_role": 9,
        "retain_as_drain_or_ablauf_without_length": 3,
    }


def test_proposed_diagnostic_corrected_role_counts_are_correct(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch)[0]["proposed_corrected_role_diagnostic_counts"] == {
        "accessory": 1,
        "drain_body_unresolved_length": 3,
        "profile_cover": 1,
        "spare_part": 4,
        "water_trap": 3,
    }


def test_no_length_overlay_allowed(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch)[0]["length_overlay_allowed_count"] == 0


def test_no_compatibility_pairing_allowed(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch)[0]["compatibility_pairing_allowed_count"] == 0


def test_no_source_pack_mutation_allowed(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch)[0]["source_pack_mutation_allowed_count"] == 0


def test_production_safe_leakage_fails(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch, rows=review_rows(production_safe="true"))[0]["production_leakage_rows"]


def test_production_promotion_blocked_false_fails(tmp_path: Path, monkeypatch) -> None:
    result, _ = report(tmp_path, monkeypatch, rows=review_rows(production_promotion_blocked="false"))
    assert result["production_promotion_blocked"] is False


def test_ready_for_benchmark_leakage_fails(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch, rows=review_rows(ready_for_benchmark="true"))[0]["readiness_leakage_rows"]


def test_ready_for_customer_view_leakage_fails(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch, rows=review_rows(ready_for_customer_view="true"))[0]["readiness_leakage_rows"]


def test_diagnostic_only_false_fails(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch, rows=review_rows(diagnostic_only="false"))[0]["diagnostic_only_leakage_rows"]


def test_duplicate_article_numbers_fail(tmp_path: Path, monkeypatch) -> None:
    rows = review_rows()
    rows[1]["article_number"] = rows[0]["article_number"]
    result, _ = report(tmp_path, monkeypatch, rows=rows)
    assert result["valid"] is False
    assert result["duplicate_article_numbers"] == ["673001"]


def test_diagnostic_only_note_is_present(tmp_path: Path, monkeypatch) -> None:
    assert report(tmp_path, monkeypatch)[0]["diagnostic_only_note"] == mod.NOTE


def test_source_pack_immutability(tmp_path: Path, monkeypatch) -> None:
    pack_obj = pack()
    before = [(row.article_number, row.tece_article_role_candidate) for row in pack_obj.rows]
    report(tmp_path, monkeypatch, pack_obj=pack_obj)
    assert before == [(row.article_number, row.tece_article_role_candidate) for row in pack_obj.rows]


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    text = aco_mod.format_report(
        Path("x.xlsx"),
        aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {}),
    )
    assert "OVERALL: ACO_BASELINE_STABLE" in text
    assert "PASS baseline_guard" in text
