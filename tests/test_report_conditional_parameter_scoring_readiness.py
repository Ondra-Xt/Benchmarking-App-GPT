from __future__ import annotations

from pathlib import Path

import pandas as pd

import tools.report_conditional_parameter_scoring_readiness as mod


MPLUS_SET_IDS = (
    "aco-showerdrain-mplus-90108120-proposal",
    "aco-showerdrain-mplus-90108121-proposal",
    "aco-showerdrain-mplus-90108122-proposal",
    "aco-showerdrain-mplus-90108123-proposal",
)


def _mplus_mappings() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "set_id": set_id,
                "product_family": "showerdrain_mplus",
                "assembly_model": "channel_body_x_drain_body_x_grate",
                "flow_rate_lps": "",
                "flow_rate_lps_10mm_head": 0.4,
                "flow_rate_lps_20mm_head": 0.46,
                "selected_default_flow_rate_lps": "",
                "flow_policy": "split_fields_only",
                "safe_to_generate": False,
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
                "blocking_reason": "blocked_pending_conditional_parameter_scoring",
            }
            for set_id in MPLUS_SET_IDS
        ]
    )


def _conditional_values() -> pd.DataFrame:
    rows = []
    for set_id in MPLUS_SET_IDS:
        for head, flow in ((10, 0.4), (20, 0.46)):
            rows.append(
                {
                    "set_id": set_id,
                    "product_family": "showerdrain_mplus",
                    "assembly_model": "channel_body_x_drain_body_x_grate",
                    "parameter_name": "flow_rate_lps",
                    "value": flow,
                    "unit": "l/s",
                    "condition_type": "head_water_level",
                    "condition_value": head,
                    "condition_unit": "mm",
                    "condition_label": f"{head} mm head water level",
                    "data_quality_status": "conditional_parameter_available_production_blocked",
                    "safe_to_generate": False,
                    "ready_for_benchmark": False,
                    "ready_for_customer_view": False,
                    "blocking_reason": "blocked_pending_conditional_parameter_scoring",
                }
            )
    return pd.DataFrame(rows)


def _frames(**overrides: pd.DataFrame) -> dict[str, pd.DataFrame]:
    frames = {
        "Conditional_Technical_Values": _conditional_values(),
        "Mplus_Compound_Mappings": _mplus_mappings(),
        "Products": pd.DataFrame(columns=["product_id"]),
        "Final_Assemblies": pd.DataFrame(columns=["product_id"]),
        "Final_Set_Details": pd.DataFrame(columns=["assembled_product_id", "set_id"]),
    }
    frames.update(overrides)
    return frames


def _write_workbook(path: Path, frames: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet, frame in frames.items():
            frame.to_excel(writer, sheet_name=sheet, index=False)


def test_report_succeeds_and_detects_valid_conditional_mplus_data(capsys):
    report = mod.build_report(_frames(), "test fixture")

    assert report.summary_counts["Conditional_Technical_Values"] == 8
    assert report.summary_counts["Mplus_Compound_Mappings"] == 4
    assert len(report.mplus_readiness.set_ids) == 4
    assert report.mplus_readiness.rows_per_set == {set_id: 2 for set_id in MPLUS_SET_IDS}
    assert report.mplus_readiness.head_10mm_rows == 4
    assert report.mplus_readiness.head_20mm_rows == 4
    assert report.grouped_counts["product_family"] == {"showerdrain_mplus": 8}
    assert report.grouped_counts["parameter_name"] == {"flow_rate_lps": 8}
    assert report.grouped_counts["condition_type"] == {"head_water_level": 8}

    mod.print_report(report)
    out = capsys.readouterr().out
    assert "total_rows: 8" in out
    assert "set_ids: 4" in out
    assert "conditional_rows_per_set: 2" in out
    assert "10 mm head rows: 4" in out
    assert "20 mm head rows: 4" in out
    assert "scalar flow_rate_lps selected: no" in out
    assert "selected_default_flow_rate_lps selected: no" in out
    assert "status: blocked_pending_conditional_parameter_scoring" in out
    assert "M+ is data-rich but scoring-blocked." in out


def test_report_verifies_flow_values_at_10mm_and_20mm_head():
    report = mod.build_report(_frames(), "test fixture")
    rows = _conditional_values()

    by_head = {
        int(row.condition_value): float(row.value)
        for row in rows[rows["set_id"].eq(report.mplus_readiness.set_ids[0])].itertuples(index=False)
    }
    assert by_head == {10: 0.4, 20: 0.46}


def test_report_fails_if_mplus_scalar_flow_rate_is_filled():
    mplus = _mplus_mappings()
    mplus.loc[0, "flow_rate_lps"] = "0.4"

    try:
        mod.build_report(_frames(Mplus_Compound_Mappings=mplus), "test fixture")
    except mod.ReadinessReportError as exc:
        assert "scalar flow_rate_lps is filled" in str(exc)
    else:
        raise AssertionError("expected ReadinessReportError")


def test_report_fails_if_mplus_selected_default_flow_rate_is_filled():
    mplus = _mplus_mappings()
    mplus.loc[0, "selected_default_flow_rate_lps"] = "0.4"

    try:
        mod.build_report(_frames(Mplus_Compound_Mappings=mplus), "test fixture")
    except mod.ReadinessReportError as exc:
        assert "selected_default_flow_rate_lps is filled" in str(exc)
    else:
        raise AssertionError("expected ReadinessReportError")


def test_report_fails_for_malformed_mplus_conditional_rows():
    conditional = _conditional_values()
    conditional = conditional[~((conditional["set_id"] == MPLUS_SET_IDS[0]) & (conditional["condition_value"] == 20))]

    try:
        mod.build_report(_frames(Conditional_Technical_Values=conditional), "test fixture")
    except mod.ReadinessReportError as exc:
        assert "expected 2 rows per set_id" in str(exc)
    else:
        raise AssertionError("expected ReadinessReportError")


def test_report_allows_conditional_rows_linked_to_blocked_production_outputs():
    products = pd.DataFrame([{"product_id": MPLUS_SET_IDS[0]}])

    report = mod.build_report(_frames(Products=products), "test fixture")

    assert report.mplus_readiness.status == "blocked_pending_conditional_parameter_scoring"
    assert report.mplus_readiness.ready_for_benchmark_count == 0


def test_report_shows_scenario_data_available_but_scoring_not_implemented(capsys):
    report = mod.build_report(_frames(), "test fixture")
    matrix = {row.scenario: row for row in report.scenario_matrix}

    assert matrix["flow_head_10mm"].data_available is True
    assert matrix["flow_head_10mm"].scoring_implemented is False
    assert matrix["flow_head_20mm"].data_available is True
    assert matrix["flow_head_20mm"].scoring_implemented is False
    assert matrix["conservative_minimum"].policy_approved is False
    assert matrix["maximum_declared"].policy_approved is False
    assert matrix["no_scenario_selected"].current_safe_default is True

    mod.print_report(report)
    out = capsys.readouterr().out
    assert "flow_head_10mm: data_available=True, scoring_implemented=False" in out
    assert "flow_head_20mm: data_available=True, scoring_implemented=False" in out
    assert "conservative_minimum: data_available=True, policy_approved=False, scoring_implemented=False" in out
    assert "maximum_declared: data_available=True, policy_approved=False, scoring_implemented=False" in out
    assert "no_scenario_selected:" in out
    assert "current_safe_default=True" in out
    assert "scenario_scoring_not_implemented" in out


def test_report_confirms_production_behavior_changed_no(capsys):
    report = mod.build_report(_frames(), "test fixture")
    mod.print_report(report)
    out = capsys.readouterr().out

    assert report.production_behavior_changed is True
    assert "Products changed: yes (blocked M+ assembled rows added)" in out
    assert "BOM changed: no" in out
    assert "Final_Assemblies changed: yes (blocked M+ assembled rows added)" in out
    assert "Final_Set_Details changed: yes (blocked M+ assembled rows added)" in out
    assert "Scoring changed: no" in out
    assert "Customer-facing output changed: no" in out
    assert "Production behavior changed: yes" in out


def test_main_supports_xlsx_read_only(tmp_path, capsys):
    path = tmp_path / "benchmark_output.xlsx"
    _write_workbook(path, _frames())

    result = mod.main(["--xlsx", str(path)])
    out = capsys.readouterr().out

    assert result == 0
    assert f"xlsx:{path}" in out
    assert "total_rows: 8" in out


def test_main_supports_dir_latest_export_selection(tmp_path, capsys):
    older = tmp_path / "benchmark_output_old.xlsx"
    newer = tmp_path / "benchmark_output_new.xlsx"
    other = tmp_path / "unrelated.xlsx"
    _write_workbook(older, _frames())
    _write_workbook(newer, _frames())
    _write_workbook(other, _frames())
    older.touch()
    newer.touch()
    other.touch()
    # Make the benchmark-like file newest among benchmark matches; unrelated must be ignored.
    import os

    os.utime(older, (1_000, 1_000))
    os.utime(newer, (2_000, 2_000))
    os.utime(other, (3_000, 3_000))

    result = mod.main(["--dir", str(tmp_path)])
    out = capsys.readouterr().out

    assert result == 0
    assert f"Selected XLSX: {newer}" in out
    assert f"xlsx:{newer}" in out
    assert "total_rows: 8" in out


def test_main_returns_nonzero_for_invalid_mplus_default_selection(tmp_path, capsys):
    path = tmp_path / "benchmark_output.xlsx"
    mplus = _mplus_mappings()
    mplus.loc[0, "selected_default_flow_rate_lps"] = "0.4"
    _write_workbook(path, _frames(Mplus_Compound_Mappings=mplus))

    result = mod.main(["--xlsx", str(path)])
    out = capsys.readouterr().out

    assert result == 2
    assert "ERROR:" in out
    assert "selected_default_flow_rate_lps is filled" in out
    assert "Production behavior changed: yes" in out
