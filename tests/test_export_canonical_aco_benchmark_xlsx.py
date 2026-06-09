from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.canonical_aco_export import CanonicalAcoFrames


def _write_canonical_summary_workbook(path: Path) -> None:
    product_ids = (
        [f"aco-assembled-showerdrain-splus-base-{index}__drain-{index}" for index in range(16)]
        + [f"aco-assembled-showerdrain-c-base-{index}__grate-{index}" for index in range(4)]
        + [f"aco-assembled-showerdrain-mplus-channel__drain-{index}__grate" for index in range(4)]
        + [f"aco-assembled-showerdrain-cplus-base-{index}__grate-{index}" for index in range(30)]
        + [f"canonical-product-{index}" for index in range(26)]
    )
    products = pd.DataFrame({"product_id": product_ids})
    mplus_ids = product_ids[20:24]
    scenario_10 = pd.DataFrame({
        "product_id": product_ids,
        "flow_rate_lps": [0.40 if product_id in mplus_ids else 0.91 for product_id in product_ids],
    })
    scenario_20 = pd.DataFrame({
        "product_id": product_ids,
        "flow_rate_lps": [0.46 if product_id in mplus_ids else 0.91 for product_id in product_ids],
    })
    frames = {
        "Products": products,
        "Comparison": products.copy(),
        "Scoring_Field_Coverage": products.copy(),
        "Candidates_All": pd.DataFrame({"product_id": range(118)}),
        "Components": pd.DataFrame({"product_id": range(100)}),
        "BOM_Options": pd.DataFrame(
            [
                {"option_type": "compatible_grate", "product_family": "showerdrain_cplus"}
                for _ in range(30)
            ]
            + [
                {"option_type": "compatible_grate", "product_family": "other"}
                for _ in range(53)
            ]
            + [
                {"option_type": "optional_accessory", "product_family": "other"}
                for _ in range(168)
            ]
        ),
        "Final_Assemblies": pd.DataFrame({"product_id": product_ids[:62]}),
        "Final_Set_Details": pd.DataFrame({"assembled_product_id": product_ids[:62]}),
        "Cplus_Compatible_Grate_Evidence": pd.DataFrame({"mapping_id": range(30)}),
        "Comparison_flow_head_10mm": scenario_10,
        "Comparison_flow_head_20mm": scenario_20,
    }
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, frame in frames.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)


def test_canonical_aco_builder_runs_only_aco_connector(monkeypatch):
    from src import canonical_aco_export

    calls: list[tuple[str, tuple[str, ...]]] = []
    registry = pd.DataFrame({"product_id": ["aco-candidate"]})
    outputs = tuple(pd.DataFrame({"product_id": [name]}) for name in (
        "product", "comparison", "excluded", "evidence", "bom"
    ))

    def fake_discovery(**kwargs):
        calls.append(("discovery", tuple(kwargs["selected_connectors"])))
        return registry, pd.DataFrame()

    def fake_update(received_registry, _cfg, **kwargs):
        assert received_registry is registry
        calls.append(("update", tuple(kwargs["selected_connectors"])))
        return outputs

    monkeypatch.setattr(canonical_aco_export, "run_discovery", fake_discovery)
    monkeypatch.setattr(canonical_aco_export, "run_update", fake_update)

    frames = canonical_aco_export.build_canonical_aco_frames(object())

    assert calls == [("discovery", ("aco",)), ("update", ("aco",))]
    assert frames.registry is registry
    assert frames.products is outputs[0]
    assert frames.bom_options is outputs[4]


def test_canonical_aco_export_uses_shared_fail_closed_export(monkeypatch, tmp_path):
    from src import canonical_aco_export

    frames = CanonicalAcoFrames(*(pd.DataFrame({"sentinel": [name]}) for name in (
        "registry", "products", "comparison", "excluded", "evidence", "bom"
    )))
    calls = []

    def fake_export(template, output, cfg, **kwargs):
        calls.append((Path(template), Path(output), cfg, kwargs))
        Path(output).write_bytes(b"published")

    monkeypatch.setattr(canonical_aco_export, "export_streamlit_workbook", fake_export)
    destination = tmp_path / "benchmark_output.xlsx"
    result = canonical_aco_export.export_canonical_aco_workbook(
        "template.xlsx",
        destination,
        object(),
        frame_builder=lambda *_args, **_kwargs: frames,
    )

    assert result == destination
    assert destination.read_bytes() == b"published"
    assert len(calls) == 1
    assert calls[0][3] == {
        "registry": frames.registry,
        "products": frames.products,
        "comparison": frames.comparison,
        "excluded": frames.excluded,
        "evidence": frames.evidence,
        "bom_options": frames.bom_options,
        "input_description": "canonical ACO pipeline output",
    }


def test_canonical_aco_cli_creates_expected_workbook_and_summary(monkeypatch, tmp_path, capsys):
    from tools import export_canonical_aco_benchmark_xlsx as cli
    from tools.validate_xlsx_export import validate_app_export_baseline

    destination = tmp_path / "benchmark_output.xlsx"

    def fake_validated_export(_template, output, _cfg):
        _write_canonical_summary_workbook(Path(output))
        return Path(output)

    monkeypatch.setattr(cli, "export_canonical_aco_workbook", fake_validated_export)

    assert cli.main(["--out", str(destination)]) == 0
    assert destination.is_file()
    passed, results = validate_app_export_baseline(str(destination))
    assert passed, [result.detail for result in results if not result.passed]

    with pd.ExcelFile(destination, engine="openpyxl") as xls:
        assert len(pd.read_excel(xls, sheet_name="Products")) == 80
        assert len(pd.read_excel(xls, sheet_name="BOM_Options")) == 251
        assert len(pd.read_excel(xls, sheet_name="Final_Assemblies")) == 62
        assert len(pd.read_excel(xls, sheet_name="Final_Set_Details")) == 62
        assert len(pd.read_excel(xls, sheet_name="Cplus_Compatible_Grate_Evidence")) == 30
        scenario_10 = pd.read_excel(xls, sheet_name="Comparison_flow_head_10mm")
        scenario_20 = pd.read_excel(xls, sheet_name="Comparison_flow_head_20mm")
        assert len(scenario_10) == len(scenario_20) == 80
        mplus_10 = scenario_10[scenario_10["product_id"].str.startswith("aco-assembled-showerdrain-mplus-")]
        mplus_20 = scenario_20[scenario_20["product_id"].str.startswith("aco-assembled-showerdrain-mplus-")]
        assert mplus_10["flow_rate_lps"].eq(0.40).all()
        assert mplus_20["flow_rate_lps"].eq(0.46).all()

    output = capsys.readouterr().out
    assert f"Output: {destination}" in output
    assert "Products: 80" in output
    assert "BOM_Options: 251" in output
    assert "Final_Assemblies: 62" in output
    assert "Cplus_Compatible_Grate_Evidence: 30" in output
    assert "Cplus_assembled: 30" in output
    assert output.rstrip().endswith("OVERALL: PASS")
