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
        + [f"aco-showerdrain-b-finished-set-{index}" for index in range(8)]
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
        "Eplus_Compatible_Grate_Evidence": pd.DataFrame({"mapping_id": range(3)}),
        "Cplus_Compatible_Grate_Evidence": pd.DataFrame({"mapping_id": range(30)}),
        "Bline_Source_Evidence": pd.DataFrame({"evidence_id": range(8)}),
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
    assert set(outputs[0]["product_id"]).issubset(set(frames.products["product_id"]))
    assert {"aco-showerdrain-cplus-standard-h92", "aco-showerdrain-cplus-low-h69"}.issubset(
        set(frames.products["product_id"])
    )
    assert len(frames.excluded) == 16
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



def test_canonical_splus_fixture_fallback_survives_failed_live_source(monkeypatch):
    from src import canonical_aco_export
    from src.config import default_config
    from src.connectors import aco

    def failed_live_get(url, timeout=35):
        return 503, aco._canonicalize_url(url), "", "simulated live S+ outage"

    monkeypatch.setattr(aco, "_safe_get_text", failed_live_get)

    def discovery_probe(**_kwargs):
        rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
        registry = pd.DataFrame(rows)
        splus = registry[registry["product_family"].eq("showerdrain_splus")].copy()
        assert len(splus[splus["system_role"].eq("profile_channel")]) == 8
        assert len(splus[splus["system_role"].eq("drain_body")]) == 2
        return registry, pd.DataFrame()

    def update_probe(registry, _cfg, **_kwargs):
        from src import pipeline

        with monkeypatch.context() as m2:
            m2.setitem(pipeline.CONNECTORS, "aco", aco)
            return pipeline.run_update(registry, default_config(), selected_connectors=("aco",))

    monkeypatch.setattr(canonical_aco_export, "run_discovery", discovery_probe)
    monkeypatch.setattr(canonical_aco_export, "run_update", update_probe)

    frames = canonical_aco_export.build_canonical_aco_frames(default_config())
    splus_assemblies = frames.products[
        frames.products["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-splus-")
    ].copy()
    assert len(splus_assemblies) == 16
    complete_fields = [
        "flow_rate_lps",
        "water_seal_mm",
        "outlet_dn",
        "height_adj_min_mm",
        "height_adj_max_mm",
    ]
    assert splus_assemblies[complete_fields].notna().all().all()


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
        assert len(pd.read_excel(xls, sheet_name="Products")) == 88
        assert len(pd.read_excel(xls, sheet_name="BOM_Options")) == 251
        assert len(pd.read_excel(xls, sheet_name="Final_Assemblies")) == 62
        assert len(pd.read_excel(xls, sheet_name="Final_Set_Details")) == 62
        assert len(pd.read_excel(xls, sheet_name="Cplus_Compatible_Grate_Evidence")) == 30
        assert len(pd.read_excel(xls, sheet_name="Bline_Source_Evidence")) == 8
        scenario_10 = pd.read_excel(xls, sheet_name="Comparison_flow_head_10mm")
        scenario_20 = pd.read_excel(xls, sheet_name="Comparison_flow_head_20mm")
        assert len(scenario_10) == len(scenario_20) == 88
        mplus_10 = scenario_10[scenario_10["product_id"].str.startswith("aco-assembled-showerdrain-mplus-")]
        mplus_20 = scenario_20[scenario_20["product_id"].str.startswith("aco-assembled-showerdrain-mplus-")]
        assert mplus_10["flow_rate_lps"].eq(0.40).all()
        assert mplus_20["flow_rate_lps"].eq(0.46).all()

    output = capsys.readouterr().out
    assert f"Output: {destination}" in output
    assert "Products: 88" in output
    assert "BOM_Options: 251" in output
    assert "Final_Assemblies: 62" in output
    assert "Cplus_Compatible_Grate_Evidence: 30" in output
    assert "Bline_Source_Evidence: 8" in output
    assert "Cplus_assembled: 30" in output
    assert output.rstrip().endswith("OVERALL: PASS")


def test_actual_canonical_builder_promotes_old_50_32_221_pipeline_state(monkeypatch, tmp_path):
    """Regression: the CLI's real frame builder must enrich live C+ source rows."""
    import openpyxl

    from src import canonical_aco_export
    from src.config import default_config
    from src.excel_export import export_excel
    from tools.report_cplus_compatible_grate_evidence import (
        catalog_backed_cplus_products_and_components,
    )

    catalog_bases, catalog_grates = catalog_backed_cplus_products_and_components()
    existing_assembly_ids = (
        [f"aco-assembled-showerdrain-splus-base-{index}__drain-{index}" for index in range(16)]
        + [f"aco-assembled-showerdrain-c-base-{index}__grate-{index}" for index in range(4)]
        + [f"aco-assembled-easyflow-base-{index}__grate-{index}" for index in range(4)]
        + [f"aco-assembled-easyflowplus-base-{index}__grate-{index}" for index in range(4)]
    )
    ordinary = catalog_bases.to_dict("records") + [
        {
            "manufacturer": "aco",
            "product_id": f"aco-canonical-product-{index}",
            "product_name": f"Canonical product {index}",
            "flow_rate_lps": 0.6,
        }
        for index in range(16)
    ]
    assemblies = [
        {
            "manufacturer": "aco",
            "product_id": product_id,
            "product_name": product_id,
            "flow_rate_lps": 0.6,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": 80,
            "height_adj_max_mm": 120,
            "assembled_from_bom": True,
            "ready_for_benchmark": True,
            "ready_for_customer_view": False,
        }
        for product_id in existing_assembly_ids
    ]
    products = pd.DataFrame(ordinary + assemblies)
    products["flow_rate_lps"] = products["flow_rate_lps"].astype(object)
    comparison = products.copy()

    # Reproduce the live regression: all 15 grate articles exist, but only one carries
    # metadata that lets the two protected bases produce two diagnostic mappings.
    malformed_grates = catalog_grates.copy()
    malformed_grates["product_family"] = "unclassified_component"
    malformed_grates["system_role"] = "component"
    malformed_grates.loc[0, "product_family"] = "showerdrain_c_article_grate"
    malformed_grates.loc[0, "system_role"] = "grate"
    fillers = pd.DataFrame([
        {
            "manufacturer": "aco",
            "product_id": f"aco-component-{index}",
            "candidate_type": "component",
            "system_role": "accessory",
            "why_not_product_reason": "component_not_final_product",
        }
        for index in range(85)
    ])
    excluded = pd.concat([malformed_grates, fillers], ignore_index=True, sort=False)
    registry = pd.DataFrame([
        {"manufacturer": "aco", "product_id": f"aco-candidate-{index}"}
        for index in range(118)
    ])
    bom = pd.DataFrame([
        {
            "product_id": "aco-canonical-product-0",
            "component_id": f"aco-component-{index % 85}",
            "option_type": "optional_accessory",
            "product_family": "other",
        }
        for index in range(221)
    ])

    from tools.report_cplus_compatible_grate_evidence import build_export_evidence_dataframe

    assert len(build_export_evidence_dataframe(products, excluded)) == 2

    monkeypatch.setattr(
        canonical_aco_export,
        "run_discovery",
        lambda **_kwargs: (registry, pd.DataFrame()),
    )
    monkeypatch.setattr(
        canonical_aco_export,
        "run_update",
        lambda *_args, **_kwargs: (
            products, comparison, excluded, pd.DataFrame(), bom
        ),
    )

    frames = canonical_aco_export.build_canonical_aco_frames(default_config())
    assert len(frames.products) == 46
    assert len(frames.excluded) == 100
    normalized_grates = frames.excluded[
        frames.excluded["product_id"].isin(set(catalog_grates["product_id"]))
    ]
    assert len(normalized_grates) == 15
    assert normalized_grates["system_role"].eq("grate").all()
    assert normalized_grates["product_family"].eq("showerdrain_c_article_grate").all()
    assert len(build_export_evidence_dataframe(frames.products, frames.excluded)) == 30

    template = tmp_path / "template.xlsx"
    workbook = openpyxl.Workbook()
    workbook.active.title = "Candidates_All"
    workbook.save(template)
    workbook.close()
    output = tmp_path / "canonical.xlsx"
    export_excel(
        template,
        output,
        default_config(),
        registry_df=frames.registry,
        products_df=frames.products,
        comparison_df=frames.comparison,
        excluded_df=frames.excluded,
        evidence_df=frames.evidence,
        bom_options_df=frames.bom_options,
        components_df=None,
    )

    with pd.ExcelFile(output, engine="openpyxl") as xls:
        exported_products = pd.read_excel(xls, sheet_name="Products")
        exported_comparison = pd.read_excel(xls, sheet_name="Comparison")
        scoring_coverage = pd.read_excel(xls, sheet_name="Scoring_Field_Coverage")
        candidates = pd.read_excel(xls, sheet_name="Candidates_All")
        components = pd.read_excel(xls, sheet_name="Components")
        exported_bom = pd.read_excel(xls, sheet_name="BOM_Options")
        final_assemblies = pd.read_excel(xls, sheet_name="Final_Assemblies")
        final_details = pd.read_excel(xls, sheet_name="Final_Set_Details")
        eplus_evidence = pd.read_excel(xls, sheet_name="Eplus_Compatible_Grate_Evidence")
        cplus_evidence = pd.read_excel(xls, sheet_name="Cplus_Compatible_Grate_Evidence")
        bline_evidence = pd.read_excel(xls, sheet_name="Bline_Source_Evidence")
        assert len(exported_products) == 88
        assert len(exported_comparison) == 88
        assert len(scoring_coverage) == 88
        assert len(candidates) == 118
        assert len(components) == 100
        assert len(exported_bom) == 251
        assert len(final_assemblies) == 62
        assert len(final_details) == 62
        assert len(eplus_evidence) == 3
        assert len(cplus_evidence) == 30
        assert len(bline_evidence) == 8
        assert bline_evidence["safe_to_generate"].eq(False).all()
        evidence_ids = set(bline_evidence["evidence_id"])
        assert not evidence_ids & set(exported_products["product_id"])
        assert not evidence_ids & set(final_assemblies["product_id"])
        assert not evidence_ids & set(final_details["set_id"])
        assert not final_assemblies["product_id"].str.startswith("aco-assembled-showerdrain-b-").any()
        assert not final_assemblies["product_id"].str.startswith("aco-assembled-showerdrain-eplus-").any()
        assert exported_products["product_id"].str.startswith(
            "aco-assembled-showerdrain-cplus-"
        ).sum() == 30
        assert (
            exported_bom.get("product_family", pd.Series(dtype=str))
            .fillna("")
            .eq("showerdrain_cplus")
            .sum()
            == 30
        )
        for scenario_sheet in (
            "Comparison_flow_head_10mm",
            "Comparison_flow_head_20mm",
        ):
            assert len(pd.read_excel(xls, sheet_name=scenario_sheet)) == 88
