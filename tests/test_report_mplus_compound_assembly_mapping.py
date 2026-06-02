from dataclasses import replace

import pandas as pd

import tools.report_mplus_compound_assembly_mapping as mod
from tools.diagnose_mplus_base_row_sources import MPlusCandidateRow


def _candidate(article, pid, candidate_type, role, source_url, **fields):
    return MPlusCandidateRow(
        article_number=article,
        proposed_product_id=pid,
        candidate_type=candidate_type,
        product_family="showerdrain_mplus",
        system_role=role,
        source_url=source_url,
        row_text=f"ACO ShowerDrain M+ {article}",
        confidence=fields.pop("confidence", "high"),
        **fields,
    )


def test_build_report_dedupes_and_maps_channel_drain_grate_without_accessories(monkeypatch):
    channel = _candidate(
        "9010.90.10",
        "aco-channel",
        "mplus_channel_body_candidate",
        "profile_channel",
        "https://example.test/mplus/channel/",
        height_adj_min_mm="25",
        height_adj_max_mm="128",
    )
    drains = (
        _candidate("9010.81.20", "aco-drain-a", "mplus_drain_body_candidate", "drain_body", "https://example.test/mplus/drains/", water_seal_mm="50", outlet_dn="DN50"),
        _candidate("9010.81.20", "aco-drain-a-duplicate-rendering", "mplus_drain_body_candidate", "drain_body", "https://example.test/mplus/drains/#dup", water_seal_mm="50", outlet_dn="DN50"),
        _candidate("9010.81.21", "aco-drain-b", "mplus_drain_body_candidate", "drain_body", "https://example.test/mplus/drains/", water_seal_mm="30", outlet_dn="DN50"),
        _candidate("", "", "mplus_drain_body_candidate", "drain_body", "https://example.test/mplus/generic-drain-page/", water_seal_mm="", outlet_dn=""),
    )
    grate = _candidate("9010.90.30", "aco-grate", "mplus_grate_component", "grate", "https://example.test/mplus/grate/")
    accessory = _candidate("9010.90.40", "aco-accessory", "mplus_accessory", "accessory", "https://example.test/mplus/accessory/")

    class FakeDiag:
        channel_body_candidates = (channel, channel)
        drain_body_candidates = drains
        grate_candidates = (grate, grate)
        accessory_candidates = (accessory,)

    monkeypatch.setattr(mod.mplus_sources, "build_diagnostic", lambda *args: FakeDiag())

    products = pd.DataFrame([{"product_id": "existing-product"}])
    final_assemblies = pd.DataFrame([{"product_id": "existing-assembly"}])
    final_set_details = pd.DataFrame([{"set_id": "existing-set", "assembled_product_id": "existing-assembled"}])
    report = mod.build_report(
        candidates_all=pd.DataFrame([{"product_id": "candidate"}]),
        products=products,
        comparison=pd.DataFrame(),
        components=pd.DataFrame(),
        bom_options=pd.DataFrame(),
        final_assemblies=final_assemblies,
        final_set_details=final_set_details,
    )

    assert report.raw_candidate_counts == {
        "channel_bodies": 2,
        "drain_bodies": 4,
        "grates": 2,
        "optional_accessories": 1,
    }
    assert report.deduplicated_candidate_counts == {
        "channel_bodies": 1,
        "drain_bodies": 3,
        "article_level_drain_bodies": 2,
        "generic_drain_body_pages": 1,
        "grates": 1,
        "optional_accessories": 1,
    }
    assert len(report.proposed_mappings) == 2
    assert len(report.article_level_drain_bodies) == 2
    assert len(report.generic_drain_body_pages) == 1
    assert all(row.drain_body_article_number for row in report.proposed_mappings)
    assert [row.proposed_compound_set_id for row in report.proposed_mappings] == [
        "diagnostic-mplus-aco-channel__aco-drain-a__aco-grate",
        "diagnostic-mplus-aco-channel__aco-drain-b__aco-grate",
    ]
    assert all(row.safe_to_generate is False for row in report.proposed_mappings)
    assert report.safe_to_generate_counts == {"safe": 0, "blocked": 2}
    assert report.missing_technical_field_summary == {
        "flow_rate_lps": 2,
        "water_seal_mm": 0,
        "outlet_dn": 0,
        "height_adj_min_mm": 0,
        "height_adj_max_mm": 0,
    }
    assert report.risk_checks.optional_accessories_in_required_parts == ()
    assert report.risk_checks.proposed_ids_overlapping_products == ()
    assert report.production_behavior_changed is False


def test_risk_checks_report_duplicate_ids_missing_ids_overlaps_and_missing_fields(monkeypatch):
    channel = _candidate("", "", "mplus_channel_body_candidate", "profile_channel", "")
    drain = _candidate("9010.81.20", "aco-90108120", "mplus_drain_body_candidate", "drain_body", "", water_seal_mm="50")
    grate = _candidate("", "", "mplus_grate_component", "grate", "")

    mappings = mod.build_proposed_mappings((channel,), (drain,), (grate,))
    duplicate_mappings = tuple(replace(row, drain_body_id="") for row in mappings + mappings)
    proposed_id = duplicate_mappings[0].proposed_compound_set_id
    risks = mod._risk_checks(
        duplicate_mappings,
        optional_accessories=(),
        products=pd.DataFrame([{"product_id": proposed_id}]),
        final_assemblies=pd.DataFrame([{"product_id": proposed_id}]),
        final_set_details=pd.DataFrame([{"set_id": proposed_id, "assembled_product_id": "other"}]),
    )

    assert risks.duplicate_proposed_compound_set_ids == (proposed_id,)
    assert risks.missing_channel_body_ids == 2
    assert risks.missing_drain_body_ids == 2
    assert risks.missing_grate_ids == 2
    assert risks.missing_required_technical_fields == (
        "flow_rate_lps",
        "height_adj_max_mm",
        "height_adj_min_mm",
        "outlet_dn",
    )
    assert risks.proposed_ids_overlapping_products == (proposed_id,)
    assert risks.proposed_ids_overlapping_final_assemblies == (proposed_id,)
    assert risks.proposed_ids_overlapping_final_set_details == (proposed_id,)


def test_print_report_includes_required_diagnostic_sections(monkeypatch, capsys):
    class FakeDiag:
        channel_body_candidates = ()
        drain_body_candidates = ()
        grate_candidates = ()
        accessory_candidates = ()

    monkeypatch.setattr(mod.mplus_sources, "build_diagnostic", lambda *args: FakeDiag())
    report = mod.build_report(
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
    )

    mod.print_report(report)
    out = capsys.readouterr().out
    assert "Input sheet/frame counts" in out
    assert "M+ raw candidate counts by class" in out
    assert "M+ deduplicated candidate counts by class" in out
    assert "Proposed compound mapping count: 0" in out
    assert "Mapping confidence summary" in out
    assert "safe_to_generate counts" in out
    assert "Missing technical-field summary" in out
    assert "Article-level drain bodies: 0" in out
    assert "Generic drain body pages: 0" in out
    assert "Generic / non-article drain body evidence" in out
    assert "Optional accessory count: 0" in out
    assert "Risk checks" in out
    assert "Recommended next action" in out
    assert "production behavior changed: no" in out


def test_article_level_drain_split_excludes_generic_page_and_uses_stable_ids(monkeypatch):
    channel = _candidate(
        "",
        "",
        "mplus_channel_body_candidate",
        "profile_channel",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/",
        height_adj_min_mm="25",
        height_adj_max_mm="128",
    )
    drains = tuple(
        _candidate(
            article,
            f"aco-{article.replace('.', '')}",
            "mplus_drain_body_candidate",
            "drain_body",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/",
            water_seal_mm="50",
            outlet_dn="DN50",
        )
        for article in ("9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23")
    )
    generic_page = _candidate(
        "",
        "",
        "mplus_drain_body_candidate",
        "drain_body",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/",
        water_seal_mm="",
        outlet_dn="",
        evidence_type="source_page_text",
    )
    grate = _candidate(
        "",
        "",
        "mplus_grate_component",
        "grate",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/",
    )

    class FakeDiag:
        channel_body_candidates = (channel,)
        drain_body_candidates = drains + (generic_page,)
        grate_candidates = (grate,)
        accessory_candidates = ()

    monkeypatch.setattr(mod.mplus_sources, "build_diagnostic", lambda *args: FakeDiag())

    report = mod.build_report(
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
    )

    assert [row.proposed_product_id for row in report.article_level_drain_bodies] == [
        "aco-90108120",
        "aco-90108121",
        "aco-90108122",
        "aco-90108123",
    ]
    assert report.generic_drain_body_pages == (generic_page,)
    assert [row.proposed_compound_set_id for row in report.proposed_mappings] == [
        "diagnostic-mplus-channel-body-25-128__aco-90108120__mplus-design-roste-elektropoliert",
        "diagnostic-mplus-channel-body-25-128__aco-90108121__mplus-design-roste-elektropoliert",
        "diagnostic-mplus-channel-body-25-128__aco-90108122__mplus-design-roste-elektropoliert",
        "diagnostic-mplus-channel-body-25-128__aco-90108123__mplus-design-roste-elektropoliert",
    ]
    assert len(report.proposed_mappings) == 4
    assert not any("http" in row.proposed_compound_set_id for row in report.proposed_mappings)
    assert all(row.source_url_channel_body for row in report.proposed_mappings)
    assert all(row.source_url_drain_body for row in report.proposed_mappings)
    assert all(row.source_url_grate for row in report.proposed_mappings)
    assert report.safe_to_generate_counts == {"safe": 0, "blocked": 4}
    assert report.missing_technical_field_summary["flow_rate_lps"] == 4
    assert report.missing_technical_field_summary["water_seal_mm"] == 0
    assert report.missing_technical_field_summary["outlet_dn"] == 0
