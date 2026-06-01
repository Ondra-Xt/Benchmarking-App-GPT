import pandas as pd

import tools.diagnose_cplus_compatible_grate_evidence as mod


def test_locate_cplus_base_rows_reports_required_technical_fields():
    products = pd.DataFrame([
        {
            "product_id": "aco-showerdrain-cplus-standard-h92",
            "flow_rate_lps": 0.91,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": 80,
            "height_adj_max_mm": 128,
        },
        {
            "product_id": "aco-showerdrain-cplus-low-h69",
            "flow_rate_lps": 0.62,
            "water_seal_mm": 25,
            "outlet_dn": "DN40",
            "height_adj_min_mm": 57,
            "height_adj_max_mm": 128,
        },
    ])

    rows, missing = mod.locate_cplus_base_rows(products)

    assert missing == ()
    assert rows["aco-showerdrain-cplus-standard-h92"] == {
        "flow_rate_lps": "0.91",
        "water_seal_mm": "50",
        "outlet_dn": "DN50",
        "height_adj_min_mm": "80",
        "height_adj_max_mm": "128",
    }
    assert rows["aco-showerdrain-cplus-low-h69"]["height_adj_min_mm"] == "57"


def test_collect_related_source_urls_includes_known_cplus_and_related_grate_sources():
    candidates = pd.DataFrame([
        {
            "product_id": "aco-90108801",
            "product_family": "showerdrain_c_article_grate",
            "system_role": "grate",
            "source_url": "https://www.aco.example/c/design-roste/",
        },
        {
            "product_id": "unrelated",
            "product_family": "other",
            "source_url": "https://www.aco.example/unrelated/",
        },
    ])
    bom = pd.DataFrame([
        {
            "product_id": "aco-showerdrain-cplus-standard-h92",
            "component_id": "aco-90108801",
            "option_type": "compatible_grate",
            "parent_family": "showerdrain_cplus",
            "source_url": "https://www.aco.example/cplus/matrix/",
        }
    ])

    urls = mod.collect_related_source_urls(candidates, bom)

    assert "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/" in urls
    assert "https://www.aco.example/c/design-roste/" in urls
    assert "https://www.aco.example/cplus/matrix/" in urls
    assert "https://www.aco.example/unrelated/" not in urls


def test_find_candidate_evidence_classifies_explicit_article_matrix(monkeypatch):
    html = """
    <html><body><table><tr>
    <td>ACO ShowerDrain C+ compatible Design-Rost</td>
    <td>Artikel 9010.88.01</td>
    </tr></table></body></html>
    """

    def fake_get(url, timeout=35):
        return 200, url, html, None

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)
    components = pd.DataFrame([
        {
            "product_id": "aco-90108801",
            "article_no": "9010.88.01",
            "system_role": "grate",
            "product_name": "Design-Rost",
            "source_url": "https://example.test/cplus-grates/",
        }
    ])

    inspections, evidence = mod.find_candidate_evidence(["https://example.test/cplus-grates/"], components)

    assert inspections[0].status == "ok"
    explicit = [ev for ev in evidence if ev.evidence_type == "explicit_article_matrix"]
    assert explicit
    assert explicit[0].compatibility_confidence == "explicit"
    assert explicit[0].product_id == "aco-90108801"


def test_find_candidate_evidence_keeps_showerdrain_c_inheritance_ambiguous(monkeypatch):
    html = """
    <html><body><table><tr>
    <td>Design-Rost passend für ACO ShowerDrain C</td>
    <td>Artikel 9010.88.01</td>
    </tr></table></body></html>
    """

    def fake_get(url, timeout=35):
        return 200, url, html, None

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _inspections, evidence = mod.find_candidate_evidence(
        ["https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/design-roste-aus-geschliffenem-edelstahl/"]
    )

    assert any(ev.evidence_type == "ambiguous" and ev.compatibility_confidence == "ambiguous" for ev in evidence)
    assert not any(ev.evidence_type == "explicit_article_matrix" for ev in evidence)


def test_build_diagnostic_recommends_no_bom_rows_without_explicit_article_evidence(monkeypatch):
    def fake_find(source_urls, *frames):
        return (
            (mod.SourceInspection("https://example.test/cplus/", "ok", ("ACO ShowerDrain C+ výběr z několika roštů",)),),
            (mod.CandidateEvidence("", "", "https://example.test/cplus/", "ACO ShowerDrain C+ výběr z několika roštů", "implicit_family_level", "implicit"),),
        )

    monkeypatch.setattr(mod, "find_candidate_evidence", fake_find)
    products = pd.DataFrame([
        {
            "product_id": "aco-showerdrain-cplus-standard-h92",
            "product_family": "showerdrain_cplus",
            "flow_rate_lps": 0.91,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": 80,
            "height_adj_max_mm": 128,
            "source_url": "https://example.test/cplus/",
        },
        {
            "product_id": "aco-showerdrain-cplus-low-h69",
            "product_family": "showerdrain_cplus",
            "flow_rate_lps": 0.62,
            "water_seal_mm": 25,
            "outlet_dn": "DN40",
            "height_adj_min_mm": 57,
            "height_adj_max_mm": 128,
            "source_url": "https://example.test/cplus/",
        },
    ])

    diag = mod.build_diagnostic(pd.DataFrame(), products, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    assert diag.safe_to_add_compatible_grate_bom_rows is False
    assert "Do not add" in diag.recommendation


def test_run_diagnostic_invokes_discovery_and_pipeline_with_required_args(monkeypatch):
    calls = {}

    def fake_discover_candidates(target_length_mm, tolerance_mm):
        calls["discover"] = (target_length_mm, tolerance_mm)
        return ([{"manufacturer": "aco", "product_id": "seed", "product_url": "https://example.test/seed"}], [])

    def fake_run_update(registry_df, cfg, target_length_mm, tolerance_mm, selected_connectors):
        calls["run_update"] = (len(registry_df), target_length_mm, tolerance_mm, tuple(selected_connectors))
        products = pd.DataFrame([
            {
                "product_id": "aco-showerdrain-cplus-standard-h92",
                "flow_rate_lps": 0.91,
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "height_adj_min_mm": 80,
                "height_adj_max_mm": 128,
            },
            {
                "product_id": "aco-showerdrain-cplus-low-h69",
                "flow_rate_lps": 0.62,
                "water_seal_mm": 25,
                "outlet_dn": "DN40",
                "height_adj_min_mm": 57,
                "height_adj_max_mm": 128,
            },
        ])
        return products, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    monkeypatch.setattr(mod.aco, "discover_candidates", fake_discover_candidates)
    monkeypatch.setattr(mod.pipeline, "run_update", fake_run_update)
    monkeypatch.setattr(mod, "find_candidate_evidence", lambda source_urls, *frames: ((), ()))

    diag = mod.run_diagnostic()

    assert calls["discover"] == (1200, 100)
    assert calls["run_update"] == (1, 1200, 100, ("aco",))
    assert diag.missing_base_ids == ()
