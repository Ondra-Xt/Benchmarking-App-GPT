import pandas as pd

import tools.diagnose_easyflow_source_data as mod


def test_locate_current_values_and_diagnostic_fields():
    products = pd.DataFrame([
        {
            "product_id": mod.BASE_ID,
            "flow_rate_lps": "",
            "water_seal_mm": 50.0,
            "outlet_dn": "DN50",
            "height_adj_min_mm": "",
            "height_adj_max_mm": "",
            "product_url": "https://example.test/easyflow/",
            "raw_detail": "raw source detail",
            "unrelated": "ignore",
        }
    ])

    location, row = mod.locate_product_id(mod.BASE_ID, {"Products": products})

    assert location == "Products"
    assert mod.current_values(row) == {
        "flow_rate_lps": "",
        "water_seal_mm": "50.0",
        "outlet_dn": "DN50",
        "height_adj_min_mm": "",
        "height_adj_max_mm": "",
    }
    assert mod.diagnostic_row_fields(row) == {
        "product_url": "https://example.test/easyflow/",
        "raw_detail": "raw source detail",
    }


def test_easyflow_source_urls_collects_row_registry_and_debug_urls():
    registry = pd.DataFrame([
        {
            "product_id": "aco-easyflow-item",
            "product_family": "easyflow",
            "product_name": "ACO Easyflow",
            "sources": "https://example.test/easyflow/;https://example.test/easyflow/table/?x=1",
        },
        {
            "product_id": "aco-other",
            "product_family": "other",
            "product_name": "Other",
            "sources": "https://example.test/other/",
        },
    ])
    base_row = pd.Series({"product_url": "https://example.test/base/#article-1"})
    debug = [{"final_url": "https://example.test/easyflow/debug/", "seed_url": "", "error": ""}]

    urls = mod.easyflow_source_urls(registry, debug, [base_row])

    assert urls == [
        "https://example.test/base/",
        "https://example.test/easyflow/",
        "https://example.test/easyflow/table/",
        "https://example.test/easyflow/debug/",
    ]


def test_scan_sources_classifies_structured_candidate_values(monkeypatch):
    html = """
    <html><body><main>
      <h1>ACO Easyflow DN50</h1>
      <table>
        <tr><th>Ablaufleistung</th><th>Einbauhöhe</th></tr>
        <tr><td>0,8 l/s</td><td>80-120 mm</td></tr>
      </table>
      <p>Sperrwasserhöhe 50 mm DN50</p>
    </main></body></html>
    """

    monkeypatch.setattr(mod.aco, "_safe_get_text", lambda url, timeout=35: (200, url, html, ""))
    monkeypatch.setattr(
        mod.aco,
        "extract_parameters",
        lambda url: {"flow_rate_lps": None, "height_adj_min_mm": None, "height_adj_max_mm": None, "evidence": []},
    )

    inspected, hits, candidates = mod.scan_easyflow_sources(["https://example.test/easyflow/"])
    assessments = mod.assess_missing_fields(["flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"], hits, candidates)

    assert inspected == ["https://example.test/easyflow/"]
    assert assessments["flow_rate_lps"].verdict == "source-backed and extractable"
    assert assessments["height_adj_min_mm"].verdict == "source-backed and extractable"
    assert assessments["height_adj_max_mm"].verdict == "source-backed and extractable"
    assert any(candidate.value == "0.8" and candidate.structured for candidate in assessments["flow_rate_lps"].candidates)
    assert any(candidate.value == "80" and candidate.structured for candidate in assessments["height_adj_min_mm"].candidates)
    assert any(candidate.value == "120" and candidate.structured for candidate in assessments["height_adj_max_mm"].candidates)


def test_assessments_distinguish_nonstructured_ambiguous_and_absent():
    candidates = [
        mod.CandidateEvidence(
            field="flow_rate_lps",
            value="0.7",
            url="https://example.test/easyflow/",
            snippet="Ablaufleistung 0,7 l/s",
            structured=False,
        )
    ]
    hits = [mod.SourceHit(url="https://example.test/easyflow/", term="Einbauhöhe", snippet="Einbauhöhe nach Bedarf")]

    assessments = mod.assess_missing_fields(["flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"], hits, candidates)

    assert assessments["flow_rate_lps"].verdict == "present only in non-structured text"
    assert assessments["height_adj_min_mm"].verdict == "mentioned but ambiguous"
    assert assessments["height_adj_max_mm"].verdict == "mentioned but ambiguous"

    absent = mod.assess_missing_fields(["flow_rate_lps"], [], {})
    assert absent["flow_rate_lps"].verdict == "absent"


def test_assessments_mark_multiple_structured_values_ambiguous():
    candidates = [
        mod.CandidateEvidence("flow_rate_lps", "1.5", "https://example.test/easyflow/", "Abflussleistung 1,5 l/s", True),
        mod.CandidateEvidence("flow_rate_lps", "1.0", "https://example.test/easyflow/", "Abflussleistung 1,0 l/s", True),
    ]

    assessments = mod.assess_missing_fields(["flow_rate_lps"], [], candidates)

    assert assessments["flow_rate_lps"].verdict == "mentioned but ambiguous"


def test_evidence_rows_for_product_filters_by_base_id():
    evidence = pd.DataFrame([
        {"product_id": mod.BASE_ID, "field": "water_seal_mm", "extracted_value": "50", "source_url": "u", "snippet": "s"},
        {"product_id": "other", "field": "flow_rate_lps", "extracted_value": "0.8", "source_url": "u2", "snippet": "s2"},
    ])

    rows = mod.evidence_rows_for_product(evidence, mod.BASE_ID)

    assert rows == [{"product_id": mod.BASE_ID, "field": "water_seal_mm", "extracted_value": "50", "source_url": "u", "snippet": "s"}]
