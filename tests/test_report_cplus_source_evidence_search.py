from pathlib import Path

import tools.report_cplus_source_evidence_search as mod


def test_source_audit_finds_explicit_2025_catalog_matrix_and_protected_bases():
    rows = mod.build_source_audit()
    mappings = mod.explicit_catalog_mappings()

    assert rows
    catalog = next(row for row in rows if row.source_id == "cplus_showerdrain_catalog_2025_cz")
    assert catalog.fetch_or_parse_status == "ok"
    assert catalog.evidence_classification == "explicit_catalog_matrix"
    assert catalog.evidence_confidence == "high"
    assert set(mod.PROTECTED_CPLUS_BASE_IDS).issubset(set(catalog.detected_base_ids.split(", ")))
    assert "9010.85.20" in catalog.detected_base_article_numbers
    assert "9010.85.40" in catalog.detected_base_article_numbers
    assert "9010.88.61" in catalog.detected_grate_article_numbers
    assert mappings
    assert all(mapping.evidence_classification == "explicit_catalog_matrix" for mapping in mappings)


def test_explicit_catalog_mappings_are_length_matched_and_exact():
    mappings = mod.explicit_catalog_mappings()
    by_key = {(m.base_id, m.grate_article_number): m for m in mappings}

    standard = by_key[("aco-showerdrain-cplus-standard-h92", "9010.88.61")]
    low = by_key[("aco-showerdrain-cplus-low-h69", "9010.88.61")]
    assert standard.nominal_length_mm == 685
    assert standard.base_article_numbers == ("9010.85.20", "9010.85.30")
    assert low.base_article_numbers == ("9010.85.40", "9010.85.50")
    assert ("aco-showerdrain-cplus-standard-h92", "9010.88.60") not in by_key
    assert all(not m.grate_article_number.startswith("9010.85.") for m in mappings)


def test_shared_c_grate_page_remains_inferred_and_body_pages_are_not_grates():
    rows = {row.source_id: row for row in mod.build_source_audit()}

    shared = rows["c_design_grates_de"]
    assert shared.evidence_classification == "inferred_from_shared_c_grate_page"
    assert shared.evidence_confidence == "low"
    body = rows["c_standard_h92_de"]
    assert body.evidence_classification == "insufficient"
    assert not body.detected_grate_article_numbers


def test_synthetic_explicit_table_requires_cplus_scope_and_matching_articles(tmp_path: Path):
    # A synthetic PDF is unnecessary for parser coverage: mutate extracted page text
    # through a small PdfReader stand-in and verify strict heading/table requirements.
    class Page:
        def __init__(self, text: str): self.text = text
        def extract_text(self): return self.text

    pages = [Page("") for _ in range(26)]
    pages[24] = Page(
        "Objednávková data, žlab 685 9010.85.20 685 9010.85.30 "
        "Sprchové žlaby ACO ShowerDrain C+"
    )
    pages[25] = Page(
        "Objednávková data, nerezové krycí rošty 685 9010.88.61 "
        "Sprchové žlaby ACO ShowerDrain C & C+"
    )
    fake = tmp_path / mod.CATALOG_FIXTURE.name
    fake.write_bytes(b"fixture")

    original = mod.PdfReader
    mod.PdfReader = lambda _path: type("Reader", (), {"pages": pages})()
    try:
        mappings = mod.explicit_catalog_mappings(fake)
    finally:
        mod.PdfReader = original

    assert {(m.base_id, m.grate_article_number) for m in mappings} == {
        ("aco-showerdrain-cplus-standard-h92", "9010.88.61")
    }
    assert mappings[0].base_article_numbers == ("9010.85.20", "9010.85.30")


def test_main_prints_explicit_finding_with_production_generation(capsys):
    assert mod.main([]) == 0
    out = capsys.readouterr().out
    assert "Explicit article-level compatibility found: YES" in out
    assert "C+ production assemblies generated: YES" in out
    assert "Customer-facing C+ output changed: NO" in out
    assert "9010.85.20,9010.85.30 -> grate articles" in out
