from __future__ import annotations

import pandas as pd

from tools import report_eplus_compatible_grate_evidence as mod


def _proposals() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "set_id": f"diagnostic-eplus-body-{index}__grate",
            "product_family": "showerdrain_eplus",
            "assembly_model": "base_x_grate",
            "body_id": f"body-{index}",
            "body_article_number": "",
            "body_source_url": f"https://example.test/eplus/body-{index}",
            "grate_id": "grate",
            "grate_article_number": "",
            "grate_source_url": "https://example.test/eplus/grate",
        }
        for index in range(3)
    ])


def test_family_page_is_non_explicit_and_unsafe():
    result = mod.inspect_source_text("ACO ShowerDrain E+ Design-Roste und Rinnenkörper", "family.html")

    assert result.eplus_mentioned is True
    assert result.explicit_article_level_link_found is False
    assert result.evidence_classification == "family_level_only"
    assert result.confidence == "low"


def test_article_cooccurrence_without_link_is_not_explicit():
    result = mod.inspect_source_text(
        "ACO ShowerDrain E+ Rinnenkörper 9010.10.10\nACO ShowerDrain E+ Design-Rost 9010.20.20",
        "catalog.txt",
    )

    assert result.body_articles == ("9010.10.10",)
    assert result.grate_articles == ("9010.20.20",)
    assert result.explicit_article_level_link_found is False
    assert result.evidence_classification == "article_numbers_without_explicit_compatibility"


def test_explicit_same_row_article_link_is_detected_but_not_promoted():
    result = mod.inspect_source_text(
        "ACO ShowerDrain E+ Rinnenkörper 9010.10.10 compatible -> Design-Rost 9010.20.20",
        "matrix.txt",
    )

    assert result.explicit_article_level_link_found is True
    assert result.evidence_classification == "explicit_article_level_body_to_grate_link"
    assert result.confidence == "high"


def test_export_evidence_is_diagnostic_only_and_matches_proposals():
    proposals = _proposals()
    evidence = mod.build_export_evidence_dataframe(proposals)

    assert len(evidence) == 3
    assert list(evidence.columns) == list(mod.EVIDENCE_COLUMNS)
    assert set(evidence["set_id"]) == set(proposals["set_id"])
    assert set(evidence["compatibility_evidence_type"]) == {"no_explicit_article_level_matrix_found"}
    assert set(evidence["compatibility_confidence"]) == {"low"}
    for column in ("article_level_compatibility_found", "safe_to_generate", "ready_for_benchmark", "ready_for_customer_view"):
        assert set(evidence[column]) == {False}
    assert evidence["blocking_reason"].str.contains("Explicit article-level E+ base-to-grate compatibility is missing", regex=False).all()
    assert evidence["production_status_note"].str.contains("diagnostic-only", regex=False).all()
