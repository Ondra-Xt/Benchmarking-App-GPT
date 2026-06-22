

def _write_session_snapshot(path, *, products=2, comparison=1, candidates=3):
    import pandas as pd
    product_rows = pd.DataFrame({
        "product_id": [f"p{i}" for i in range(products)],
        "system_role": ["channel"] * products,
    })
    comparison_rows = pd.DataFrame({"product_id": [f"p{i}" for i in range(comparison)]})
    candidate_rows = pd.DataFrame({"product_id": [f"c{i}" for i in range(candidates)]})
    bom_rows = pd.DataFrame({"product_id": ["p0"], "option_product_id": ["p1"], "option_type": ["optional_accessory"]})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        candidate_rows.to_excel(writer, sheet_name="Candidates_All", index=False)
        product_rows.to_excel(writer, sheet_name="Products", index=False)
        comparison_rows.to_excel(writer, sheet_name="Comparison", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Excluded", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Evidence", index=False)
        bom_rows.to_excel(writer, sheet_name="BOM_Options", index=False)


def test_session_snapshot_validator_accepts_noncanonical_counts(tmp_path):
    from src.app_export import validate_session_snapshot_xlsx

    workbook = tmp_path / "snapshot.xlsx"
    _write_session_snapshot(workbook, products=58, comparison=108, candidates=295)

    passed, results = validate_session_snapshot_xlsx(workbook)

    assert passed, [result for result in results if not result.passed]


def test_session_snapshot_validator_rejects_empty_and_malformed_workbooks(tmp_path):
    from src.app_export import validate_session_snapshot_xlsx

    malformed = tmp_path / "malformed.xlsx"
    malformed.write_bytes(b"not an xlsx")
    malformed_passed, malformed_results = validate_session_snapshot_xlsx(malformed)
    assert malformed_passed is False
    assert any(result.name == "session_snapshot_open_workbook" for result in malformed_results)

    empty = tmp_path / "empty.xlsx"
    _write_session_snapshot(empty, products=0, comparison=0, candidates=0)
    empty_passed, empty_results = validate_session_snapshot_xlsx(empty)
    assert empty_passed is False
    assert any(result.name == "session_snapshot_not_empty" and not result.passed for result in empty_results)


def test_session_snapshot_export_uses_structural_validator_not_canonical_fixed_count(monkeypatch, tmp_path):
    import pandas as pd
    from src import app_export
    from src.config import default_config

    calls = {"session": 0, "baseline": 0, "full": 0}

    def fake_export_excel(_template, out_path, _cfg, **_kwargs):
        _write_session_snapshot(out_path, products=109, comparison=108, candidates=295)

    def fake_session_validator(path):
        calls["session"] += 1
        return True, []

    def forbidden_validator(*_args, **_kwargs):
        raise AssertionError("canonical fixed-count validator must not be called")

    monkeypatch.setattr(app_export, "export_excel", fake_export_excel)
    monkeypatch.setattr(app_export, "validate_session_snapshot_xlsx", fake_session_validator)
    monkeypatch.setattr(app_export, "validate_app_export_baseline", forbidden_validator)
    monkeypatch.setattr(app_export, "validate_xlsx", forbidden_validator)

    destination = tmp_path / "benchmark_session_snapshot.xlsx"
    app_export.export_session_snapshot_workbook(
        tmp_path / "template.xlsx",
        destination,
        default_config(),
        registry=pd.DataFrame({"product_id": ["c0"]}),
        products=pd.DataFrame({"product_id": ["p0"]}),
        comparison=pd.DataFrame({"product_id": ["p0"]}),
        excluded=pd.DataFrame(),
        evidence=pd.DataFrame(),
        bom_options=pd.DataFrame(),
    )

    assert calls["session"] == 1
    assert destination.exists()
