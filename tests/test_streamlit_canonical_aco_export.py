from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit
from streamlit.testing.v1 import AppTest

from src.app_export import AppExportValidationError
from src.config import default_config
from src.run_manager import RunPaths
from tools.validate_xlsx_export import CheckResult


DOWNLOAD_KEY = "canonical_aco_workbook_bytes"
BUILD_LABEL = "Build canonical ACO Excel"


def _patch_app_dependencies(monkeypatch, tmp_path: Path, exporter):
    from src import canonical_aco_export, run_manager

    downloads: list[dict[str, object]] = []

    def fake_create_run_dirs(_base_data_dir: Path, run_id: str) -> RunPaths:
        run_dir = tmp_path / run_id
        paths = RunPaths(
            run_id=run_id,
            run_dir=run_dir,
            html_dir=run_dir / "pages" / "html",
            pdf_dir=run_dir / "pages" / "pdf",
            logs_dir=run_dir / "logs",
            outputs_dir=run_dir / "outputs",
        )
        for path in (paths.html_dir, paths.pdf_dir, paths.logs_dir, paths.outputs_dir):
            path.mkdir(parents=True, exist_ok=True)
        return paths

    def fake_download_button(label, **kwargs):
        downloads.append({"label": label, **kwargs})
        return False

    monkeypatch.setattr(canonical_aco_export, "export_canonical_aco_workbook", exporter)
    monkeypatch.setattr(run_manager, "create_run_dirs", fake_create_run_dirs)
    monkeypatch.setattr(streamlit, "download_button", fake_download_button)
    return downloads


def _button(app: AppTest, label: str):
    return next(button for button in app.button if button.label == label)


def test_products_58_session_builds_canonical_export_without_passing_session_frames(
    monkeypatch, tmp_path
):
    calls: list[tuple[Path, Path, object]] = []

    def validated_export(template_path, output_path, cfg):
        calls.append((Path(template_path), Path(output_path), cfg))
        Path(output_path).write_bytes(b"validated canonical workbook")
        return Path(output_path)

    downloads = _patch_app_dependencies(monkeypatch, tmp_path, validated_export)
    app = AppTest.from_file("app.py").run(timeout=30)
    app.session_state["products"] = pd.DataFrame(
        {"product_id": [f"live-product-{index}" for index in range(58)]}
    )

    _button(app, BUILD_LABEL).click()
    app.run(timeout=30)

    assert not app.exception
    assert len(calls) == 1
    template_path, output_path, cfg = calls[0]
    assert template_path.name == "benchmark_template.xlsx"
    assert output_path.parent.name == "outputs"
    assert output_path.name == "benchmark_aco_canonical.xlsx"
    assert cfg == default_config()
    assert app.session_state["products"].shape[0] == 58
    assert app.session_state[DOWNLOAD_KEY] == b"validated canonical workbook"
    assert len(downloads) == 1
    assert downloads[0]["file_name"] == "benchmark_aco_canonical.xlsx"
    assert downloads[0]["data"] == b"validated canonical workbook"


def test_download_bytes_persist_and_download_is_offered_on_rerun(monkeypatch, tmp_path):
    def validated_export(_template_path, output_path, _cfg):
        Path(output_path).write_bytes(b"persistent workbook bytes")
        return Path(output_path)

    downloads = _patch_app_dependencies(monkeypatch, tmp_path, validated_export)
    app = AppTest.from_file("app.py").run(timeout=30)
    _button(app, BUILD_LABEL).click()
    app.run(timeout=30)

    assert len(downloads) == 1
    downloads.clear()
    app.run(timeout=30)

    assert not app.exception
    assert app.session_state[DOWNLOAD_KEY] == b"persistent workbook bytes"
    assert len(downloads) == 1
    assert downloads[0]["data"] == b"persistent workbook bytes"


def test_validation_failure_shows_exact_error_and_does_not_offer_download(
    monkeypatch, tmp_path
):
    validation_error = AppExportValidationError(
        [CheckResult("Products row count", False, "expected 88, got 58")],
        input_description="canonical ACO pipeline output",
    )

    def rejected_export(_template_path, _output_path, _cfg):
        raise validation_error

    downloads = _patch_app_dependencies(monkeypatch, tmp_path, rejected_export)
    app = AppTest.from_file("app.py").run(timeout=30)
    app.session_state[DOWNLOAD_KEY] = b"stale workbook"
    _button(app, BUILD_LABEL).click()
    app.run(timeout=30)

    assert not app.exception
    assert [error.value for error in app.error] == [str(validation_error)]
    assert DOWNLOAD_KEY not in app.session_state.filtered_state
    assert downloads == []
    assert not list(tmp_path.rglob("*.xlsx"))


def test_discovery_update_and_session_reset_invalidate_stale_download(
    monkeypatch, tmp_path
):
    def unused_export(_template_path, _output_path, _cfg):
        raise AssertionError("canonical export should not run")

    downloads = _patch_app_dependencies(monkeypatch, tmp_path, unused_export)

    from src import pipeline

    monkeypatch.setattr(
        pipeline,
        "run_discovery",
        lambda **_kwargs: (pd.DataFrame(), pd.DataFrame()),
    )

    app = AppTest.from_file("app.py").run(timeout=30)
    app.session_state[DOWNLOAD_KEY] = b"stale"
    _button(app, "Run discovery").click()
    app.run(timeout=30)
    assert DOWNLOAD_KEY not in app.session_state.filtered_state
    assert downloads == []

    app.session_state[DOWNLOAD_KEY] = b"stale"
    _button(app, "Run update").click()
    app.run(timeout=30)
    assert DOWNLOAD_KEY not in app.session_state.filtered_state
    assert downloads == []

    app.session_state[DOWNLOAD_KEY] = b"stale"
    _button(app, "Reset in-app session state").click()
    app.run(timeout=30)
    assert DOWNLOAD_KEY not in app.session_state.filtered_state
    assert downloads == []
