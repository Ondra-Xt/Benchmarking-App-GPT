from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone

@dataclass
class RunPaths:
    run_id: str
    run_dir: Path
    html_dir: Path
    pdf_dir: Path
    logs_dir: Path
    outputs_dir: Path

def utc_run_id(prefix: str = "run") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}"

def create_run_dirs(base_data_dir: Path, run_id: str) -> RunPaths:
    run_dir = base_data_dir / "runs" / run_id
    html_dir = run_dir / "pages" / "html"
    pdf_dir = run_dir / "pages" / "pdf"
    logs_dir = run_dir / "logs"
    outputs_dir = run_dir / "outputs"
    for p in [html_dir, pdf_dir, logs_dir, outputs_dir]:
        p.mkdir(parents=True, exist_ok=True)
    return RunPaths(run_id=run_id, run_dir=run_dir, html_dir=html_dir, pdf_dir=pdf_dir, logs_dir=logs_dir, outputs_dir=outputs_dir)
