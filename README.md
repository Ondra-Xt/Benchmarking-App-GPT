# Drain Benchmark Agent

Benchmarking tool for shower drain systems (Dallmer, Hansgrohe, TECE, etc.).

---

## Setup (Windows)

Run once:

```bat
setup_windows.bat
```

This will:
- create local `.venv`
- install dependencies from `requirements.txt`

---

## Run application

```bat
run_windows.bat
```

or manually:

```bash
.venv\Scripts\activate
streamlit run app.py
```

---

## Important

- `.venv` is local only (not part of repository)
- Generated runs are stored in `data/runs/` (ignored by git)
- Do not commit `.venv` or runtime artifacts

---

## Architecture Overview

- `src/connectors/` – vendor connectors
- `pipeline.py` – discovery + update orchestration
- `scoring.py` – scoring logic
- `excel_export.py` – Excel export
- `pdf_text.py` – PDF parsing
---

## Validation

Pro kontrolu konzistence exportu použijte skript:

```bash
python scripts/validate_export.py --xlsx data/runs/<run_id>/outputs/benchmark_output.xlsx
```

Bez argumentu `--xlsx` skript automaticky vyhledá nejnovější export v `data/runs/*/outputs/*.xlsx` a zkontroluje invariants:

```bash
python scripts/validate_export.py
```

Skript vždy vypíše `PASS` nebo `FAIL` a vrací exit code `0` (PASS) / `1` (FAIL).

Skript navíc vypisuje `WARN` pro podezřelé hodnoty výšek (`height_adj_*`) v sheetech `Products` i `Components` (např. velmi nízké max výšky nebo kombinace tile-depth + instalační výšky).

Pokud v `Products` nebo `Components` chybí `height_adj_min_mm` nebo `height_adj_max_mm`, validátor vypíše seznam dotčených řádků (manufacturer, product_id, candidate_type, product_url).


## Scoring note

Finální skóre nyní obsahuje i `system_score` (kompletnost systému):
- `finish_set`: `1.0` pokud existují BOM options, jinak `0.5`
- `drain`: `1.0`
- ostatní: `0.5`

Vzorec: `final_score = 0.7*param_score + 0.2*system_score + 0.1*equiv_score` (pokud není v konfiguraci přepsáno).
