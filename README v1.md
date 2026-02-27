# Drain Systems Benchmark – MVP

## Spuštění (lokálně)
1) Vytvoř a aktivuj virtuální prostředí
- Windows: `python -m venv .venv` + `.venv\Scripts\activate`
- macOS/Linux: `python3 -m venv .venv` + `source .venv/bin/activate`

2) Instalace závislostí

Doporučený způsob (funguje spolehlivě i když je v PATH více Pythonů):

- Windows: `py -m pip install -r requirements.txt`
- macOS/Linux: `python3 -m pip install -r requirements.txt`

Poznámka: příkaz typu `python pip install ...` nebo `py pip install ...` je špatně – Python se pak snaží spustit soubor `pip` v aktuální složce.

3) Spuštění
`streamlit run app.py`

## Troubleshooting: Discovery vrací 0 kandidátů

Od verze v3 se při „Run discovery“ ukládá soubor `discovery_debug.csv` do složky `data/runs/.../outputs/`.
V aplikaci se zároveň zobrazí tabulka s HTTP status kódy a případnými chybami.

Typické příčiny:
- firemní proxy (HTTP/HTTPS) – knihovna requests nemusí automaticky převzít systémové proxy;
  aplikace se nyní snaží proxy načíst ze systémového nastavení (Windows) přes urllib.
- blokace botů / rate limiting (403/429) – je potřeba doplnit backoff, případně přejít na prohlížečový režim
  (Playwright/Selenium) – to je plán pro další iteraci.

## Diagnostika discovery (když najde 0 kandidátů)
Po Run discovery se do `data/runs/<run_id>/outputs/` uloží `discovery_debug.csv`, kde jsou status kódy / chyby HTTP požadavků.
V korporátních sítích bývá častý problém systémový proxy server – aplikace se jej nyní snaží automaticky převzít z OS nastavení.

## Data
- `data/config/weights.json` – uložené váhy a penalizace
- `data/runs/<run_id>/...` – snapshoty běhů (registry, výsledky, logy)
- `data/templates/benchmark_template.xlsx` – Excel šablona

## Aktuálně implementované weby (v1)
- Dallmer (heuristická extrakce)
- hansgrohe (heuristická extrakce)

Discovery a extrakce jsou MVP; během prvních reálných běhů doplníme robustní parsování a BOМ varianty.


## Průtok (flow rate)
- Ukládá se jako `flow_rate_lps` (l/s), zaokrouhlené na 2 desetinná místa.
- Přepočty: l/min → l/s (÷60), m³/h → l/s (×1000/3600), l/h → l/s (÷3600).
- Hodnoty mimo 0.3–1.4 l/s jsou označeny jako `rejected_out_of_range` a berou se jako unknown.
- Pokud stránka uvádí více průtoků, preferuje se varianta v kontextu EN 1253.


## Windows
- Spusť `setup_windows.bat` (vytvoří .venv a nainstaluje závislosti)
- Poté spusť `run_windows.bat`
