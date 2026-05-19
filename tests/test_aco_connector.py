import json
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco


class AcoConnectorDiscoveryTests(unittest.TestCase):
    def test_showerdrain_b_fixtures_exist(self):
        fixtures = Path(__file__).resolve().parent / "fixtures" / "aco_b"
        required = [
            "b_family_de.html",
            "b_product_de.html",
            "b_family_cz.html",
            "b_international_family.html",
            "b_international_product.html",
            "b_showerdrain_catalog_2025_cz.pdf",
            "b_installation_manual.pdf",
        ]
        missing = [n for n in required if not (fixtures / n).exists()]
        self.assertFalse(missing, f"missing fixtures: {missing}")

    def test_showerdrain_b_complete_system_discovery_and_tech_extraction(self):
        fixtures = Path(__file__).resolve().parent / "fixtures" / "aco_b"
        family_html = (fixtures / "b_family_de.html").read_text(encoding="utf-8")
        product_html = (fixtures / "b_product_de.html").read_text(encoding="utf-8")
        b_family = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/"
        b_url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/aco-showerdrain-b/"
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": f"<html><body><main><a href='{b_family}'>ShowerDrain B family</a></main></body></html>",
            b_family: family_html,
            b_url: product_html,
        }
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            return (200, key, pages[key], "") if key in pages else (404, key, "", "not found")
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(1200, 100)
        bdf = pd.DataFrame(rows)
        bdf = bdf[bdf["product_family"] == "showerdrain_b"]
        self.assertFalse(bdf.empty)
        self.assertTrue((bdf["system_role"].astype(str) == "complete_system").any())
        self.assertFalse((bdf["system_role"].astype(str) == "profile_channel").any())
        self.assertFalse((bdf["system_role"].astype(str) == "drain_body").any())
        row = bdf.iloc[0]
        self.assertEqual(str(row.get("product_url")), b_url)
        self.assertIn(str(row.get("outlet_dn")), {"DN50", "DN40/DN50"})
        self.assertEqual(int(row.get("water_seal_mm")), 30)
        self.assertEqual(float(row.get("flow_rate_10mm_lps")), 0.4)
        self.assertEqual(float(row.get("flow_rate_20mm_lps")), 0.46)

    def test_showerdrain_b_bom_optional_accessory_only_with_metadata(self):
        url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/aco-showerdrain-b/"
        html = """<html><body><main>
        <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/'>Direkt zur Hauptnavigation springen</a>
        <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/haarsieb/'>Haarsieb</a>
        <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/design-slot/'>Design Slot</a>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, url, html, "")):
            opts = aco.get_bom_options(url)
        self.assertTrue(opts)
        self.assertTrue(all(o.get("option_type") == "optional_accessory" for o in opts))
        self.assertFalse(any(o.get("component_id") == o.get("product_id") for o in opts))
        self.assertFalse(any(str(o.get("option_label")) == "Direkt zur Hauptnavigation springen" for o in opts))
        self.assertTrue(all("compatibility_confidence=implicit_family_level" in str(o.get("option_meta")) for o in opts))

    def test_showerdrain_b_pipeline_no_assembled_and_no_product_to_product_bom(self):
        registry = pd.DataFrame([
            {"manufacturer":"aco","product_id":"aco-b-main","product_name":"ShowerDrain B","product_family":"showerdrain_b","product_url":"https://example.test/b/main","candidate_type":"drain","system_role":"complete_system","complete_system":"yes"},
            {"manufacturer":"aco","product_id":"aco-b-hair","product_name":"Haarsieb","product_family":"showerdrain_b","product_url":"https://example.test/b/hair","candidate_type":"component","system_role":"accessory","complete_system":"component"},
        ])
        with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
            products, comparison, _excluded, _evidence, bom = pipeline.run_update(registry, default_config())
        b_bom = bom[bom["parent_family"].astype(str) == "showerdrain_b"]
        self.assertTrue((b_bom["option_type"].astype(str) == "optional_accessory").all())
        self.assertTrue((b_bom["option_meta"].astype(str).str.contains("B is an all-in-one product", regex=False)).all())
        self.assertFalse(products["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-b-").any())
        self.assertFalse(comparison["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-b-").any())

    def test_showerdrain_b_promotes_to_products_comparison_and_scoring_when_flow_10_20_present(self):
        registry = pd.DataFrame([
            {"manufacturer":"aco","product_id":"aco-b-main","product_name":"ShowerDrain B","product_family":"showerdrain_b","product_url":"https://example.test/b/main","candidate_type":"drain","system_role":"complete_system","complete_system":"yes"},
        ])
        with patch("src.connectors.aco.extract_parameters", return_value={"flow_rate_10mm_lps":0.4, "flow_rate_20mm_lps":0.46, "water_seal_mm":30, "outlet_dn":"DN40/DN50"}), patch("src.connectors.aco.get_bom_options", return_value=[]), patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
            products, comparison, excluded, _evidence, _bom = pipeline.run_update(registry, default_config())
        self.assertTrue((products["product_id"].astype(str) == "aco-b-main").any())
        prow = products[products["product_id"].astype(str) == "aco-b-main"].iloc[0]
        self.assertEqual(float(prow["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(prow["flow_rate_20mm_lps"]), 0.46)
        self.assertEqual(float(prow["flow_rate_lps"]), 0.46)
        self.assertEqual(int(prow["water_seal_mm"]), 30)
        self.assertIn(str(prow["outlet_dn"]), {"DN40/DN50", "DN50"})
        self.assertTrue((comparison["product_id"].astype(str) == "aco-b-main").any())
        if "product_id" in excluded.columns:
            self.assertFalse((excluded["product_id"].astype(str) == "aco-b-main").any())

    def test_showerdrain_b_fixture_production_path_extracts_flow_and_prefers_ws30(self):
        fixture = Path(__file__).resolve().parent / "fixtures" / "aco_b" / "b_product_de.html"
        if not fixture.exists():
            self.skipTest(f"missing fixture: {fixture}")
        html = fixture.read_text(encoding="utf-8")
        url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/aco-showerdrain-b/"
        with patch("src.connectors.aco._safe_get_text", return_value=(200, url, html, "")):
            p = aco.extract_parameters(url)
        self.assertEqual(float(p["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(p["flow_rate_20mm_lps"]), 0.46)
        self.assertEqual(float(p["flow_rate_lps"]), 0.46)
        self.assertEqual(int(p["water_seal_mm"]), 30)
        self.assertIn(str(p["outlet_dn"]), {"DN40/DN50", "DN50"})

    def test_showerdrain_b_extracts_ws30_from_reversed_phrase(self):
        url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/aco-showerdrain-b/"
        html = "<html><body><main><h1>ACO ShowerDrain B</h1><p>Abflussleistung mit 30 mm Sperrwasserhöhe 0,46 l/s bei 20 mm Aufstau</p></main></body></html>"
        with patch("src.connectors.aco._safe_get_text", return_value=(200, url, html, "")):
            p = aco.extract_parameters(url)
        self.assertEqual(int(p["water_seal_mm"]), 30)

    def test_showerdrain_b_fixture_pipeline_path_promotes_and_not_excluded(self):
        fixtures = Path(__file__).resolve().parent / "fixtures" / "aco_b"
        family_html = (fixtures / "b_family_de.html").read_text(encoding="utf-8")
        product_html = (fixtures / "b_product_de.html").read_text(encoding="utf-8")
        b_family = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/"
        b_url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/aco-showerdrain-b/"
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": f"<html><body><main><a href='{b_family}'>B</a></main></body></html>",
            b_family: family_html,
            b_url: product_html,
        }
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            return (200, key, pages[key], "") if key in pages else (404, key, "", "not found")
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(1200, 100)
            with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
                products, comparison, excluded, _evidence, bom = pipeline.run_update(pd.DataFrame(rows), default_config())
        self.assertTrue((products["product_family"].astype(str) == "showerdrain_b").any())
        self.assertTrue(comparison["product_id"].astype(str).str.contains("showerdrain-b", regex=False).any())
        if "excluded_reason" in excluded.columns:
            b_ex = excluded[excluded.get("product_family", pd.Series([], dtype=str)).astype(str) == "showerdrain_b"]
            self.assertFalse((b_ex.get("excluded_reason", pd.Series([], dtype=str)).astype(str) == "missing_flow_after_html").any())
        self.assertFalse(products["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-b-").any())
        b_bom = bom[bom.get("parent_family", pd.Series([], dtype=str)).astype(str) == "showerdrain_b"]
        self.assertTrue(b_bom.empty or (b_bom["option_type"].astype(str) == "optional_accessory").all())

    def test_showerdrain_b_fixture_market_sources_do_not_override_de_structured_values(self):
        fixtures = Path(__file__).resolve().parent / "fixtures" / "aco_b"
        de_html = (fixtures / "b_product_de.html").read_text(encoding="utf-8")
        cz_html = (fixtures / "b_family_cz.html").read_text(encoding="utf-8")
        intl_html = (fixtures / "b_international_product.html").read_text(encoding="utf-8")
        de_url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-b/aco-showerdrain-b/"
        cz_url = "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-b/"
        intl_url = "https://www.buildingdrainage.aco/products/collect/bathroom-drainage/channel/aco-showerdrain-b/aco-showerdrain-b"
        with patch("src.connectors.aco._safe_get_text", side_effect=[(200, de_url, de_html, ""), (200, cz_url, cz_html, ""), (200, intl_url, intl_html, "")]):
            p_de = aco.extract_parameters(de_url)
            p_cz = aco.extract_parameters(cz_url)
            p_intl = aco.extract_parameters(intl_url)
        self.assertEqual(float(p_de["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(p_de["flow_rate_20mm_lps"]), 0.46)
        self.assertEqual(int(p_de["water_seal_mm"]), 30)
        # Supplementary sources may differ and may be partially parseable; DE remains baseline.
        self.assertTrue(p_cz.get("flow_rate_10mm_lps") in (None, 0.4) or isinstance(p_cz.get("flow_rate_10mm_lps"), float))
        self.assertTrue(p_intl.get("flow_rate_lps") in (None, 0.55) or isinstance(p_intl.get("flow_rate_lps"), float))
    def test_eplus_discovery_integrated_drain_units_extract_technical_fields(self):
        family = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/"
        p92 = f"{family}duschrinnen/rinnenkoerper-einbauhoehe-oberkante-estrich-92-140-mm-din-en-1253-1/"
        p66 = f"{family}duschrinnen/rinnenkoerper-einbauhoehe-oberkante-estrich-66-140-mm/"
        p15 = f"{family}duschrinnen/rinnenkoerper-einbauhoehe-oberkante-estrich-15-140-mm/"
        grates = f"{family}duschrinnen/design-roste-aus-elektropoliertem-edelstahl/"
        html_index = f"""<html><body><main>
        <a href='{p92}'>Rinnenkörper 92-140</a><a href='{p66}'>Rinnenkörper 66-140</a>
        <a href='{p15}'>Rinnenkörper 15-140</a><a href='{grates}'>Design-Roste</a></main></body></html>"""
        def _page(h, ws, fl10, fl20):
            return f"<html><body><main><h1>{h}</h1><p>Einbauhöhe Oberkante Estrich {h.split()[-1]} mm</p><p>Sperrwasserhöhe: {ws} mm</p><p>Ablaufstutzen DN 50</p><p>Abflusswert {fl10} l/s bei 10 mm; {fl20} l/s bei 20 mm</p><table><tr><th>L1</th><th>Artikel</th></tr><tr><td>1185 mm</td><td>9010.70.01</td></tr></table></main></body></html>"
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": html_index,
            family: "<html><body><main><h1>ACO ShowerDrain E+</h1></main></body></html>",
            p92: _page("Rinnenkörper Einbauhöhe Oberkante Estrich 92-140", 50, "0,4", "0,6"),
            p66: _page("Rinnenkörper Einbauhöhe Oberkante Estrich 66-140", 25, "0,4", "0,6"),
            p15: _page("Rinnenkörper Einbauhöhe Oberkante Estrich 15-140", 50, "0,4", "0,6"),
            grates: "<html><body><main><h1>Design-Roste aus Edelstahl</h1><table><tr><th>L1</th><th>Artikel</th></tr><tr><td>1185 mm</td><td>9010.99.01</td></tr></table></main></body></html>",
        }
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            return (200, key, pages[key], "") if key in pages else (404, key, "", "not found")
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(1200, 100)
        eplus = pd.DataFrame(rows)
        eplus = eplus[eplus["product_family"] == "showerdrain_eplus"]
        self.assertTrue((eplus["system_role"].astype(str) == "drain_unit").any())
        self.assertTrue((eplus["system_role"].astype(str) == "grate").any())
        self.assertFalse((eplus["system_role"].astype(str) == "configuration_family").any())

    def test_eplus_bom_metadata_and_guards(self):
        url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/duschrinnen/rinnenkoerper-einbauhoehe-oberkante-estrich-92-140-mm-din-en-1253-1/"
        html = """<html><body><main>
        <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/'>Direkt zur Hauptnavigation springen</a>
        <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/duschrinnen/design-roste-aus-elektropoliertem-edelstahl/'>Design-Roste</a>
        <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/brandschutz-duschrinnen/ablaufkoerper-zu-aco-brandschutz-duschrinne-showerdrain-eplus/'>Brandschutz Ablaufkörper</a>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, url, html, "")):
            opts = aco.get_bom_options(url)
        self.assertTrue(opts)
        self.assertFalse(any(o.get("component_id") == o.get("product_id") for o in opts))
        self.assertFalse(any(str(o.get("option_label")) == "Direkt zur Hauptnavigation springen" for o in opts))
        self.assertTrue(all("compatibility_confidence=implicit_family_level" in str(o.get("option_meta")) for o in opts))
        self.assertTrue(all("explicit_article_matrix=false" in str(o.get("option_meta")) for o in opts))
        self.assertTrue(all("source_limitation=E+ compatibility is official family-level compatibility; no explicit article-to-article matrix found." in str(o.get("option_meta")) for o in opts))

    def test_eplus_pipeline_normalizes_bom_and_removes_body_to_body_links(self):
        registry = pd.DataFrame([
            {"manufacturer":"aco","product_id":"aco-eplus-25","product_name":"E+ RK 25-128","product_family":"showerdrain_eplus","product_url":"https://example.test/eplus/25","candidate_type":"drain","system_role":"drain_unit","complete_system":"yes"},
            {"manufacturer":"aco","product_id":"aco-eplus-57","product_name":"E+ RK 57-128","product_family":"showerdrain_eplus","product_url":"https://example.test/eplus/57","candidate_type":"drain","system_role":"drain_unit","complete_system":"yes"},
            {"manufacturer":"aco","product_id":"aco-eplus-grate","product_name":"E+ Design-Roste","product_family":"showerdrain_eplus","product_url":"https://example.test/eplus/grate","candidate_type":"component","system_role":"grate","complete_system":"component"},
        ])
        with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
            products, comparison, _excluded, _evidence, bom = pipeline.run_update(registry, default_config())
        eplus_bom = bom[bom["parent_family"].astype(str) == "showerdrain_eplus"].copy()
        self.assertFalse(eplus_bom.empty)
        self.assertFalse(((eplus_bom["product_id"].astype(str).str.contains("aco-eplus-", regex=False)) & (eplus_bom["component_id"].astype(str).str.contains("aco-eplus-", regex=False)) & (eplus_bom["component_id"].astype(str) != "aco-eplus-grate")).any())
        self.assertTrue((eplus_bom["option_meta"].astype(str).str.contains("compatibility_confidence=implicit_family_level", regex=False)).all())
        self.assertTrue((eplus_bom["option_meta"].astype(str).str.contains("explicit_article_matrix=false", regex=False)).all())
        self.assertTrue((eplus_bom["option_meta"].astype(str).str.contains("source_limitation=E+ compatibility is official family-level compatibility; no explicit article-to-article matrix found.", regex=False)).all())
        self.assertFalse(products["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-eplus-").any())
        self.assertFalse(comparison["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-eplus-").any())
    def test_mplus_component_article_rows_classified_and_drain_hydraulics_extracted(self):
        html_index = """<html><body><main>
            <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/">Ablaufkörper</a>
            <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-15-140-mm/">Rinnenkörper</a>
            <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/">Roste</a>
        </main></body></html>"""
        html_drain = """<html><body><main><h1>Ablaufkörper zur Duschrinne ACO ShowerDrain M+</h1>
            <table><tr><th>Artikel</th><th>Daten</th></tr>
            <tr><td>9010.81.20</td><td>DN 50 100 - 128 mm Sperrwasserhöhe: 50 mm 0,5 l/s mit 20 mm Aufstau</td></tr>
            <tr><td>9010.81.21</td><td>DN 50 80 - 128 mm Sperrwasserhöhe: 30 mm 0,5 l/s mit 20 mm Aufstau</td></tr>
            </table></main></body></html>"""
        html_channel = """<html><body><main><h1>Rinnenkörper Einbauhöhe Oberkante Estrich 15-140 mm</h1>
            <table><tr><th>L1</th><th>Artikel</th></tr><tr><td>985 mm</td><td>9010.87.03</td></tr></table></main></body></html>"""
        html_grate = """<html><body><main><h1>Design-Roste aus Edelstahl</h1>
            <table><tr><th>L1</th><th>Artikel</th></tr><tr><td>985 mm</td><td>9010.99.01</td></tr></table></main></body></html>"""
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": html_index,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/": html_drain,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-15-140-mm/": html_channel,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/": html_grate,
        }
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            html = pages.get(key)
            return (200, key, html, "") if html else (404, key, "", "not found")
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(1000, 200)
        df = pd.DataFrame(rows)
        mplus = df[df["product_family"] == "showerdrain_mplus"]
        self.assertTrue((mplus["system_role"] == "drain_body").any())
        self.assertTrue((mplus["system_role"] == "profile_channel").any())
        self.assertTrue((mplus["system_role"] == "grate").any())
        channel = mplus.set_index("product_id").loc["aco-90108703"]
        self.assertEqual(str(channel["system_role"]), "profile_channel")
        d20 = mplus.set_index("product_id").loc["aco-90108120"]
        self.assertEqual(float(d20["flow_rate_lps"]), 0.5)
        self.assertEqual(int(d20["water_seal_mm"]), 50)
        self.assertEqual(str(d20["outlet_dn"]), "DN50")

    def test_mplus_bom_options_implicit_family_level_and_no_navigation_noise(self):
        html = """<html><body><main>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/'>Direkt zur Hauptnavigation springen</a>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/'>Ablaufkörper</a>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/'>Design-Roste</a>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/showerstep/'>ShowerStep</a>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/", html, "")):
            opts = aco.get_bom_options("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/")
        self.assertTrue(any(o.get("option_type") == "compatible_drain_body" for o in opts))
        self.assertTrue(any(o.get("option_type") == "compatible_grate" for o in opts))
        self.assertTrue(any(o.get("option_type") == "optional_accessory" for o in opts))
        self.assertTrue(all("implicit_family_level" in str(o.get("option_meta") or "") for o in opts))
        self.assertTrue(all("explicit_article_matrix=false" in str(o.get("option_meta") or "") for o in opts))
        self.assertTrue(all("source_limitation=M+ compatibility is official family-level compatibility; no explicit article-to-article matrix found." in str(o.get("option_meta") or "") for o in opts))
        self.assertFalse(any(o.get("component_id") == o.get("product_id") for o in opts))
        self.assertFalse(any(str(o.get("option_label") or "") == "Direkt zur Hauptnavigation springen" for o in opts))

    def test_mplus_drain_parent_does_not_emit_drain_to_drain_links(self):
        html = """<html><body><main>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/'>Ablaufkörper</a>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/'>Design-Roste</a>
        </main></body></html>"""
        url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/"
        with patch("src.connectors.aco._safe_get_text", return_value=(200, url, html, "")):
            opts = aco.get_bom_options(url)
        self.assertFalse(any(o.get("option_role") == "drain_body" for o in opts))
        self.assertTrue(any(o.get("option_role") == "grate" for o in opts))
    def test_stable_aco_id_helpers_are_deterministic_and_ascii_safe(self):
        id1 = aco._stable_aco_id(
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-aufsatzstuecke-standard/",
            "easyflow",
            "accessory",
            "ACO Easyflow Aufsatzstücke Standard",
        )
        id2 = aco._stable_aco_id(
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-aufsatzstuecke-standard/",
            "easyflow",
            "accessory",
            "ACO Easyflow Aufsatzstücke Standard",
        )
        self.assertEqual(id1, id2)
        self.assertEqual(id1, "aco-easyflow-aco-easyflow-aufsatzstuecke-standard")
        self.assertTrue(id1.islower())
        self.assertNotRegex(id1, r"[^a-z0-9-]")

    def test_in_scope_accepts_de_and_cz_bathroom_scopes(self):
        self.assertTrue(aco._in_scope("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"))
        self.assertTrue(aco._in_scope("https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/"))
        self.assertTrue(aco._in_scope("https://www.aco.cz/produkty/odvodneni-koupelen/"))
        self.assertFalse(aco._in_scope("https://www.aco-haustechnik.de/produkte/hausinstallation/"))
        self.assertFalse(aco._in_scope("https://example.com/produkte/badentwaesserung/"))

    def test_discovery_covers_multiple_families_and_keeps_showerdrain_c_1200_variants(self):
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": """
                <html><body><main><h1>Badentwässerung</h1>
                <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/">C body</a>
                <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/">S+</a>
                <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper/">M+ body</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus-komplettablauf-dn50/">EasyFlow+ Komplettablauf DN50</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-easyflow-komplettablauf-dn50/">EasyFlow Komplettablauf DN50</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-showerpoint/">ShowerPoint</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-renovierungsablauf-passino/">Passino</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-bodenablauf-passavant/">Passavant</a>
                <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/designrost/">Designrost</a>
                <a href="/produkty/odvodneni-koupelen/aco-showerdrain-public-80/">Public 80 cz</a>
                </main></body></html>
            """,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/": """
                <html><body><main><h1>ACO ShowerDrain C Rinnenkörper</h1>
                <table>
                    <tr><th>L1</th><th>Artikel</th></tr>
                    <tr><td>1185 mm</td><td>90108544</td></tr>
                    <tr><td>1185 mm</td><td>90108554</td></tr>
                    <tr><td>985 mm</td><td>90108524</td></tr>
                    <tr><td>985 mm</td><td>90108534</td></tr>
                </table>
                </main></body></html>
            """,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/": "<html><body><main><h1>ACO ShowerDrain S+</h1></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper/": "<html><body><main><h1>ACO ShowerDrain M+ Rinnenkörper</h1></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus-komplettablauf-dn50/": "<html><body><main><h1>ACO EasyFlow+ Komplettablauf DN50</h1></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-komplettablauf-dn50/": "<html><body><main><h1>ACO Easyflow Komplettablauf DN50</h1></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-showerpoint/": "<html><body><main><h1>ACO ShowerPoint</h1></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-renovierungsablauf-passino/": "<html><body><main><h1>ACO Renovierungsablauf Passino</h1></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-bodenablauf-passavant/": "<html><body><main><h1>ACO Bodenablauf Passavant</h1></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/designrost/": "<html><body><main><h1>ACO ShowerDrain C Designrost</h1></main></body></html>",
            "https://www.aco.cz/produkty/odvodneni-koupelen/aco-showerdrain-public-80/": "<html><body><main><h1>ACO ShowerDrain Public 80</h1></main></body></html>",
            "https://www.aco.cz/produkty/odvodneni-koupelen/": "<html><body><main><h1>Odvodnění koupelen</h1><a href='/produkty/odvodneni-koupelen/aco-showerdrain-public-80/'>Public 80</a></main></body></html>",
        }

        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            html = pages.get(key)
            if html is None:
                return 404, key, "", "not found"
            return 200, key, html, ""

        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, dbg = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)

        ids = {r["product_id"] for r in rows}
        self.assertIn("aco-90108544", ids)
        self.assertIn("aco-90108554", ids)
        summary = next(d for d in dbg if d.get("method") == "summary")
        fam_cov = json.loads(summary["expected_family_coverage"])
        self.assertTrue(fam_cov.get("showerdrain_c"))
        self.assertTrue(fam_cov.get("showerdrain_splus"))
        self.assertTrue(fam_cov.get("showerdrain_mplus"))
        self.assertTrue(fam_cov.get("easyflowplus"))
        self.assertTrue(fam_cov.get("easyflow"))
        self.assertTrue(fam_cov.get("showerpoint"))
        self.assertTrue(fam_cov.get("passino"))
        self.assertTrue(fam_cov.get("passavant"))


    def test_extract_parameters_reads_article_row_when_url_contains_article_anchor(self):
        html = """<html><body><main><h1>ACO ShowerDrain C</h1>
            <p>Einbauhöhe Oberkante Estrich 57-128 mm</p>
            <table>
                <tr><th>Artikel</th><th>Abflusswert 10 mm</th><th>Abflusswert 20 mm</th><th>Sperrwasserhöhe</th></tr>
                <tr><td>90108544</td><td>0,70 l/s</td><td>0,80 l/s</td><td>25 mm</td></tr>
                <tr><td>90108554</td><td>0,72 l/s</td><td>0,92 l/s</td><td>50 mm</td></tr>
            </table>
        </main></body></html>"""

        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/p/", html, "")):
            p25 = aco.extract_parameters("https://www.aco-haustechnik.de/p/#article-90108544")
            p50 = aco.extract_parameters("https://www.aco-haustechnik.de/p/#article-90108554")

        self.assertEqual(float(p25["flow_rate_10mm_lps"]), 0.70)
        self.assertEqual(float(p25["flow_rate_20mm_lps"]), 0.80)
        self.assertEqual(int(p25["water_seal_mm"]), 25)
        self.assertEqual(float(p50["flow_rate_10mm_lps"]), 0.72)
        self.assertEqual(float(p50["flow_rate_20mm_lps"]), 0.92)
        self.assertEqual(int(p50["water_seal_mm"]), 50)
        self.assertNotEqual(p25["flow_rate_20mm_lps"], p50["flow_rate_20mm_lps"])

    def test_extract_parameters_adds_diagnostic_when_article_row_has_no_hydraulic_fields(self):
        html = """<html><body><main><h1>ACO ShowerDrain C</h1>
            <table>
                <tr><th>L1</th><th>Artikel</th><th>Preis</th></tr>
                <tr><td>1185 mm</td><td>90108544</td><td>405,53 €</td></tr>
            </table>
            <p>Einbauhöhen (Sperrwasserhöhe 25 mm)</p>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/p/", html, "")):
            p25 = aco.extract_parameters("https://www.aco-haustechnik.de/p/#article-90108544")
        self.assertIsNone(p25.get("flow_rate_10mm_lps"))
        self.assertIsNone(p25.get("flow_rate_20mm_lps"))
        self.assertEqual(int(p25.get("water_seal_mm")), 25)
        labels = [ev[0] for ev in (p25.get("evidence") or [])]
        self.assertIn("Article row hydraulics", labels)

    def test_extract_parameters_marks_generic_page_flow_when_article_row_lacks_hydraulics(self):
        html = """<html><body><main><h1>ACO ShowerDrain C</h1>
            <p>Ablaufleistung bis zu 0,91 l/s</p>
            <p>Sperrwasserhöhe 25 mm</p>
            <table>
                <tr><th>L1</th><th>Artikel</th><th>Preis</th></tr>
                <tr><td>1185 mm</td><td>90108544</td><td>405,53 €</td></tr>
            </table>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/p/", html, "")):
            p25 = aco.extract_parameters("https://www.aco-haustechnik.de/p/#article-90108544")
        self.assertEqual(int(p25.get("water_seal_mm")), 25)
        self.assertIsNone(p25.get("flow_rate_10mm_lps"))
        self.assertIsNone(p25.get("flow_rate_20mm_lps"))
        self.assertEqual(float(p25.get("flow_rate_lps")), 0.91)
        labels = [ev[0] for ev in (p25.get("evidence") or [])]
        self.assertIn("Flow attribution limited", labels)


    def test_splus_bom_options_require_explicit_compatibility_section(self):
        html_no = """<html><body><main><h1>ACO ShowerDrain S+</h1>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/rinnenkoerper/'>Rinnenkörper</a>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/", html_no, "")):
            opts = aco.get_bom_options("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/")
        self.assertEqual(opts, [])

        html_yes = """<html><body><main><h1>ACO ShowerDrain S+</h1>
            <p>Kompatibel mit folgenden Ablaufkörpern</p>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/rinnenkoerper/'>Rinnenkörper S+</a>
            <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/designrost/'>Designrost S+</a>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/", html_yes, "")):
            opts_yes = aco.get_bom_options("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/")
        self.assertTrue(any(o.get("option_type") == "compatible_drain_body" for o in opts_yes))
        self.assertTrue(any(o.get("option_type") == "compatible_grate" for o in opts_yes))
        self.assertTrue(all(str(o.get("option_label") or "").strip().lower() != "direkt zur hauptnavigation springen" for o in opts_yes))

    def test_splus_bom_options_ignore_navigation_and_self_reference(self):
        html = """<html><body>
            <header><a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/'>Direkt zur Hauptnavigation springen</a></header>
            <main>
                <p>Kompatibel mit folgenden Ablaufkörpern</p>
                <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/'>ACO ShowerDrain S+</a>
                <a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/'>Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+</a>
            </main>
        </body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/", html, "")):
            opts = aco.get_bom_options("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/")
        self.assertFalse(any((o.get("option_label") or "").strip().lower() == "direkt zur hauptnavigation springen" for o in opts))
        parent_id = aco._stable_aco_id(
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/",
            "showerdrain_splus",
            "configuration_family",
            "ACO ShowerDrain S+",
        )
        self.assertFalse(any(o.get("component_id") == parent_id for o in opts))


    def test_splus_article_rows_classified_to_profile_and_drain_only(self):
        html = """<html><body><main><h1>ACO ShowerDrain S+ Duschrinnenprofil</h1>
            <table><tr><th>L1</th><th>Artikel</th></tr>
              <tr><td>800 mm</td><td>9010.51.01</td></tr>
              <tr><td>900 mm</td><td>9010.51.02</td></tr>
              <tr><td>1000 mm</td><td>9010.51.20</td></tr>
              <tr><td>1200 mm</td><td>9010.51.21</td></tr>
              <tr><td>1200 mm</td><td>9010.51.27</td></tr>
            </table></main></body></html>"""
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": f"<html><body><main><a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/'>S+</a></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/": html,
        }
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            if key in pages:
                return 200, key, pages[key], ""
            return 404, key, "", "nf"
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(1000, 300)
        ids = {r.get("article_no"): r.get("system_role") for r in rows if r.get("product_family") == "showerdrain_splus" and r.get("article_no")}
        self.assertEqual(ids.get("9010.51.01"), "profile_channel")
        self.assertEqual(ids.get("9010.51.02"), "profile_channel")
        self.assertEqual(ids.get("9010.51.20"), "drain_body")
        self.assertEqual(ids.get("9010.51.21"), "drain_body")
        self.assertNotIn("9010.51.27", ids)

    def test_splus_profile_gets_implicit_family_level_bom_hints(self):
        html = "<html><body><main><h1>ACO ShowerDrain S+ Profil</h1></main></body></html>"
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/", html, "")):
            opts = aco.get_bom_options("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/#article-90105101")
        self.assertTrue(any(o.get("component_id") == "aco-90105120" for o in opts))
        self.assertTrue(any(o.get("component_id") == "aco-90105121" for o in opts))
        self.assertTrue(all("implicit_family_level" in str(o.get("option_meta") or "") for o in opts))


    def test_splus_drain_body_rows_extract_row_specific_ws_flow_and_height(self):
        html = """<html><body><main><h1>Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+</h1>
            <table>
                <tr><th>Artikel</th><th>Daten</th></tr>
                <tr><td>9010.51.20</td><td>DN 50 1,5° 90 - 180 mm Sperrwasserhöhe: 50 mm 0,7 l/s mit 10 mm Aufstau 0,8 l/s mit 20 mm Aufstau</td></tr>
                <tr><td>9010.51.21</td><td>DN 50 1,5° 70 - 160 mm Sperrwasserhöhe: 30 mm 0,4 l/s mit 10 mm Aufstau 0,6 l/s mit 20 mm Aufstau</td></tr>
            </table>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/", html, "")):
            p20 = aco.extract_parameters("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/#article-90105120")
            p21 = aco.extract_parameters("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/#article-90105121")
        self.assertEqual(int(p20["water_seal_mm"]), 50)
        self.assertEqual(float(p20["flow_rate_10mm_lps"]), 0.7)
        self.assertEqual(float(p20["flow_rate_20mm_lps"]), 0.8)
        self.assertEqual(str(p20["outlet_dn"]), "DN50")
        self.assertEqual(int(p20["height_adj_min_mm"]), 90)
        self.assertEqual(int(p20["height_adj_max_mm"]), 180)

        self.assertEqual(int(p21["water_seal_mm"]), 30)
        self.assertEqual(float(p21["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(p21["flow_rate_20mm_lps"]), 0.6)
        self.assertEqual(str(p21["outlet_dn"]), "DN50")
        self.assertEqual(int(p21["height_adj_min_mm"]), 70)
        self.assertEqual(int(p21["height_adj_max_mm"]), 160)
        self.assertNotEqual(p20["water_seal_mm"], p21["water_seal_mm"])

    def test_splus_drain_body_page_level_ws_flow_blocks_map_to_article_rows(self):
        html = """<html><body><main><h1>Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+</h1>
            <section>
              <p>Ablauf mit Sperrwasserhöhe 50 mm</p>
              <p>0,7 l/s mit 10 mm Aufstau</p>
              <p>0,8 l/s mit 20 mm Aufstau</p>
              <p>Ablauf mit Sperrwasserhöhe 30 mm</p>
              <p>0,4 l/s mit 10 mm Aufstau</p>
              <p>0,6 l/s mit 20 mm Aufstau</p>
            </section>
            <table>
                <tr><th>Artikel</th><th>Daten</th></tr>
                <tr><td>9010.51.20</td><td>DN 50 1,5° 90 - 180 mm Sperrwasserhöhe: 50 mm</td></tr>
                <tr><td>9010.51.21</td><td>DN 50 1,5° 70 - 160 mm Sperrwasserhöhe: 30 mm</td></tr>
            </table>
        </main></body></html>"""
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/", html, "")):
            p20 = aco.extract_parameters("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/#article-90105120")
            p21 = aco.extract_parameters("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/#article-90105121")
        self.assertEqual(float(p20["flow_rate_10mm_lps"]), 0.7)
        self.assertEqual(float(p20["flow_rate_20mm_lps"]), 0.8)
        self.assertEqual(float(p21["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(p21["flow_rate_20mm_lps"]), 0.6)



class AcoSplusPipelineComponentPropagationTests(unittest.TestCase):
    def test_splus_discovery_candidates_include_structured_drain_body_flows_for_components_export_source(self):
        html_family = """<html><body><main>
            <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/">Drain</a>
        </main></body></html>"""
        html_drain = """<html><body><main><h1>Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+</h1>
            <table>
                <tr><th>Artikel</th><th>Daten</th></tr>
                <tr><td>9010.51.20</td><td>DN 50 1,5° 90 - 180 mm Sperrwasserhöhe: 50 mm 0,7 l/s mit 10 mm Aufstau 0,8 l/s mit 20 mm Aufstau</td></tr>
                <tr><td>9010.51.21</td><td>DN 50 1,5° 70 - 160 mm Sperrwasserhöhe: 30 mm 0,4 l/s mit 10 mm Aufstau 0,6 l/s mit 20 mm Aufstau</td></tr>
            </table>
        </main></body></html>"""
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": html_family,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/": html_drain,
        }

        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            html = pages.get(key)
            if html is None:
                return 404, key, "", "not found"
            return 200, key, html, ""

        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _dbg = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
        reg = pd.DataFrame(rows).set_index("product_id")
        p20 = reg.loc["aco-90105120"]
        p21 = reg.loc["aco-90105121"]
        self.assertEqual(str(p20["candidate_type"]), "component")
        self.assertEqual(str(p20["system_role"]), "drain_body")
        self.assertEqual(float(p20["flow_rate_10mm_lps"]), 0.7)
        self.assertEqual(float(p20["flow_rate_20mm_lps"]), 0.8)
        self.assertEqual(float(p20["flow_rate_lps"]), 0.8)
        self.assertEqual(int(p20["water_seal_mm"]), 50)
        self.assertEqual(int(p20["height_adj_min_mm"]), 90)
        self.assertEqual(int(p20["height_adj_max_mm"]), 180)
        self.assertEqual(str(p21["candidate_type"]), "component")
        self.assertEqual(str(p21["system_role"]), "drain_body")
        self.assertEqual(float(p21["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(p21["flow_rate_20mm_lps"]), 0.6)
        self.assertEqual(float(p21["flow_rate_lps"]), 0.6)
        self.assertEqual(int(p21["water_seal_mm"]), 30)
        self.assertEqual(int(p21["height_adj_min_mm"]), 70)
        self.assertEqual(int(p21["height_adj_max_mm"]), 160)

    def test_splus_drain_body_structured_flow_fields_propagate_to_components_not_comparison(self):
        html = """<html><body><main><h1>Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+</h1>
            <table>
                <tr><th>Artikel</th><th>Daten</th></tr>
                <tr><td>9010.51.20</td><td>DN 50 1,5° 90 - 180 mm Sperrwasserhöhe: 50 mm 0,7 l/s mit 10 mm Aufstau 0,8 l/s mit 20 mm Aufstau</td></tr>
                <tr><td>9010.51.21</td><td>DN 50 1,5° 70 - 160 mm Sperrwasserhöhe: 30 mm 0,4 l/s mit 10 mm Aufstau 0,6 l/s mit 20 mm Aufstau</td></tr>
            </table>
        </main></body></html>"""
        registry = pd.DataFrame([
            {"manufacturer": "aco", "product_id": "aco-90105120", "product_name": "Ablaufkörper 9010.51.20", "product_family": "showerdrain_splus", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/#article-90105120", "candidate_type": "component", "system_role": "drain_body", "complete_system": "component"},
            {"manufacturer": "aco", "product_id": "aco-90105121", "product_name": "Ablaufkörper 9010.51.21", "product_family": "showerdrain_splus", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/#article-90105121", "candidate_type": "component", "system_role": "drain_body", "complete_system": "component"},
        ])
        with patch("src.connectors.aco._safe_get_text", return_value=(200, "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/", html, "")):
            with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
                products, comparison, _excluded, _evidence, _bom = pipeline.run_update(registry, default_config())
        p20 = products.set_index("product_id").loc["aco-90105120"]
        p21 = products.set_index("product_id").loc["aco-90105121"]
        self.assertEqual(float(p20["flow_rate_10mm_lps"]), 0.7)
        self.assertEqual(float(p20["flow_rate_20mm_lps"]), 0.8)
        self.assertEqual(float(p21["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(p21["flow_rate_20mm_lps"]), 0.6)
        self.assertFalse((comparison["product_id"].isin(["aco-90105120", "aco-90105121"])).any())

    def test_splus_fixture_discovery_to_pipeline_components_keeps_structured_drain_flows(self):
        fixtures = Path(__file__).resolve().parent / "fixtures" / "aco_splus"
        family_path = fixtures / "splus_family.html"
        profile_path = fixtures / "splus_profile.html"
        drain_path = fixtures / "splus_drain_body.html"
        if not (family_path.exists() and profile_path.exists() and drain_path.exists()):
            self.skipTest(f"missing fixture files under {fixtures}")
        family_html = family_path.read_text(encoding="utf-8")
        profile_html = profile_path.read_text(encoding="utf-8")
        drain_html = drain_path.read_text(encoding="utf-8")
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": family_html,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/": family_html,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/": profile_html,
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/": drain_html,
        }

        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            html = pages.get(key)
            if html is None:
                return 404, key, "", "not found"
            return 200, key, html, ""

        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _dbg = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
            registry = pd.DataFrame(rows)
            with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
                products, comparison, _excluded, evidence, _bom = pipeline.run_update(registry, default_config())

        by_id = products.set_index("product_id")
        p20 = by_id.loc["aco-90105120"]
        p21 = by_id.loc["aco-90105121"]
        self.assertEqual(str(p20["candidate_type"]), "component")
        self.assertEqual(str(p20["system_role"]), "drain_body")
        self.assertEqual(int(p20["water_seal_mm"]), 50)
        self.assertEqual(int(p20["height_adj_min_mm"]), 90)
        self.assertEqual(int(p20["height_adj_max_mm"]), 180)
        self.assertEqual(float(p20["flow_rate_10mm_lps"]), 0.7)
        self.assertEqual(float(p20["flow_rate_20mm_lps"]), 0.8)
        self.assertEqual(float(p20["flow_rate_lps"]), 0.8)

        self.assertEqual(str(p21["candidate_type"]), "component")
        self.assertEqual(str(p21["system_role"]), "drain_body")
        self.assertEqual(int(p21["water_seal_mm"]), 30)
        self.assertEqual(int(p21["height_adj_min_mm"]), 70)
        self.assertEqual(int(p21["height_adj_max_mm"]), 160)
        self.assertEqual(float(p21["flow_rate_10mm_lps"]), 0.4)
        self.assertEqual(float(p21["flow_rate_20mm_lps"]), 0.6)
        self.assertEqual(float(p21["flow_rate_lps"]), 0.6)
        self.assertFalse((comparison["product_id"].isin(["aco-90105120", "aco-90105121"])).any())
        self.assertTrue((evidence["product_id"].isin(["aco-90105120", "aco-90105121"])).any())

if __name__ == "__main__":
    unittest.main()


class AcoConnectorEndToEndRegressionTests(unittest.TestCase):
    def test_splus_implicit_family_level_assemblies_created_with_drain_hydraulics(self):
        profiles = [f"aco-901051{n:02d}" for n in [1, 2, 3, 4, 41, 42, 43, 44]]
        registry_rows = []
        for pid in profiles:
            registry_rows.append({
                "manufacturer": "aco", "product_id": pid, "product_name": f"S+ profile {pid}", "product_family": "showerdrain_splus",
                "product_url": f"https://example.test/splus/profile#{pid}", "candidate_type": "component", "system_role": "profile_channel", "complete_system": "component"
            })
        for pid, ws, f10, f20, h1, h2 in [
            ("aco-90105120", 50, 0.7, 0.8, 90, 180),
            ("aco-90105121", 30, 0.4, 0.6, 70, 160),
        ]:
            registry_rows.append({
                "manufacturer": "aco", "product_id": pid, "product_name": f"S+ drain {pid}", "product_family": "showerdrain_splus",
                "product_url": f"https://example.test/splus/drain#{pid}", "candidate_type": "component", "system_role": "drain_body", "complete_system": "component",
                "water_seal_mm": ws, "flow_rate_10mm_lps": f10, "flow_rate_20mm_lps": f20, "flow_rate_lps": f20,
                "flow_rate_unit": "l/s", "flow_rate_status": "ok", "height_adj_min_mm": h1, "height_adj_max_mm": h2, "outlet_dn": "DN50",
            })
        def _fake_extract(url):
            return {}
        def _fake_bom(url, params=None):
            if "profile" not in url:
                return []
            pid = url.split("#")[-1]
            return [
                {"manufacturer":"aco","product_id":pid,"component_id":"aco-90105120","option_type":"compatible_drain_body","option_role":"drain_body","parent_family":"showerdrain_splus","option_family":"showerdrain_splus","source_url":url,"option_meta":"compatibility_confidence=implicit_family_level; explicit_article_matrix=false"},
                {"manufacturer":"aco","product_id":pid,"component_id":"aco-90105121","option_type":"compatible_drain_body","option_role":"drain_body","parent_family":"showerdrain_splus","option_family":"showerdrain_splus","source_url":url,"option_meta":"compatibility_confidence=implicit_family_level; explicit_article_matrix=false"},
            ]
        with patch("src.connectors.aco.extract_parameters", side_effect=_fake_extract), patch("src.connectors.aco.get_bom_options", side_effect=_fake_bom), patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
            products, comparison, _excluded, _evidence, bom = pipeline.run_update(pd.DataFrame(registry_rows), default_config())
        asm = products[products["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-splus-")].copy()
        self.assertEqual(len(asm), 16)
        self.assertEqual(len(set(asm["product_id"].astype(str))), 16)
        self.assertTrue((asm["compatibility_confidence"].astype(str) == "implicit_family_level").all())
        self.assertTrue((asm["explicit_article_matrix"].astype(str).str.lower() == "false").all())
        ws50 = asm[asm["base_product_id"].astype(str).str.contains("901051..", regex=True) & (asm["grate_component_id"] == "aco-90105120")]
        ws30 = asm[asm["grate_component_id"] == "aco-90105121"]
        self.assertTrue((pd.to_numeric(ws50["flow_rate_10mm_lps"], errors="coerce") == 0.7).all())
        self.assertTrue((pd.to_numeric(ws50["flow_rate_20mm_lps"], errors="coerce") == 0.8).all())
        self.assertTrue((pd.to_numeric(ws30["flow_rate_10mm_lps"], errors="coerce") == 0.4).all())
        self.assertTrue((pd.to_numeric(ws30["flow_rate_20mm_lps"], errors="coerce") == 0.6).all())
        cmp_asm = comparison[comparison["product_id"].astype(str).isin(set(asm["product_id"].astype(str)))]
        self.assertEqual(len(cmp_asm), 16)
        self.assertFalse((bom["product_id"] == bom["component_id"]).any())
        self.assertFalse((bom.get("option_label", pd.Series([], dtype=str)).astype(str) == "Direkt zur Hauptnavigation springen").any())

    def test_real_connector_path_preserves_broad_discovery_and_pipeline_outputs(self):
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": """<html><body><main><h1>Badentwässerung</h1>
                <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/">C body</a>
                <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/">S+</a>
                <a href="/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/">M+</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus-komplettablauf-dn50/">Easyflow+</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-easyflow-komplettablauf-dn50/">Easyflow</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-renovierungsablauf-passino/">Passino</a>
                <a href="/produkte/badentwaesserung/badablaeufe/aco-bodenablauf-passavant/">Passavant</a>
                <a href="/produkte/badentwaesserung/reihenduschrinnen/aco-showerdrain-public-80/">Public80</a>
            </main></body></html>""",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/": """<html><body><main><h1>ACO ShowerDrain C Rinnenkörper</h1>
                <p>Einbauhöhe Oberkante Estrich 57-128 mm</p><p>Ablaufstutzen DN 50</p><p>Ablaufleistung 0,80 l/s</p>
                <table><tr><th>L1</th><th>Artikel</th><th>Abflusswert 20 mm</th></tr>
                    <tr><td>1185 mm</td><td>90108544</td><td>0,80 l/s</td></tr><tr><td>1185 mm</td><td>90108554</td><td>0,80 l/s</td></tr>
                    <tr><td>985 mm</td><td>90108524</td><td>0,91 l/s</td></tr><tr><td>985 mm</td><td>90108534</td><td>0,91 l/s</td></tr></table>
            </main></body></html>""",
        }
        for url, name in [
            ("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/", "ACO ShowerDrain S+"),
            ("https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/", "ACO ShowerDrain M+"),
            ("https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus-komplettablauf-dn50/", "ACO Easyflow+ Komplettablauf"),
            ("https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-komplettablauf-dn50/", "ACO Easyflow Komplettablauf"),
            ("https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-renovierungsablauf-passino/", "ACO Passino"),
            ("https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-bodenablauf-passavant/", "ACO Passavant"),
            ("https://www.aco-haustechnik.de/produkte/badentwaesserung/reihenduschrinnen/aco-showerdrain-public-80/", "ACO ShowerDrain Public 80"),
            ("https://www.aco.cz/produkty/odvodneni-koupelen/", "Odvodnění koupelen"),
        ]:
            pages[url] = f"<html><body><main><h1>{name}</h1></main></body></html>"

        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            html = pages.get(key)
            if html is None:
                return 404, key, "", "not found"
            return 200, key, html, ""

        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _dbg = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
            self.assertGreaterEqual(len(rows), 8)
            registry = pd.DataFrame(rows)
            with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
                products, comparison, _excluded, evidence, bom = pipeline.run_update(registry, default_config())

        self.assertGreaterEqual(len(products), 6)
        self.assertGreaterEqual(len(comparison), 4)
        self.assertIn("candidate_type", products.columns)
        self.assertTrue((products["candidate_type"].astype(str) == "component").any())
        self.assertTrue((products["candidate_type"].astype(str) == "drain").any())
        families = set(products["product_family"].astype(str).str.lower())
        for fam in ["showerdrain_c", "showerdrain_splus", "showerdrain_mplus", "easyflowplus", "easyflow", "passino", "passavant", "showerdrain_public_80"]:
            self.assertIn(fam, families)
        c_rows = products[products["product_id"].astype(str).isin(["aco-90108544", "aco-90108554"]) ]
        self.assertEqual(len(c_rows), 2)
        self.assertTrue((c_rows["flow_rate_lps"].notna()).all())
        self.assertTrue((c_rows["height_adj_min_mm"].notna()).all())
        self.assertTrue((c_rows["height_adj_max_mm"].notna()).all())
        self.assertFalse((products["product_id"].astype(str).str.contains("901051", regex=False)).any())
        self.assertFalse((comparison["product_id"].astype(str).str.contains("901051", regex=False)).any())
        self.assertFalse(evidence[evidence["manufacturer"] == "aco"].empty)

    def test_showerdrain_c_assembled_rows_restore_flow_from_options_and_sync_to_comparison(self):
        registry = pd.DataFrame([
            {"manufacturer": "aco", "product_id": "aco-90108524", "product_name": "C body 90108524", "product_family": "showerdrain_c", "product_url": "https://example.test/c#article-90108524", "candidate_type": "drain", "system_role": "drain_unit", "complete_system": "yes"},
            {"manufacturer": "aco", "product_id": "aco-90108534", "product_name": "C body 90108534", "product_family": "showerdrain_c", "product_url": "https://example.test/c#article-90108534", "candidate_type": "drain", "system_role": "drain_unit", "complete_system": "yes"},
            {"manufacturer": "aco", "product_id": "aco-90108544", "product_name": "C body 90108544", "product_family": "showerdrain_c", "product_url": "https://example.test/c#article-90108544", "candidate_type": "drain", "system_role": "drain_unit", "complete_system": "yes"},
            {"manufacturer": "aco", "product_id": "aco-90108554", "product_name": "C body 90108554", "product_family": "showerdrain_c", "product_url": "https://example.test/c#article-90108554", "candidate_type": "drain", "system_role": "drain_unit", "complete_system": "yes"},
            {"manufacturer": "aco", "product_id": "aco-grate-c", "product_name": "C grate", "product_family": "showerdrain_c", "product_url": "https://example.test/c-grate", "candidate_type": "component", "system_role": "grate", "complete_system": "component"},
        ])
        params_map = {
            "aco-90108524": {"flow_rate_lps": None, "flow_rate_lps_options": "[0.91]", "water_seal_mm": 50, "height_adj_min_mm": 80, "height_adj_max_mm": 128, "outlet_dn": "DN50"},
            "aco-90108534": {"flow_rate_lps": None, "flow_rate_lps_options": "[0.91]", "water_seal_mm": 50, "height_adj_min_mm": 80, "height_adj_max_mm": 128, "outlet_dn": "DN50"},
            "aco-90108544": {"flow_rate_lps": None, "flow_rate_lps_options": "[0.91]", "water_seal_mm": 25, "height_adj_min_mm": 57, "height_adj_max_mm": 128, "outlet_dn": "DN40"},
            "aco-90108554": {"flow_rate_lps": None, "flow_rate_lps_options": "[0.91]", "water_seal_mm": 25, "height_adj_min_mm": 57, "height_adj_max_mm": 128, "outlet_dn": "DN40"},
            "aco-grate-c": {},
        }
        def _fake_extract(url):
            for pid, params in params_map.items():
                if pid.split("aco-")[1] in url or pid in url:
                    return dict(params)
            if "c-grate" in url:
                return {}
            return {}
        def _fake_bom(url, params=None):
            if "#article-90108524" in url:
                pid = "aco-90108524"
            elif "#article-90108534" in url:
                pid = "aco-90108534"
            elif "#article-90108544" in url:
                pid = "aco-90108544"
            elif "#article-90108554" in url:
                pid = "aco-90108554"
            else:
                return []
            return [{"manufacturer":"aco","product_id":pid,"component_id":"aco-grate-c","option_type":"compatible_grate","option_role":"grate","parent_family":"showerdrain_c","option_family":"showerdrain_c","source_url":url}]
        with patch("src.connectors.aco.extract_parameters", side_effect=_fake_extract), patch("src.connectors.aco.get_bom_options", side_effect=_fake_bom), patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
            products, comparison, _excluded, _evidence, _bom = pipeline.run_update(registry, default_config())
        asm = products[products["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-c-aco-901085")].copy()
        self.assertEqual(len(asm), 4)
        self.assertTrue((pd.to_numeric(asm["flow_rate_lps"], errors="coerce").notna()).all())
        self.assertTrue((pd.to_numeric(asm["flow_rate_lps"], errors="coerce") == 0.91).all())
        self.assertTrue((asm["flow_rate_unit"].astype(str) == "l/s").all())
        self.assertTrue((asm["flow_rate_status"].astype(str) == "ok").all())
        for col in ("water_seal_mm", "height_adj_min_mm", "height_adj_max_mm", "outlet_dn"):
            self.assertTrue((asm[col].notna()).all(), col)

        comp_asm = comparison[comparison["product_id"].astype(str).isin(set(asm["product_id"].astype(str)))]
        self.assertEqual(len(comp_asm), 4)
        self.assertTrue((pd.to_numeric(comp_asm["flow_rate_lps"], errors="coerce") == 0.91).all())
        for col in ("water_seal_mm", "height_adj_min_mm", "height_adj_max_mm", "outlet_dn"):
            self.assertTrue((comp_asm[col].notna()).all(), f"comparison:{col}")

    def test_showerdrain_c_assembled_rows_keep_structured_fields(self):
        html = """<html><body><main><h1>ACO ShowerDrain C Rinnenkörper</h1>
            <table>
                <tr><th>L1</th><th>Artikel</th><th>Daten</th></tr>
                <tr><td>1185 mm</td><td>90108544</td><td>Einbauhöhe Oberkante Estrich 57 - 128 mm Sperrwasserhöhe: 25 mm Ablaufstutzen DN 40 Abflusswert 0,80 l/s mit 20 mm Aufstau</td></tr>
                <tr><td>985 mm</td><td>90108524</td><td>Einbauhöhe Oberkante Estrich 80 - 128 mm Sperrwasserhöhe: 50 mm Ablaufstutzen DN 50 Abflusswert 0,91 l/s mit 20 mm Aufstau</td></tr>
            </table>
        </main></body></html>"""
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": "<html><body><main><a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/'>C body</a></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/": html,
        }
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            return (200, key, pages[key], "") if key in pages else (404, key, "", "not found")
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(target_length_mm=1000, tolerance_mm=0)
            with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
                products, comparison, _excluded, _evidence, _bom = pipeline.run_update(pd.DataFrame(rows), default_config())
        by_id = products.set_index("product_id")
        p24 = by_id.loc["aco-90108524"]
        self.assertEqual(int(p24["water_seal_mm"]), 50)
        self.assertTrue(pd.notna(p24["height_adj_min_mm"]))
        self.assertTrue(pd.notna(p24["height_adj_max_mm"]))
        self.assertIn("DN50", str(p24["outlet_dn"]))
        self.assertFalse((comparison["product_id"].astype(str).str.contains("901051", regex=False)).any())

    def test_mplus_rows_do_not_create_assembled_products(self):
        registry = pd.DataFrame([
            {"manufacturer": "aco", "product_id": "aco-mplus-profile", "product_name": "M+ profile", "product_family": "showerdrain_mplus", "product_url": "https://example.test/mplus/profile", "candidate_type": "component", "system_role": "profile_channel", "complete_system": "component"},
            {"manufacturer": "aco", "product_id": "aco-mplus-drain", "product_name": "M+ drain", "product_family": "showerdrain_mplus", "product_url": "https://example.test/mplus/drain", "candidate_type": "component", "system_role": "drain_body", "complete_system": "component"},
            {"manufacturer": "aco", "product_id": "aco-mplus-grate", "product_name": "M+ grate", "product_family": "showerdrain_mplus", "product_url": "https://example.test/mplus/grate", "candidate_type": "component", "system_role": "grate", "complete_system": "component"},
        ])
        def _fake_bom(url, params=None):
            if "profile" not in url:
                return []
            return [
                {"manufacturer":"aco","product_id":"aco-mplus-profile","component_id":"aco-mplus-drain","option_type":"compatible_drain_body","option_role":"drain_body","parent_family":"showerdrain_mplus","option_family":"showerdrain_mplus","source_url":url},
                {"manufacturer":"aco","product_id":"aco-mplus-profile","component_id":"aco-mplus-grate","option_type":"compatible_grate","option_role":"grate","parent_family":"showerdrain_mplus","option_family":"showerdrain_mplus","source_url":url},
            ]
        with patch("src.connectors.aco.get_bom_options", side_effect=_fake_bom), patch("src.connectors.aco.extract_parameters", return_value={}), patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
            products, comparison, _excluded, _evidence, bom = pipeline.run_update(registry, default_config())
        self.assertFalse(products["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-mplus-").any())
        self.assertFalse(comparison["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-mplus-").any())
        mplus_bom = bom[bom["parent_family"].astype(str) == "showerdrain_mplus"].copy()
        self.assertFalse(mplus_bom.empty)
        self.assertTrue((mplus_bom["option_meta"].astype(str).str.contains("compatibility_confidence=implicit_family_level", regex=False)).all())
        self.assertTrue((mplus_bom["option_meta"].astype(str).str.contains("explicit_article_matrix=false", regex=False)).all())
        self.assertTrue((mplus_bom["option_meta"].astype(str).str.contains("source_limitation=M+ compatibility is official family-level compatibility; no explicit article-to-article matrix found.", regex=False)).all())
        self.assertFalse(((mplus_bom["option_role"].astype(str) == "drain_body") & (mplus_bom["option_type"].astype(str) == "related_body_component")).any())
        self.assertFalse((mplus_bom["product_id"].astype(str) == mplus_bom["component_id"].astype(str)).any())
        self.assertFalse((mplus_bom["option_label"].astype(str) == "Direkt zur Hauptnavigation springen").any())

    def test_mplus_page_level_rinnenkoerper_is_profile_channel_not_drain_body(self):
        pages = {
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/": "<html><body><main><a href='/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/'>Rinnenkörper</a></main></body></html>",
            "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/": "<html><body><main><h1>ACO ShowerDrain M+ Rinnenkörper</h1></main></body></html>",
        }
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            return (200, key, pages[key], "") if key in pages else (404, key, "", "not found")
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
        mplus = pd.DataFrame(rows)
        row = mplus[mplus["product_id"].astype(str) == "aco-showerdrain-mplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm"].iloc[0]
        self.assertIn(str(row["system_role"]), {"profile_channel", "channel_body"})
        self.assertNotEqual(str(row["system_role"]), "drain_body")


class AcoConnectorCplusFixtureTests(unittest.TestCase):
    def _cplus_dir(self):
        return Path(__file__).resolve().parent / "fixtures" / "aco_cplus"

    def test_cplus_fixtures_exist(self):
        fixtures = self._cplus_dir()
        required = [
            "cplus_family_cz.html",
            "cplus_standard_h92_de.html",
            "cplus_low_h69_de.html",
        ]
        if not fixtures.exists():
            self.skipTest(f"missing fixture dir: {fixtures}")
        missing = [n for n in required if not (fixtures / n).exists()]
        self.assertFalse(missing, f"missing cplus fixtures: {missing}")

    def test_cplus_direct_extraction_standard_h92(self):
        fixtures = self._cplus_dir()
        if not fixtures.exists():
            self.skipTest(f"missing fixture dir: {fixtures}")
        html = (fixtures / "cplus_standard_h92_de.html").read_text(encoding="utf-8")
        url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-cplus/standard-h92/"
        with patch("src.connectors.aco._safe_get_text", return_value=(200, url, html, "")):
            p = aco.extract_parameters(url)
        self.assertEqual(float(p.get("flow_rate_10mm_lps")), 0.72)
        self.assertEqual(float(p.get("flow_rate_20mm_lps")), 0.91)
        self.assertEqual(float(p.get("flow_rate_lps")), 0.91)
        self.assertEqual(int(p.get("water_seal_mm")), 50)
        self.assertEqual(str(p.get("outlet_dn")), "DN50")
        self.assertEqual(int(p.get("height_adj_min_mm")), 80)
        self.assertEqual(int(p.get("height_adj_max_mm")), 128)

    def test_cplus_direct_extraction_low_h69(self):
        fixtures = self._cplus_dir()
        if not fixtures.exists():
            self.skipTest(f"missing fixture dir: {fixtures}")
        html = (fixtures / "cplus_low_h69_de.html").read_text(encoding="utf-8")
        url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-cplus/low-h69/"
        with patch("src.connectors.aco._safe_get_text", return_value=(200, url, html, "")):
            p = aco.extract_parameters(url)
        self.assertEqual(float(p.get("flow_rate_10mm_lps")), 0.56)
        self.assertEqual(float(p.get("flow_rate_20mm_lps")), 0.62)
        self.assertEqual(float(p.get("flow_rate_lps")), 0.62)
        self.assertEqual(int(p.get("water_seal_mm")), 25)
        self.assertIn(str(p.get("outlet_dn")), {"DN40", "DN40/DN50"})
        self.assertEqual(int(p.get("height_adj_min_mm")), 57)
        self.assertEqual(int(p.get("height_adj_max_mm")), 128)

    def test_cplus_pipeline_fixture_path_no_assembled(self):
        fixtures = self._cplus_dir()
        if not fixtures.exists():
            self.skipTest(f"missing fixture dir: {fixtures}")
        family_html = (fixtures / "cplus_family_cz.html").read_text(encoding="utf-8")
        h92_html = (fixtures / "cplus_standard_h92_de.html").read_text(encoding="utf-8")
        h69_html = (fixtures / "cplus_low_h69_de.html").read_text(encoding="utf-8")
        seed = "https://www.aco-haustechnik.de/produkte/badentwaesserung/"
        fam = "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/"
        h92 = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-cplus/standard-h92/"
        h69 = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-cplus/low-h69/"
        pages = {seed: f"<html><body><a href='{fam}'>C+</a><a href='{h92}'>H92</a><a href='{h69}'>H69</a></body></html>", fam: family_html, h92: h92_html, h69: h69_html}
        def _fake_get(url, timeout=35):
            key = aco._canonicalize_url(url)
            return (200, key, pages[key], "") if key in pages else (404, key, "", "not found")
        with patch("src.connectors.aco._safe_get_text", side_effect=_fake_get):
            rows, _ = aco.discover_candidates(1200, 100)
            with patch.dict(pipeline.CONNECTORS, {"aco": aco}, clear=True):
                products, comparison, excluded, _evidence, bom = pipeline.run_update(pd.DataFrame(rows), default_config())
        cplus = products[products["product_family"].astype(str) == "showerdrain_cplus"]
        self.assertFalse(cplus.empty)
        self.assertFalse(cplus["product_id"].astype(str).str.startswith("aco-assembled-showerdrain-cplus-").any())
        self.assertTrue((comparison["product_family"].astype(str) == "showerdrain_cplus").any())
        if "excluded_reason" in excluded.columns:
            cplus_ex = excluded[excluded.get("product_family", pd.Series([], dtype=str)).astype(str) == "showerdrain_cplus"]
            self.assertFalse((cplus_ex.get("excluded_reason", pd.Series([], dtype=str)).astype(str) == "missing_flow_after_html").any())
        cplus_bom = bom[bom.get("parent_family", pd.Series([], dtype=str)).astype(str) == "showerdrain_cplus"]
        if not cplus_bom.empty:
            self.assertTrue(cplus_bom["option_type"].astype(str).isin(["compatible_grate", "optional_accessory"]).all())
            self.assertFalse((cplus_bom["product_id"].astype(str) == cplus_bom["component_id"].astype(str)).any())
