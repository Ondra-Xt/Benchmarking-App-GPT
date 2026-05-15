import json
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco


class AcoConnectorDiscoveryTests(unittest.TestCase):
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



class AcoSplusPipelineComponentPropagationTests(unittest.TestCase):
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
        self.assertTrue(family_path.exists(), f"missing fixture: {family_path}")
        self.assertTrue(profile_path.exists(), f"missing fixture: {profile_path}")
        self.assertTrue(drain_path.exists(), f"missing fixture: {drain_path}")
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
        self.assertFalse(evidence[evidence["manufacturer"] == "aco"].empty)
