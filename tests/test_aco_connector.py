import json
import unittest
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
                <table><tr><th>L1</th><th>Artikel</th></tr>
                    <tr><td>1185 mm</td><td>90108544</td></tr><tr><td>1185 mm</td><td>90108554</td></tr>
                    <tr><td>985 mm</td><td>90108524</td></tr><tr><td>985 mm</td><td>90108534</td></tr></table>
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
