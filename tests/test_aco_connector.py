import json
import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
