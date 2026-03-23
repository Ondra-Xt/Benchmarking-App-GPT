import unittest
from unittest.mock import patch

from src.connectors import geberit


class GeberitExtractionRegressionTests(unittest.TestCase):
    def _run_extract(self, slug: str, html_text: str):
        product_url = f"https://catalog.geberit.de/de-DE/product/{slug}/"
        html = f"<html><body><main>{html_text}</main></body></html>"
        with patch.object(geberit, "_safe_get_text", return_value=(200, product_url, html, "")):
            return geberit.extract_parameters(product_url)

    def test_extracts_material_din_and_sealing_fleece(self):
        params = self._run_extract(
            "PRO_1111111",
            """
            Geberit CleanLine80 Duschrinne Werkstoff Edelstahl 1.4404.
            Geprüft nach DIN EN 1253.
            Verbundabdichtung nach DIN 18534.
            Dichtvlies werkseitig montiert.
            Ablaufleistung 0,8 l/s. DN 50. Einbauhöhe 90 mm.
            """,
        )
        self.assertEqual(params["material_detail"], "1.4404")
        self.assertEqual(params["material_v4a"], "yes")
        self.assertEqual(params["din_en_1253_cert"], "yes")
        self.assertEqual(params["din_18534_compliance"], "yes")
        self.assertEqual(params["sealing_fleece_preassembled"], "yes")
        self.assertEqual(params["flow_rate_lps"], 0.8)
        self.assertEqual(params["outlet_dn_default"], "DN50")

    def test_extracts_colours_count_and_v2a_material_token(self):
        params = self._run_extract(
            "PRO_2222222",
            """
            Geberit CleanLine50 Duschprofil Material Edelstahl 1.4301.
            Farben: Edelstahl, Champagner, Schwarzchrom.
            Ablaufleistung 0,55 l/s. DN 40 / DN 50. Installationshöhe 80-100 mm.
            """,
        )
        self.assertEqual(params["material_detail"], "1.4301")
        self.assertEqual(params["material_v4a"], "no")
        self.assertEqual(params["colours_count"], 3)
        self.assertEqual(params["outlet_dn"], "DN40/DN50")
        self.assertEqual(params["height_adj_min_mm"], 80)
        self.assertEqual(params["height_adj_max_mm"], 100)

    def test_extracts_numeric_colours_count(self):
        params = self._run_extract(
            "PRO_3333333",
            """
            Geberit CleanLine20 Duschrinne in 4 Farben.
            Werkstoff Edelstahl 304.
            Ablaufleistung 0,5 l/s. DN 50. Einbauhöhe 70 mm.
            """,
        )
        self.assertEqual(params["material_detail"], "304")
        self.assertEqual(params["material_v4a"], "no")
        self.assertEqual(params["colours_count"], 4)

    def test_discovery_accepts_product_reached_from_cleanline_context(self):
        marketing_url = geberit.MARKETING_SEED
        landing_url = geberit.PUBLIC_SEEDS[1]
        product_url = "https://catalog.geberit.de/de-DE/product/154.441.KS.1/"
        unrelated_url = "https://www.geberit.de/badezimmerprodukte/duofix/"
        marketing_html = f'<html><body><a href="{landing_url}">CleanLine30</a><a href="{unrelated_url}">Irgendein Produkt</a></body></html>'
        landing_html = f'<html><body><a href="{product_url}">CleanLine80</a></body></html>'
        product_html = """
        <html><body><main>
        <h1>Geberit Duschentwässerung 1200 mm</h1>
        Produktseite 1200 mm für bodenebene Duschen.
        </main></body></html>
        """
        unrelated_html = """
        <html><body><main>
        <h1>Geberit Duofix Element</h1>
        Irgendein anderes Produkt 1200 mm.
        </main></body></html>
        """

        def fake_get(url, timeout=35):
            if url == marketing_url:
                return 200, marketing_url, marketing_html, ""
            if url == landing_url:
                return 200, landing_url, landing_html, ""
            if url == product_url:
                return 200, product_url, product_html, ""
            if url == unrelated_url:
                return 200, unrelated_url, unrelated_html, ""
            return 404, url, "", "not mocked"

        with patch.object(geberit, "_safe_get_text", side_effect=fake_get):
            candidates, debug = geberit.discover_candidates(target_length_mm=1200, tolerance_mm=100)

        self.assertTrue(candidates)
        self.assertEqual(candidates[0]["manufacturer"], "geberit")
        self.assertEqual(candidates[0]["candidate_type"], "drain")
        self.assertEqual(candidates[0]["complete_system"], "yes")
        summary = debug[-1]
        self.assertGreaterEqual(summary["total_found_links"], 1)
        self.assertGreaterEqual(summary["landing_pages_found"], 1)
        self.assertGreaterEqual(summary["detail_pages_found"], 1)
        self.assertIn(product_url, summary["accepted_product_links"])
        self.assertNotIn(unrelated_url, summary["accepted_product_links"])
        self.assertIn(landing_url, summary["sample_landing_urls"])
        self.assertIn(product_url, summary["sample_detail_urls"])
        self.assertIn("sample_accepted_urls", summary)
        self.assertIn("sample_rejected_urls", summary)
        self.assertIn("dropped_reason_counts", summary)
        self.assertIn("sample_rejected_lengths", summary)
        self.assertIn("sample_missing_length_rows", summary)


if __name__ == "__main__":
    unittest.main()
