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

    def test_extracts_variant_from_pro_article_table(self):
        pro_url = "https://catalog.geberit.de/de-DE/product/PRO_170941/"
        html = """
        <html><body><main>
        <h1>Geberit Duschrinne System</h1>
        <a href="/docs/pro_170941.pdf">Produktdatenblatt herunterladen (PDF)</a>
        <table>
          <tr><th>Art.-Nr.</th><th>Länge</th><th>Ablaufleistung</th><th>DN</th><th>Einbauhöhe</th></tr>
          <tr><td>154.111.00.1</td><td>900 mm</td><td>0,4 l/s</td><td>DN 50</td><td>90 mm</td></tr>
          <tr><td>154.451.KS.1</td><td>1200 mm</td><td>0,8 l/s</td><td>DN 50</td><td>Einbauhöhe 100 mm</td></tr>
        </table>
        </main></body></html>
        """
        with patch.object(geberit, "_safe_get_text", return_value=(200, pro_url, html, "")):
            params = geberit.extract_parameters(pro_url)

        self.assertEqual(params["resolved_length_mm"], 1200)
        self.assertEqual(params["flow_rate_lps"], 0.8)
        self.assertEqual(params["flow_rate_unit"], "l/s")
        self.assertIn("0,8", params["flow_rate_raw_text"])
        self.assertEqual(params["outlet_dn_default"], "DN50")
        self.assertEqual(params["height_adj_min_mm"], 100)
        self.assertTrue(params["pdf_url"].endswith(".pdf/"))
        self.assertIn("154.451.KS.1", params["article_rows_json"])

    def test_uses_rendered_html_fallback_for_pdf_and_article_rows(self):
        pro_url = "https://catalog.geberit.de/de-DE/product/PRO_170944/"
        static_html = "<html><body><main><h1>Geberit Rohbauset</h1></main></body></html>"
        rendered_html = """
        <html><body><main>
        <h1>Geberit Rohbauset für CleanLine Duschrinnen</h1>
        <a href="/docs/pro_170944.pdf">Produktdatenblatt herunterladen (PDF)</a>
        <table>
          <tr><th>Art.-Nr.</th><th>Ablaufleistung l/s</th><th>DN</th><th>L cm</th><th>H cm</th></tr>
          <tr><td>154.452.KS.1</td><td>0,8</td><td>50</td><td>120</td><td>10</td></tr>
        </table>
        </main></body></html>
        """
        with patch.object(geberit, "_safe_get_text", return_value=(200, pro_url, static_html, "")), patch.object(
            geberit, "_safe_get_rendered_html", return_value=(True, rendered_html)
        ):
            params = geberit.extract_parameters(pro_url)
        self.assertIsNotNone(params["pdf_url"])
        self.assertIn("154.452.KS.1", params["article_rows_json"])
        self.assertEqual(params["flow_rate_lps"], 0.8)
        self.assertEqual(params["flow_rate_unit"], "l/s")
        self.assertIn("0,8", params["flow_rate_raw_text"])

    def test_extracts_cover_rows_from_pro_article_table(self):
        pro_url = "https://catalog.geberit.de/de-DE/product/PRO_1447036/"
        html = """
        <html><body><main>
        <h1>Geberit CleanLine Abdeckung</h1>
        <table>
          <tr><th>Art.-Nr.</th><th>Farbe/Oberfläche</th><th>L cm</th></tr>
          <tr><td>154.461.KS.1</td><td>Edelstahl</td><td>120</td></tr>
        </table>
        </main></body></html>
        """
        with patch.object(geberit, "_safe_get_text", return_value=(200, pro_url, html, "")):
            params = geberit.extract_parameters(pro_url)
        self.assertIn("154.461.KS.1", params["article_rows_json"])

    def test_edelstahl_without_explicit_v4a_stays_unknown(self):
        params = self._run_extract(
            "PRO_6666666",
            """
            Geberit CleanLine Abdeckung.
            Material Edelstahl.
            Art.-Nr. 154.316.00.1
            """,
        )
        self.assertEqual(params["material_detail"], "edelstahl")
        self.assertIsNone(params["material_v4a"])

    def test_does_not_infer_316_from_article_number_pattern(self):
        pro_url = "https://catalog.geberit.de/de-DE/product/PRO_999999/"
        html = """
        <html><body><main>
        <h1>Geberit CleanLine Abdeckung</h1>
        <table>
          <tr><th>Art.-Nr.</th><th>L cm</th><th>H cm</th></tr>
          <tr><td>154.316.00.1</td><td>120</td><td>1</td></tr>
        </table>
        </main></body></html>
        """
        with patch.object(geberit, "_safe_get_text", return_value=(200, pro_url, html, "")):
            params = geberit.extract_parameters(pro_url)
        self.assertIsNone(params["material_detail"])
        self.assertEqual(params["resolved_length_mm"], 1200)

    def test_cover_page_does_not_inherit_rohbauset_flow_or_height_range(self):
        pro_url = "https://catalog.geberit.de/de-DE/product/PRO_472894/"
        html = """
        <html><body><main>
        <h1>Geberit CleanLine Duschrinne befliesbar</h1>
        <table>
          <tr><th>Art.-Nr.</th><th>L cm</th><th>B cm</th><th>H cm</th><th>Farbe/Oberfläche</th></tr>
          <tr><td>154.441.KS.1</td><td>120</td><td>4,3</td><td>1</td><td>Edelstahl</td></tr>
        </table>
        Zusätzlich zu bestellen: Rohbauset, Einbauhöhe 90-220 mm, Ablaufleistung 0,8 l/s, DN 50.
        </main></body></html>
        """
        with patch.object(geberit, "_safe_get_text", return_value=(200, pro_url, html, "")):
            params = geberit.extract_parameters(pro_url)
        self.assertEqual(params["resolved_length_mm"], 1200)
        self.assertEqual(params["height_adj_min_mm"], 10)
        self.assertEqual(params["height_adj_max_mm"], 10)
        self.assertIsNone(params["flow_rate_lps"])
        self.assertIsNone(params["outlet_dn"])

    def test_discovery_accepts_pro_pages_and_rejects_siphon(self):
        system_url = geberit.CATALOG_SYSTEM_SEEDS[0]
        product_url = "https://catalog.geberit.de/de-DE/product/PRO_170941/"
        wrong_url = "https://catalog.geberit.de/de-DE/product/PRO_102454/"
        detail_url = "https://catalog.geberit.de/de-DE/product/PRO_170942/"
        system_html = f'<html><body><a href="{product_url}">CleanLine Produkt</a><a href="{wrong_url}">Siphon</a></body></html>'
        product_html = f"""
        <html><body><main>
        <h1>Geberit Produktseite</h1>
        Breadcrumb: Badezimmer / Duschbereich / Produktkatalog.
        <table><tr><th>Art.-Nr.</th><th>Ablaufleistung l/s</th><th>DN</th><th>L cm</th><th>H cm</th></tr><tr><td>154.451.KS.1</td><td>0,8</td><td>DN 50</td><td>120</td><td>10</td></tr></table>
        <a href="{detail_url}">Variante</a>
        </main></body></html>
        """
        detail_html = """
        <html><body><main>
        <h1>Geberit CleanLine Abdeckung</h1>
        <table><tr><th>Art.-Nr.</th><th>Farbe/Oberfläche</th><th>L cm</th></tr><tr><td>154.455.00.1</td><td>Edelstahl</td><td>120</td></tr></table>
        </main></body></html>
        """

        wrong_html = """
        <html><body><main>
        <h1>Rohrbogengeruchsverschluss für Ausgussbecken</h1>
        Siphon für Ausgussbecken.
        </main></body></html>
        """

        def fake_get(url, timeout=35):
            if url == system_url:
                return 200, system_url, system_html, ""
            if url == product_url:
                return 200, product_url, product_html, ""
            if url == wrong_url:
                return 200, wrong_url, wrong_html, ""
            if url == detail_url:
                return 200, detail_url, detail_html, ""
            if url == geberit.CATALOG_SYSTEM_SEEDS[1]:
                return 404, url, "", "not mocked"
            return 404, url, "", "not mocked"

        with patch.object(geberit, "_safe_get_text", side_effect=fake_get):
            candidates, debug = geberit.discover_candidates(target_length_mm=1200, tolerance_mm=100)

        self.assertTrue(candidates)
        self.assertEqual(candidates[0]["manufacturer"], "geberit")
        self.assertEqual(candidates[0]["candidate_type"], "drain")
        self.assertEqual(candidates[0]["complete_system"], "yes")
        summary = debug[-1]
        self.assertGreaterEqual(summary["total_found_links"], 1)
        self.assertGreaterEqual(summary["detail_pages_found"], 1)
        self.assertNotIn(system_url, summary["accepted_product_links"])
        self.assertIn(product_url, summary["accepted_product_links"])
        self.assertNotIn(detail_url, summary["accepted_product_links"])
        self.assertNotIn(wrong_url, summary["accepted_product_links"])
        self.assertIn(system_url, summary["sample_listing_urls"])
        self.assertIn(product_url, summary["sample_listing_card_urls"])
        self.assertIn(product_url, summary["sample_detail_urls"])
        self.assertNotIn("wrong_product_family", summary["dropped_reason_counts"])


if __name__ == "__main__":
    unittest.main()
