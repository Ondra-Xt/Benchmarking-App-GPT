import json
import unittest
from unittest.mock import patch

from src.connectors import viega


class ViegaExtractionRegressionTests(unittest.TestCase):
    def _run_extract(self, slug: str, html_main: str):
        url = f"https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/{slug}.html"
        html = f"<html><body><main>{html_main}</main></body></html>"
        with patch.object(viega, "_safe_get_text", return_value=(200, url, html, "")):
            return viega.extract_parameters(url)

    def test_4983_10_complete_drain_extracts_flow_pdf_and_article_rows(self):
        params = self._run_extract(
            "Advantix-Duschrinne-4983-10",
            """
            <h1>Advantix Duschrinne 4983.10</h1>
            <a href="/docs/4983_10_datenblatt.pdf">Technische Daten / Datenblatt</a>
            Material Edelstahl 1.4301.
            Ablaufleistung Anstauhöhe 10 mm 0,5 l/s.
            Ablaufleistung Anstauhöhe 20 mm 0,55 l/s.
            güteüberwacht nach DIN EN 1253.
            Ablauf DN40 drehbar, Übergangsstück auf DN50.
            <table>
              <tr><th>L</th><th>Artikel</th></tr>
              <tr><td>1200</td><td>4983.10</td></tr>
            </table>
            """,
        )
        self.assertTrue(params["pdf_url"].endswith(".pdf"))
        self.assertIsNotNone(params["article_rows_json"])
        self.assertEqual(params["flow_rate_lps"], 0.55)
        self.assertEqual(params["flow_rate_lps_10mm"], 0.5)
        self.assertEqual(params["flow_rate_lps_20mm"], 0.55)
        self.assertEqual(params["outlet_dn_default"], "DN50")

    def test_4981_10_rich_table_maps_bh_dn_and_colours(self):
        params = self._run_extract(
            "Advantix-Cleviva-Duschrinne-4981-10",
            """
            <h1>Advantix Cleviva-Duschrinne 4981.10</h1>
            Material Edelstahl 1.4301.
            Ablaufleistung Anstauhöhe 10 mm 0,5-0,65 l/s.
            Ablaufleistung Anstauhöhe 20 mm 0,55-0,7 l/s.
            Abdichtungsmanschette werkseitig vormontiert.
            <table>
              <tr><th>L</th><th>BH</th><th>DN</th><th>Ausführung</th><th>VE</th><th>Artikel</th></tr>
              <tr><td>1200</td><td>70-95</td><td>DN 50</td><td>Edelstahl</td><td>1</td><td>4981.10</td></tr>
            </table>
            """,
        )
        self.assertEqual(params["resolved_length_mm"], 1200)
        self.assertEqual(params["height_adj_min_mm"], 70)
        self.assertEqual(params["height_adj_max_mm"], 95)
        self.assertEqual(params["outlet_dn"], "DN50")
        self.assertEqual(params["sealing_fleece_preassembled"], "yes")
        self.assertEqual(params["colours_count"], 1)

    def test_4982_10_base_body_is_component_candidate(self):
        self.assertEqual(
            viega._classify_candidate(
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen-Grundkoerper-4982-10.html",
                "Advantix-Duschrinnen-Grundkörper 4982.10",
                "Grundkörper mit L und Artikel",
            ),
            "component",
        )

    def test_4981_90_vertical_outlet_hint(self):
        params = self._run_extract(
            "Advantix-Cleviva-Duschrinnen-Ablauf-4981-90",
            """
            <h1>Advantix Cleviva-Duschrinnen-Ablauf 4981.90</h1>
            senkrecht.
            Ablaufleistung Anstauhöhe 10 mm 0,5 l/s.
            Ablaufleistung Anstauhöhe 20 mm 0,7 l/s.
            <table>
              <tr><th>DN</th><th>Artikel</th></tr>
              <tr><td>DN 50</td><td>4981.90</td></tr>
            </table>
            """,
        )
        self.assertEqual(params["outlet_direction_hint"], "vertical")
        self.assertEqual(params["flow_rate_lps"], 0.7)
        self.assertEqual(params["outlet_dn_default"], "DN50")

    def test_tempoplex_is_classified_as_drain_taxonomy(self):
        cand_type, drain_category, system_role, complete_system = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Badewannen-und-Duschwannenablaeufe/Tempoplex/Tempoplex-Ablauf-6963-1.html",
            "Tempoplex-Ablauf 6963.1",
            "Tempoplex Duschwannenablauf",
        )
        self.assertEqual(cand_type, "drain")
        self.assertEqual(drain_category, "shower_tray_drain")
        self.assertEqual(system_role, "complete_drain")
        self.assertEqual(complete_system, "yes")

    def test_discovery_spans_advantix_and_tempoplex_seeds(self):
        tempoplex_url = "https://www.viega.de/de/produkte/Katalog/Badewannen-und-Duschwannenablaeufe/Tempoplex/Tempoplex-Ablauf-6963-1.html"
        advantix_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html"

        def fake_get(url, timeout=35):
            if url == tempoplex_url:
                html = "<html><body><main><h1>Tempoplex-Ablauf 6963.1</h1></main></body></html>"
                return 200, url, html, ""
            if url == advantix_url:
                html = "<html><body><main><h1>Advantix Duschrinne 4983.10</h1></main></body></html>"
                return 200, url, html, ""
            return 200, url, "<html><body><a href='x'>seed</a></body></html>", ""

        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(
            viega, "_crawl_category_pages", return_value={tempoplex_url, advantix_url}
        ):
            rows, _dbg = viega.discover_candidates(1200, 100)

        urls = {r["product_url"] for r in rows}
        self.assertIn(tempoplex_url, urls)
        self.assertIn(advantix_url, urls)
        by_url = {r["product_url"]: r for r in rows}
        self.assertEqual(by_url[tempoplex_url]["drain_category"], "shower_tray_drain")
        self.assertEqual(by_url[advantix_url]["drain_category"], "line_channel")


if __name__ == "__main__":
    unittest.main()
