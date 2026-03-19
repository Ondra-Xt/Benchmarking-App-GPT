import unittest
from unittest.mock import patch

from src.connectors import dallmer


class DallmerExtractionRegressionTests(unittest.TestCase):
    def _run_extract(self, sku: str, slug: str, html_text: str, pdf_text: str):
        product_url = f"https://www.dallmer.com/en/produkte/{sku}_{slug}.php"
        html = f"<html><body><main>{html_text}</main></body></html>"

        with patch.object(dallmer, "_safe_get_text", return_value=(200, product_url, html, "")), patch.object(
            dallmer, "_find_pdf_links", return_value=[]
        ), patch.object(dallmer, "extract_pdf_text_from_url", return_value=(pdf_text, "ok")):
            return dallmer.extract_parameters(product_url)

    def test_sku_521897_extracts_material_din_and_pdf_flow(self):
        params = self._run_extract(
            "521897",
            "shower-channel-ceraline-w-duo-1200-mm",
            "shower channel CeraLine W Duo 1200 mm DN 50",
            """
            Product sheet 521897 shower channel CeraLine W Duo 1200 mm, DN 50.
            Material: channel 304 stainless steel, drain body polypropylene.
            Product standard DIN EN 1253.
            Build-in height 95 mm.
            Required | Dallmer DN 50 | 0.80 l/s | 1.40 l/s.
            """,
        )
        self.assertEqual(params["flow_rate_lps"], 1.4)
        self.assertEqual(params["material_v4a"], "no")
        self.assertEqual(params["din_en_1253_cert"], "yes")
        self.assertIn("stainless steel", (params["material_detail"] or "").lower())

    def test_sku_521842_extracts_duo_flow_from_pdf_fallback(self):
        params = self._run_extract(
            "521842",
            "shower-channel-ceraline-f-duo-1200-mm",
            "shower channel CeraLine F Duo 1200 mm DN 50",
            """
            Product sheet 521842 shower channel CeraLine F Duo 1200 mm, DN 50.
            Material: stainless steel 1.4301, drain body polypropylene.
            Product standard DIN EN 1253.
            Installation height 95-115 mm.
            Required | Dallmer DN 50 | 0.80 l/s | 1.40 l/s.
            """,
        )
        self.assertEqual(params["flow_rate_lps"], 1.4)
        self.assertEqual(params["material_v4a"], "no")
        self.assertEqual(params["din_en_1253_cert"], "yes")
        self.assertEqual(params["height_adj_min_mm"], 95)
        self.assertEqual(params["height_adj_max_mm"], 115)

    def test_sku_523181_prefers_drainage_capacity_over_min_norm_flow(self):
        params = self._run_extract(
            "523181",
            "shower-channel-ceraline-plan-w-1200-mm-dn-50",
            "shower channel CeraLine Plan W 1200 mm DN 50",
            """
            Product sheet 523181 shower channel CeraLine Plan W 1200 mm, DN 50.
            Material: channel stainless steel 1.4301; drain body polypropylene.
            Product standard DIN EN 1253.
            Build-in height 85 mm.
            Min. flow rate according to norm 0.80 l/s.
            Water level according to norm 20.00 mm.
            Drainage capacity 0.70 l/s.
            """,
        )
        self.assertEqual(params["flow_rate_lps"], 0.7)
        self.assertEqual(params["din_en_1253_cert"], "yes")
        self.assertEqual(params["material_v4a"], "no")
        self.assertEqual(params["height_adj_min_mm"], 85)
        self.assertEqual(params["height_adj_max_mm"], 85)

    def test_sku_523792_supports_search_style_official_evidence(self):
        params = self._run_extract(
            "523792",
            "shower-channel-ceraline-nano-f-1200-mm-dn-50",
            "shower channel CeraLine Nano F 1200 mm DN 50",
            """
            Official brochure snippet for shower channel CeraLine Nano F 1200 mm, DN 50.
            Material: stainless steel 1.4301 and polypropylene.
            Product standard DIN EN 1253.
            Waterproofing according to DIN 18534.
            Build-in height 78 mm.
            Drainage capacity 0.46 l/s.
            """,
        )
        self.assertEqual(params["flow_rate_lps"], 0.46)
        self.assertEqual(params["din_en_1253_cert"], "yes")
        self.assertEqual(params["din_18534_compliance"], "yes")
        self.assertEqual(params["material_v4a"], "no")

    def test_discovery_marks_components_as_complete_system_no(self):
        component_url = "https://www.dallmer.com/en/produkte/521897_shower-channel-accessor-1200-mm.php"
        search_html = f'<html><body><a href="{component_url}">component</a></body></html>'

        def fake_safe_get_text(url, timeout=35):
            if "/search/" in url or "/search/index.php" in url:
                return 200, url, search_html, ""
            return 404, url, "", "not mocked"

        with patch.object(dallmer, "_safe_get_text", side_effect=fake_safe_get_text), patch.object(
            dallmer, "_robots_sitemaps", return_value=([], {"site": "dallmer"})
        ), patch.object(dallmer, "_crawl_sitemaps", return_value=([], [])), patch.object(
            dallmer, "_load_urls_from_previous_runs", return_value=([], None)
        ), patch.object(
            dallmer, "_dedupe_found_links_by_sku", return_value=([component_url], [], {component_url: component_url})
        ):
            results, _ = dallmer.discover_candidates(1200, 100)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["candidate_type"], "component")
        self.assertEqual(results[0]["complete_system"], "no")


if __name__ == "__main__":
    unittest.main()
