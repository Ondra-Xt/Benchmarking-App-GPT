import unittest
from unittest.mock import patch
from src.connectors import kaldewei
from src.connectors import CONNECTORS

class KaldeweiConnectorTests(unittest.TestCase):
    def test_registered(self):
        self.assertIn('kaldewei', CONNECTORS)

    def test_source_registry_contains_required_ids(self):
        required = {
            'kaldewei-flow-page', 'kaldewei-flowdrain-horizontal-pdf', 'kaldewei-nexsys-product-page',
            'kaldewei-nexsys-ka-4121-4122-pdf', 'kaldewei-waste-systems-page', 'kaldewei-calima-ka-300-page',
            'kaldewei-conoflat-ka-120-techdata', 'kaldewei-ka-120-ka-125-legacy-sheet',
            'kaldewei-xetis-ka-200-installation-sheet'
        }
        ids = {row['source_id'] for row in kaldewei.SOURCE_REGISTRY}
        self.assertTrue(required.issubset(ids))

    def test_source_validation_review_logic(self):
        good_html = "<html>FLOWLINE ZERO FLOWPOINT ZERO FLOWDRAIN brushed steel brushed champagne brushed graphite alpine white matt black matt 100</html>"
        def fake_fetch(url, timeout=20):
            if 'kaldewei-flow' in url:
                return {"status_code": 200, "final_url": url, "content": good_html.encode(), "text": good_html, "content_type": "text/html", "mode": "html_text"}
            if 'nexsys' in url:
                html = '<html>NEXSYS design cover</html>'
                return {"status_code": 200, "final_url": url, "content": html.encode(), "text": html, "content_type": "text/html", "mode": "html_text"}
            if 'conoflat' in url:
                return {"status_code": 503, "final_url": url, "content": b"", "text": "", "content_type": "text/html", "mode": "html_text"}
            return {"status_code": None, "final_url": url, "content": b"", "text": "", "content_type": "", "mode": "error", "error": "ConnectionError: boom"}

        baseline = [{"source_id": "kaldewei-flow-page", "baseline_hash_sha256": kaldewei.hashlib.sha256(good_html.encode()).hexdigest(), "baseline_content_length": len(good_html.encode())}]
        with patch('src.connectors.kaldewei._fetch_source', side_effect=fake_fetch), patch('src.connectors.kaldewei.json.load', return_value=baseline), patch('builtins.open'):
            rows = kaldewei.validate_kaldewei_sources('/tmp/base.json')

        by_id = {r['source_id']: r for r in rows}
        self.assertEqual(by_id['kaldewei-flow-page']['review_required'], 'no')
        self.assertIn('baseline_missing', by_id['kaldewei-nexsys-product-page']['review_reason'])
        self.assertIn('unreachable_or_non_200', by_id['kaldewei-conoflat-ka-120-techdata']['review_reason'])
        self.assertIn('fetch_error', by_id['kaldewei-xetis-ka-200-installation-sheet']['review_reason'])
        self.assertIn('expected_terms_missing', by_id['kaldewei-nexsys-product-page']['review_reason'])


    def test_candidate_filtering_and_conoflat_warning_only(self):
        conoflat_html = """<html>CONOFLAT
        <a href="https://www.kaldewei.com/products/showers/detail/product/conoflat/">same</a>
        <a href="https://www.kaldewei.com/en/products/showers/detail/product/conoflat/?utm=1">lang</a>
        <a href="https://images.cdn.kaldewei.com/x.jpg">img</a>
        <a href="https://www.kaldewei.com/products/showers/shower-accessories/">nav</a>
        <a href="https://www.kaldewei.de/products/showers/detail/product/nexsys/">local</a>
        <a href="https://pricelist.kaldewei.com/catalog/product/fake">price</a>
        <a href="https://files.cdn.kaldewei.com/data/sprachen/deutsch/techdata/new-ka-doc.pdf">pdf</a>
        <a href="https://www.kaldewei.com/products/showers/detail/product/new-flow-system/">newprod</a>
        </html>"""
        def fake_fetch(url, timeout=20):
            if 'conoflat' in url:
                return {"status_code": 200, "final_url": url, "content": conoflat_html.encode(), "text": conoflat_html, "content_type": "text/html", "mode": "html_text"}
            return {"status_code": 200, "final_url": url, "content": b'<html></html>', "text": '<html></html>', "content_type": "text/html", "mode": "html_text"}
        with patch('src.connectors.kaldewei._fetch_source', side_effect=fake_fetch), patch('src.connectors.kaldewei.json.load', return_value=[]), patch('builtins.open'):
            rows = kaldewei.validate_kaldewei_sources('/tmp/base.json')
        row = {r['source_id']: r for r in rows}['kaldewei-conoflat-ka-120-techdata']
        self.assertNotIn('expected_terms_missing', row['review_reason'])
        self.assertIn('warning_terms_missing', row['review_warning'])
        self.assertGreaterEqual(row['new_source_candidate_count'], 2)
        self.assertGreaterEqual(row['new_pdf_source_candidate_count'], 1)
        self.assertGreaterEqual(row['new_product_source_candidate_count'], 1)
        self.assertGreaterEqual(int(row['ignored_language_variant_candidates_count']), 1)
        self.assertGreaterEqual(int(row['ignored_pricelist_candidates_count']), 1)

    def test_ka120_structured_variants_from_seed_catalog(self):
        rows, _ = kaldewei.discover_candidates()
        ka120 = [r for r in rows if str(r.get("family")) == "ka_120"]
        self.assertGreater(len(ka120), 1)
        self.assertTrue(all(str(r.get("candidate_type")) == "component" for r in ka120))
        self.assertTrue(all(str(r.get("complete_system")) == "component" for r in ka120))
        self.assertTrue(all(str(r.get("system_role")) == "tray_waste_fitting" for r in ka120))
        self.assertTrue(all(str(r.get("selected_length_mm")) in {"", "not_applicable"} for r in ka120))
        flow_vals = {float(r.get("flow_rate_lps")) for r in ka120 if r.get("flow_rate_lps") not in (None, "")}
        self.assertIn(0.85, flow_vals)
        self.assertIn(1.4, flow_vals)
        self.assertTrue(any(str(r.get("outlet_orientation")) == "horizontal" for r in ka120))
        self.assertTrue(any(str(r.get("outlet_orientation")) == "vertical" for r in ka120))
        self.assertEqual({str(r.get("model_number")) for r in ka120}, {"4106", "4107", "4108"})
        self.assertEqual(
            {str(r.get("article_number")) for r in ka120},
            {"687772530000", "687772510000", "687772520000"},
        )

    def test_ka90_structured_variants_from_seed_catalog(self):
        rows, _ = kaldewei.discover_candidates()
        ka90 = [r for r in rows if str(r.get("family")) == "ka_90"]
        self.assertEqual(len(ka90), 3)
        self.assertTrue(all(str(r.get("candidate_type")) == "component" for r in ka90))
        self.assertTrue(all(str(r.get("product_category")) == "tray_waste_fitting" for r in ka90))
        self.assertTrue(all(str(r.get("system_role")) == "tray_waste_fitting" for r in ka90))
        self.assertTrue(all(str(r.get("complete_system")) == "component" for r in ka90))
        self.assertTrue(all(str(r.get("selected_length_mm")) in {"", "not_applicable"} for r in ka90))
        by_model = {str(r.get("model_number")): r for r in ka90}
        self.assertEqual(float(by_model["4103"]["flow_rate_lps"]), 0.71)
        self.assertEqual(str(by_model["4103"]["outlet_dn"]), "DN50")
        self.assertEqual(str(by_model["4103"]["article_number"]), "687772560999")
        self.assertEqual(int(by_model["4103"]["water_seal_mm"]), 50)
        self.assertEqual(int(by_model["4103"]["construction_height_mm"]), 80)
        self.assertEqual(float(by_model["4104"]["flow_rate_lps"]), 0.68)
        self.assertEqual(str(by_model["4104"]["outlet_dn"]), "DN40")
        self.assertEqual(str(by_model["4104"]["article_number"]), "687772540999")
        self.assertEqual(int(by_model["4104"]["water_seal_mm"]), 30)
        self.assertEqual(int(by_model["4104"]["construction_height_mm"]), 60)
        self.assertEqual(float(by_model["4105"]["flow_rate_lps"]), 1.22)
        self.assertEqual(str(by_model["4105"]["outlet_dn"]), "DN50")
        self.assertEqual(str(by_model["4105"]["article_number"]), "687772550999")
        self.assertEqual(int(by_model["4105"]["water_seal_mm"]), 50)
        self.assertEqual(int(by_model["4105"]["construction_height_mm"]), 80)

if __name__ == '__main__':
    unittest.main()
