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

if __name__ == '__main__':
    unittest.main()
