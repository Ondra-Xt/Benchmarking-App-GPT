import unittest
from unittest.mock import patch
from src.connectors import kaldewei
from src.connectors import CONNECTORS

class KaldeweiConnectorTests(unittest.TestCase):
    def test_registered(self):
        self.assertIn('kaldewei', CONNECTORS)

    def test_discovery_primary_families_and_ids(self):
        rows, _dbg = kaldewei.discover_candidates()
        ids = {r['product_id'] for r in rows}
        expected = {
            'kaldewei-flowline-zero', 'kaldewei-flowpoint-zero',
            'kaldewei-flowdrain-horizontal-regular', 'kaldewei-flowdrain-horizontal-flat',
            'kaldewei-nexsys', 'kaldewei-ka-90-horizontal', 'kaldewei-ka-120-horizontal',
            'kaldewei-ka-300-horizontal', 'kaldewei-ka-125-legacy', 'kaldewei-xetis-ka-200',
            'kaldewei-ka-4121', 'kaldewei-ka-4122', 'kaldewei-nexsys-design-cover-brushed',
            'kaldewei-nexsys-design-cover-polished', 'kaldewei-nexsys-design-cover-coated-white'
        }
        self.assertTrue(expected.issubset(ids))
        self.assertFalse(any('hash' in x or x.endswith(')') for x in ids))

    def test_roles_and_tech_values(self):
        rows, _ = kaldewei.discover_candidates()
        by_id = {r['product_id']: r for r in rows}
        self.assertEqual(by_id['kaldewei-nexsys']['candidate_type'], 'drain')
        self.assertEqual(by_id['kaldewei-nexsys']['complete_system'], 'yes')
        self.assertEqual(by_id['kaldewei-flowline-zero']['candidate_type'], 'component')
        self.assertEqual(by_id['kaldewei-flowdrain-horizontal-regular']['flow_rate_lps'], 0.8)
        self.assertEqual(by_id['kaldewei-flowdrain-horizontal-regular']['outlet_dn'], 'DN50')
        self.assertEqual(by_id['kaldewei-flowdrain-horizontal-flat']['flow_rate_lps'], 0.63)
        self.assertEqual(by_id['kaldewei-ka-90-vertical']['flow_rate_lps'], 1.22)
        self.assertEqual(by_id['kaldewei-ka-300-flat']['flow_rate_lps'], 0.57)
        self.assertIn('unclear', by_id['kaldewei-ka-125-legacy']['current_status'])

    def test_bom_options(self):
        flow_opts = kaldewei.get_bom_options(kaldewei.SEEDS['flow'] + "#kaldewei-flowline-zero")
        self.assertTrue(any(o['component_id'] == 'kaldewei-flowdrain-horizontal-regular' for o in flow_opts))
        self.assertTrue(any(o['component_id'] == 'kaldewei-flowdrain-horizontal-flat' for o in flow_opts))
        nex_opts = kaldewei.get_bom_options(kaldewei.SEEDS['nexsys'] + "#kaldewei-nexsys")
        self.assertTrue(any(o['component_id'] == 'kaldewei-ka-4121' for o in nex_opts))
        self.assertTrue(any(o['component_id'] == 'kaldewei-ka-4122' for o in nex_opts))
        self.assertTrue(any(o['component_id'] == 'kaldewei-nexsys-design-cover-brushed' for o in nex_opts))
        flowline_opts = kaldewei.get_bom_options(kaldewei.SEEDS['flow'] + "#kaldewei-flowline-zero")
        finish_ids = {o['component_id'] for o in flowline_opts if o['option_type'] == 'compatible_finish'}
        self.assertEqual(len(finish_ids), 5)

    def test_source_validation_flags_review_required_cases(self):
        def fake_fetch(url, timeout=20):
            if "flowdrain-horizontal.pdf" in url:
                return {"status_code": 404, "final_url": url, "content": b"", "text": "", "content_type": "application/pdf", "mode": "binary_hash_only"}
            html = "<html><a href='/products/shower-surfaces/new-flow-item/'>new</a> FLOWLINE ZERO FLOWPOINT ZERO FLOWDRAIN NEXSYS KA 4121 KA 4122 KA 90 KA 120 KA 300 KA 125</html>"
            return {"status_code": 200, "final_url": url, "content": html.encode(), "text": html, "content_type": "text/html", "mode": "html_text"}
        with patch("src.connectors.kaldewei._fetch_source", side_effect=fake_fetch):
            rows = kaldewei.validate_kaldewei_sources(baseline_path="/tmp/does-not-exist.json")
        self.assertTrue(rows)
        self.assertTrue(any(r["review_required"] == "yes" for r in rows))
        self.assertTrue(any("baseline_missing" in r["review_reason"] for r in rows))
        self.assertTrue(any(r["status_code"] == 404 and r["review_required"] == "yes" for r in rows))

if __name__ == '__main__':
    unittest.main()
