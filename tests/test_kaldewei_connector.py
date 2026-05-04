import unittest
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
            'kaldewei-ka-4121', 'kaldewei-ka-4122'
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

if __name__ == '__main__':
    unittest.main()
