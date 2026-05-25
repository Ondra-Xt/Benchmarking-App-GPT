import unittest
from src.config import default_config
from src.scoring import compute_parameter_score

class ScoringBenchmarkTests(unittest.TestCase):
    def test_default_weights_sum_100(self):
        self.assertEqual(sum(default_config().final_weights_pct.values()), 100)

    def test_flow_rate_scores(self):
        s,d=compute_parameter_score({'flow_rate_lps':0.8}, default_config()); self.assertEqual(d['flow_rate_score'],1.0)
        s,d=compute_parameter_score({'flow_rate_lps':0.63}, default_config()); self.assertAlmostEqual(d['flow_rate_score'],0.7875,4)
        s,d=compute_parameter_score({}, default_config()); self.assertEqual(d['flow_rate_score'],0.0)

    def test_material_scores(self):
        self.assertEqual(compute_parameter_score({'material_detail':'1.4404 V4A'}, default_config())[1]['material_v4a_score'],1.0)
        self.assertEqual(compute_parameter_score({'material_detail':'1.4301'}, default_config())[1]['material_v4a_score'],0.5)
        self.assertEqual(compute_parameter_score({'material_detail':'polypropylene'}, default_config())[1]['material_v4a_score'],0.2)
        self.assertEqual(compute_parameter_score({}, default_config())[1]['material_v4a_score'],0.0)

    def test_height_adjustability(self):
        d=compute_parameter_score({'height_adj_min_mm':78,'height_adj_max_mm':179}, default_config())[1]
        self.assertEqual(d['height_adjustability_range_mm'],101)
        self.assertEqual(d['height_adjustability_score'],1.0)
        d=compute_parameter_score({'height_adj_min_mm':58,'height_adj_max_mm':78}, default_config())[1]
        self.assertEqual(d['height_adjustability_score'],0.2)
        d=compute_parameter_score({'construction_height_mm':80}, default_config())[1]
        self.assertEqual(d['height_adjustability_score'],0.0)

if __name__=='__main__':
    unittest.main()
