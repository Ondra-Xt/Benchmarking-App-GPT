import unittest
from unittest.mock import patch

from src.connectors import viega


class ViegaGoldenSetTests(unittest.TestCase):
    def test_validate_golden_set_reports_expected_classification_and_presence(self):
        def fake_get(url, timeout=35):
            title = url.split("/")[-1].replace(".html", "").replace("-", " ")
            html = f"<html><body><main><h1>{title}</h1><div class='breadcrumb'>Katalog Entwaesserungstechnik</div></main></body></html>"
            return 200, url, html, ""

        def fake_extract(url):
            return {
                "flow_rate_lps": 0.7,
                "outlet_dn": "DN50",
            }

        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(viega, "extract_parameters", side_effect=fake_extract):
            rows = viega.validate_golden_set()

        by_url = {r["url"]: r for r in rows}
        self.assertEqual(len(by_url), len(viega.VIEGA_GOLDEN_SET))
        for item in viega.VIEGA_GOLDEN_SET:
            row = by_url[item["url"]]
            self.assertEqual(row["family_detected"], item["family"])
            self.assertIn(row["drain_category_detected"], item["drain_category"])
            self.assertIn(row["system_role_detected"], item["system_role"])
            self.assertEqual(row["accepted_or_not"], item["should_be_accepted"])
            self.assertEqual(row["flow_found_or_not"], item["expected_flow_presence"])
            self.assertEqual(row["outlet_dn_found_or_not"], item["expected_outlet_dn_presence"])


if __name__ == "__main__":
    unittest.main()
