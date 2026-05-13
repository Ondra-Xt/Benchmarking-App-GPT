import unittest
from unittest.mock import patch

from src.connectors import aco


class TestAcoShowerDrainCRowParsing(unittest.TestCase):
    def _mock_html(self):
        return """
        <html><body><main>
        <table>
          <tr>
            <th>Artikel-Nr.</th><th>Einbauhöhe mm</th><th>Abflussleistung l/s</th><th>Abflussleistung 10 mm l/s</th><th>Abflussleistung 20 mm l/s</th><th>Geruchverschluss mm</th>
          </tr>
          <tr>
            <td>90123456</td><td>57-128</td><td>0,70</td><td>0,45</td><td>0,70</td><td>25</td>
          </tr>
          <tr>
            <td>90123457</td><td>80-128</td><td>0,91</td><td>0,60</td><td>0,91</td><td>50</td>
          </tr>
        </table>
        </main></body></html>
        """

    @patch("src.connectors.aco._safe_get_text")
    def test_row_specific_flow_and_variant_metadata(self, mock_get):
        mock_get.return_value = (200, "https://www.aco-haustechnik.de/x", self._mock_html(), "")

        low = aco.extract_parameters("https://www.aco-haustechnik.de/x#article=90123456")
        std = aco.extract_parameters("https://www.aco-haustechnik.de/x#article=90123457")

        self.assertEqual(low["flow_rate_lps"], 0.70)
        self.assertEqual(low["flow_rate_10mm_lps"], 0.45)
        self.assertEqual(low["flow_rate_20mm_lps"], 0.70)
        self.assertEqual(low["water_seal_mm"], 25)
        self.assertEqual(low["height_adj_min_mm"], 57)
        self.assertEqual(low["height_adj_max_mm"], 128)

        self.assertEqual(std["flow_rate_lps"], 0.91)
        self.assertEqual(std["water_seal_mm"], 50)
        self.assertEqual(std["height_adj_min_mm"], 80)
        self.assertEqual(std["height_adj_max_mm"], 128)

        self.assertNotEqual(low["flow_rate_lps"], std["flow_rate_lps"])
        self.assertNotEqual(low["water_seal_mm"], std["water_seal_mm"])

    @patch("src.connectors.aco._safe_get_text")
    def test_evidence_points_to_official_source(self, mock_get):
        final_url = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/"
        mock_get.return_value = (200, final_url, self._mock_html(), "")

        params = aco.extract_parameters(f"{final_url}#article=90123456")

        evidence_urls = [e[2] for e in params.get("evidence", []) if len(e) >= 3]
        self.assertTrue(any("aco-haustechnik.de" in (u or "") for u in evidence_urls))


if __name__ == "__main__":
    unittest.main()
