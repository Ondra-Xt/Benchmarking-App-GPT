import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pandas as pd

from src.config import default_config
from src.excel_export import export_excel
from src import pipeline


class _FakeConnectorYes:
    @staticmethod
    def extract_parameters(url):
        return {"flow_rate_lps": 0.6, "evidence": [("Flow", "0.6 l/s", url)]}

    @staticmethod
    def get_bom_options(url, params=None):
        return []


class PipelineExportTests(unittest.TestCase):
    def _make_template(self, path: Path):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Candidates_All"
        ws.append(["old"])
        ws.append(["stale"])
        for name in ["Excluded", "Products", "BOM_Options", "Components", "Evidence", "Comparison"]:
            x = wb.create_sheet(name)
            x.append(["old"])
            x.append(["stale"])
        wb.save(path)

    def _sheet_rows(self, path: Path, sheet: str):
        wb = openpyxl.load_workbook(path)
        ws = wb[sheet]
        return list(ws.iter_rows(values_only=True))

    def test_export_overwrites_candidates_all_with_latest_registry_only(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)

            first = pd.DataFrame([{"manufacturer": "Dallmer", "product_id": "d1"}])
            second = pd.DataFrame([{"manufacturer": "hansgrohe", "product_id": "h1"}])

            export_excel(template, out, default_config(), registry_df=first)
            export_excel(template, out, default_config(), registry_df=second)

            rows = self._sheet_rows(out, "Candidates_All")
            self.assertEqual(rows[0], ("manufacturer", "product_id"))
            self.assertEqual(rows[1], ("hansgrohe", "h1"))
            self.assertEqual(len(rows), 2)

    def test_export_overwrites_products_excluded_and_evidence_with_latest_run(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)

            export_excel(
                template,
                out,
                default_config(),
                registry_df=pd.DataFrame([{"manufacturer": "dallmer", "product_id": "old"}]),
                products_df=pd.DataFrame([{"manufacturer": "dallmer", "product_id": "old"}]),
                comparison_df=pd.DataFrame([{"manufacturer": "dallmer", "product_id": "old"}]),
                excluded_df=pd.DataFrame([{"manufacturer": "dallmer", "product_id": "old", "excluded_reason": "old"}]),
                evidence_df=pd.DataFrame([{"manufacturer": "dallmer", "product_id": "old", "label": "old", "source": "old"}]),
            )
            export_excel(
                template,
                out,
                default_config(),
                registry_df=pd.DataFrame([{"manufacturer": "hansgrohe", "product_id": "new"}]),
                products_df=pd.DataFrame([{"manufacturer": "hansgrohe", "product_id": "new"}]),
                comparison_df=pd.DataFrame([{"manufacturer": "hansgrohe", "product_id": "new"}]),
                excluded_df=pd.DataFrame([{"manufacturer": "aco", "product_id": "x1", "excluded_reason": "missing_flow"}]),
                evidence_df=pd.DataFrame([{"manufacturer": "hansgrohe", "product_id": "new", "label": "Flow", "source": "u"}]),
            )

            self.assertEqual(self._sheet_rows(out, "Products")[1], ("hansgrohe", "new"))
            self.assertEqual(self._sheet_rows(out, "Comparison")[1], ("hansgrohe", "new"))
            self.assertEqual(self._sheet_rows(out, "Excluded")[1], ("aco", "x1", "missing_flow"))
            self.assertEqual(self._sheet_rows(out, "Evidence")[1], ("hansgrohe", "new", "Flow", "u"))
            self.assertEqual(len(self._sheet_rows(out, "Products")), 2)
            self.assertEqual(len(self._sheet_rows(out, "Excluded")), 2)
            self.assertEqual(len(self._sheet_rows(out, "Evidence")), 2)

    def test_run_update_excludes_complete_system_no_and_normalizes_manufacturer(self):
        registry = pd.DataFrame(
            [
                {
                    "manufacturer": "Dallmer",
                    "product_id": "A1",
                    "product_name": "Accessory",
                    "product_url": "https://example.com/a1",
                    "candidate_type": "component",
                    "complete_system": "NO",
                    "excluded_reason": "complete_system_no",
                },
                {
                    "manufacturer": "dAllMer",
                    "product_id": "A2",
                    "product_name": "Drain",
                    "product_url": "https://example.com/a2",
                    "candidate_type": "drain",
                    "complete_system": "yes",
                },
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"dallmer": _FakeConnectorYes()}, clear=False):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

        self.assertEqual(products["manufacturer"].tolist(), ["dallmer"])
        self.assertEqual(products["product_id"].tolist(), ["A2"])
        self.assertEqual(comparison["product_id"].tolist(), ["A2"])
        self.assertEqual(excluded["product_id"].tolist(), ["A1"])
        self.assertEqual(excluded["excluded_reason"].tolist(), ["complete_system_no"])
        self.assertTrue((evidence["product_id"] == "A2").all())
        self.assertTrue(bom.empty)


if __name__ == "__main__":
    unittest.main()
