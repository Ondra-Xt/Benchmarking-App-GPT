import tempfile
import unittest
import re
import warnings
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pandas as pd

from src.config import default_config
from src.excel_export import export_excel
from src import pipeline
from src.connectors import kaldewei


class _FakeConnectorYes:
    @staticmethod
    def extract_parameters(url):
        return {"flow_rate_lps": 0.6, "evidence": [("Flow", "0.6 l/s", url)]}

    @staticmethod
    def get_bom_options(url, params=None):
        return []


class _FakeDiscoverA:
    @staticmethod
    def discover_candidates(target_length_mm=1200, tolerance_mm=100):
        return ([{"manufacturer": "dallmer", "product_id": "d-1", "product_name": "A", "product_url": "https://a.example/p"}], [])


class _FakeDiscoverB:
    @staticmethod
    def discover_candidates(target_length_mm=1200, tolerance_mm=100):
        return ([{"manufacturer": "hansgrohe", "product_id": "h-1", "product_name": "B", "product_url": "https://b.example/p"}], [])


class _FakeViegaConnector:
    @staticmethod
    def extract_parameters(url):
        return {"flow_rate_lps": 0.7, "outlet_dn": "DN50", "material_detail": "Edelstahl 1.4301", "evidence": []}

    @staticmethod
    def get_bom_options(url, params=None):
        return []


class _FakeAcoConnector:
    @staticmethod
    def extract_parameters(url):
        u = str(url or "").lower()
        if "showerdrain-c" in u and "901085" in u:
            return {
                "flow_rate_lps": 0.8,
                "outlet_dn": "DN50",
                "height_adj_min_mm": 57,
                "height_adj_max_mm": 128,
                "din_en_1253_cert": True,
                "evidence": [("ACO", "row variant", url)],
            }
        if any(k in u for k in ("komplettablauf", "showerpoint", "passino", "passavant", "public-80")):
            return {
                "flow_rate_lps": 0.6,
                "outlet_dn": "DN50",
                "height_adj_min_mm": 65,
                "height_adj_max_mm": 95,
                "din_en_1253_cert": True,
                "evidence": [("ACO", "complete system", url)],
            }
        return {
            "flow_rate_lps": 0.4,
            "outlet_dn": "DN50",
            "evidence": [("ACO", "component", url)],
        }

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
        try:
            wb.save(path)
        finally:
            wb.close()

    def _sheet_rows(self, path: Path, sheet: str):
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            ws = wb[sheet]
            return list(ws.iter_rows(values_only=True))
        finally:
            wb.close()

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


    def test_export_writes_article_variants_sheet_from_normalized_easyflow_diagnostics(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)

            registry = pd.DataFrame([{"manufacturer": "aco", "product_id": "aco-easyflow-base", "product_name": "ACO Easyflow"}])
            products = pd.DataFrame([{"manufacturer": "aco", "product_id": "aco-assembled-easyflow-compact", "product_name": "ACO Easyflow assembled"}])
            variant_rows = pd.DataFrame([
                {
                    "manufacturer": "aco",
                    "base_product_id": "aco-assembled-easyflow-compact",
                    "article_number": "2500.55.00",
                    "variant_type": "candidate_body_variant",
                    "product_family": "easyflow",
                    "source_url": "https://example.test/easyflow/komplettablaeufe-aco-easyflow-dn-50/",
                    "water_seal_mm": 50,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 1.5,
                    "height_adj_min_mm": 15,
                    "height_adj_max_mm": 96,
                    "cutout_mm": "150 x 150 mm",
                    "side_inlet": "false",
                    "row_text": "2500.55.00 WS50 DN50 1,5 l/s",
                    "attribution_status": "candidate_variant",
                    "why_not_promoted": "multiple_candidate_articles",
                },
                {
                    "manufacturer": "aco",
                    "base_product_id": "aco-assembled-easyflow-compact",
                    "article_number": "2500.05.00",
                    "variant_type": "candidate_body_variant",
                    "product_family": "easyflow",
                    "source_url": "https://example.test/easyflow/einzelablaeufe-aco-easyflow-dn-50/",
                    "water_seal_mm": 50,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 1.0,
                    "height_adj_min_mm": 7,
                    "height_adj_max_mm": 75,
                    "cutout_mm": "160 x 160 mm",
                    "side_inlet": "true",
                    "row_text": "2500.05.00 WS50 DN50 1,0 l/s",
                    "attribution_status": "candidate_variant",
                    "why_not_promoted": "multiple_candidate_articles",
                },
                {
                    "manufacturer": "aco",
                    "base_product_id": "aco-assembled-easyflow-compact",
                    "article_number": "2500.00.00",
                    "variant_type": "candidate_body_variant",
                    "product_family": "easyflow",
                    "source_url": "https://example.test/easyflow/einzelablaeufe-aco-easyflow-dn-50/",
                    "water_seal_mm": 50,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 1.0,
                    "height_adj_min_mm": 7,
                    "height_adj_max_mm": 75,
                    "cutout_mm": "160 x 160 mm",
                    "side_inlet": "true",
                    "row_text": "2500.00.00 WS50 DN50 1,0 l/s",
                    "attribution_status": "candidate_variant",
                    "why_not_promoted": "multiple_candidate_articles",
                },
            ])

            with patch("tools.report_easyflow_article_variants.build_article_variants_dataframe", return_value=variant_rows) as helper:
                export_excel(template, out, default_config(), registry_df=registry, products_df=products, comparison_df=products)

            helper.assert_called_once()
            rows = self._sheet_rows(out, "Article_Variants")
            self.assertEqual(rows[0], tuple(variant_rows.columns))
            article_df = pd.DataFrame(rows[1:], columns=rows[0])
            self.assertEqual(len(article_df), 3)
            self.assertEqual(set(article_df["article_number"]), {"2500.55.00", "2500.05.00", "2500.00.00"})
            self.assertEqual(set(article_df["flow_rate_lps"]), {1.5, 1.0})
            self.assertTrue((article_df["attribution_status"] == "candidate_variant").all())
            self.assertTrue((article_df["why_not_promoted"] == "multiple_candidate_articles").all())
            product_ids = {row[1] for row in self._sheet_rows(out, "Products")[1:]}
            self.assertFalse(set(article_df["article_number"]) & product_ids)



    def test_export_adds_mplus_compound_mappings_after_final_set_details(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)

            class EmptyMappingReport:
                proposed_mappings = ()
                article_level_drain_bodies = ()
                safe_to_generate_counts = {"safe": 0, "blocked": 4}
                missing_technical_field_summary = {"flow_rate_lps": 4}

            class FlowCandidate:
                def __init__(self, flow_rate_lps, head_mm):
                    self.flow_rate_lps = flow_rate_lps
                    self.head_mm = head_mm
                    self.evidence_type = "explicit_drain_body_family_level"
                    self.confidence = "medium"
                    self.article_specific = False
                    self.flow_attribution_scope = "drain_body_family_level"

            class AccessoryReduction:
                accessory_flow_reduction_lps = "0.1"

            class FlowRiskChecks:
                accessory_reduction_treated_as_flow_candidate = ()
                multiple_head_condition_flow_values_found = True
                article_level_flow_table_values_found = False

            class ArticleAssessment:
                article_specific = False

            class FlowDiagnostic:
                target_articles = ("9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23")
                drain_body_flow_candidates = (FlowCandidate("0.4", "10"), FlowCandidate("0.46", "20"))
                accessory_flow_reduction_lps = (AccessoryReduction(),)
                risk_checks = FlowRiskChecks()
                per_article = (ArticleAssessment(),)

            products = pd.DataFrame([{"manufacturer": "aco", "product_id": "aco-regular-product"}])
            with (
                patch("tools.report_mplus_compound_assembly_mapping.build_report", return_value=EmptyMappingReport()),
                patch("tools.diagnose_mplus_flow_rate_sources.build_diagnostic", return_value=FlowDiagnostic()),
            ):
                export_excel(template, out, default_config(), products_df=products, comparison_df=products)

            wb = openpyxl.load_workbook(out, read_only=True, data_only=True)
            try:
                self.assertIn("Mplus_Compound_Mappings", wb.sheetnames)
                self.assertLess(wb.sheetnames.index("Final_Set_Details"), wb.sheetnames.index("Mplus_Compound_Mappings"))
                self.assertLess(wb.sheetnames.index("Mplus_Compound_Mappings"), wb.sheetnames.index("Eplus_Proposal_Mappings"))
                self.assertIn("Conditional_Technical_Values", wb.sheetnames)
                self.assertIn("Scoring_Scenarios", wb.sheetnames)
                self.assertIn("Comparison_flow_head_10mm", wb.sheetnames)
                self.assertIn("Comparison_flow_head_20mm", wb.sheetnames)
                self.assertLess(wb.sheetnames.index("Eplus_Proposal_Mappings"), wb.sheetnames.index("Conditional_Technical_Values"))
                self.assertLess(wb.sheetnames.index("Conditional_Technical_Values"), wb.sheetnames.index("Components"))
                self.assertLess(wb.sheetnames.index("Conditional_Technical_Values"), wb.sheetnames.index("Article_Variants"))
            finally:
                wb.close()

            rows = self._sheet_rows(out, "Mplus_Compound_Mappings")
            df = pd.DataFrame(rows[1:], columns=rows[0])
            self.assertEqual(len(df), 4)
            self.assertEqual(set(df["drain_body_article_number"]), {"9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23"})
            self.assertTrue(df["flow_rate_lps"].isna().all())
            self.assertTrue(df["selected_default_flow_rate_lps"].isna().all())
            self.assertEqual(set(df["flow_rate_lps_10mm_head"]), {"0.4"})
            self.assertEqual(set(df["flow_rate_lps_20mm_head"]), {"0.46"})
            self.assertEqual(set(df["safe_to_generate"]), {False})
            conditional_rows = self._sheet_rows(out, "Conditional_Technical_Values")
            conditional_df = pd.DataFrame(conditional_rows[1:], columns=conditional_rows[0])
            self.assertEqual(len(conditional_df), 8)
            self.assertEqual(set(conditional_df["parameter_name"]), {"flow_rate_lps"})
            self.assertEqual(set(conditional_df["condition_value"]), {10, 20})
            self.assertEqual(set(conditional_df["value"]), {0.4, 0.46})
            self.assertEqual(conditional_df.groupby("set_id").size().to_dict(), {set_id: 2 for set_id in df["set_id"]})
            self.assertTrue(conditional_df["production_status_note"].str.contains("conditional flow values available in Conditional_Technical_Values").all())
            scenarios = pd.DataFrame(
                self._sheet_rows(out, "Scoring_Scenarios")[1:],
                columns=self._sheet_rows(out, "Scoring_Scenarios")[0],
            )
            self.assertEqual(set(scenarios["scenario_id"]), {"no_scenario_selected", "flow_head_10mm", "flow_head_20mm"})
            self.assertEqual(scenarios.loc[scenarios["is_default"] == True, "scenario_id"].tolist(), ["no_scenario_selected"])
            for sheet_name, expected_flow in [("Comparison_flow_head_10mm", 0.40), ("Comparison_flow_head_20mm", 0.46)]:
                scenario_rows = self._sheet_rows(out, sheet_name)
                scenario_df = pd.DataFrame(scenario_rows[1:], columns=scenario_rows[0])
                mplus_scenario = scenario_df[scenario_df["product_family"] == "showerdrain_mplus"]
                self.assertEqual(len(mplus_scenario), 4)
                self.assertTrue(pd.to_numeric(mplus_scenario["flow_rate_lps"]).round(2).eq(expected_flow).all())
                self.assertTrue(mplus_scenario["scenario_ready_for_benchmark"].eq(True).all())
                self.assertTrue(mplus_scenario["flow_rate_resolution_source"].eq("Conditional_Technical_Values").all())
            eplus_rows = self._sheet_rows(out, "Eplus_Proposal_Mappings")
            eplus_df = pd.DataFrame(eplus_rows[1:], columns=eplus_rows[0])
            self.assertEqual(len(eplus_df), 3)
            self.assertEqual(set(eplus_df["body_id"]), {
                "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm",
                "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm",
                "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1",
            })
            self.assertEqual(set(eplus_df["grate_id"]), {"aco-showerdrain-eplus-design-roste-aus-elektropoliertem-edelstahl"})
            self.assertEqual(set(eplus_df["flow_rate_lps"]), {0.7})
            self.assertEqual(set(eplus_df["water_seal_mm"]), {50})
            self.assertEqual(set(eplus_df["outlet_dn"]), {"DN50"})
            self.assertEqual(set(eplus_df["height_adj_min_mm"]), {25, 57, 80})
            self.assertEqual(set(eplus_df["height_adj_max_mm"]), {128})
            self.assertEqual(set(eplus_df["safe_to_generate"]), {False})
            self.assertEqual(set(eplus_df["ready_for_benchmark"]), {False})
            self.assertEqual(set(eplus_df["ready_for_customer_view"]), {False})
            self.assertEqual(len(self._sheet_rows(out, "Products")) - 1, 5)
            self.assertEqual(len(self._sheet_rows(out, "Final_Assemblies")) - 1, 4)
            self.assertEqual(len(self._sheet_rows(out, "Final_Set_Details")) - 1, 4)

    def test_article_variant_products_flag_defaults_to_disabled(self):
        registry = pd.DataFrame([
            {
                "manufacturer": "aco",
                "product_id": "aco-assembled-easyflow-compact",
                "product_name": "ACO Easyflow base",
                "product_url": "https://example.test/easyflow/komplettablaeufe-aco-easyflow-dn-50/",
                "candidate_type": "drain",
                "system_role": "complete_system",
                "product_family": "easyflow",
            }
        ])
        variants = pd.DataFrame([
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "2500.55.00",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/komplettablaeufe-aco-easyflow-dn-50/",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": 1.5,
                "height_adj_min_mm": 15,
                "height_adj_max_mm": 96,
                "attribution_status": "candidate_variant",
            }
        ])

        with patch.dict(pipeline.CONNECTORS, {"aco": _FakeAcoConnector()}, clear=True), \
             patch("tools.report_easyflow_article_variants.build_article_variants_dataframe", return_value=variants) as helper:
            products, comparison, _excluded, _evidence, bom = pipeline.run_update(registry, default_config())

        helper.assert_not_called()
        article_ids = {"aco-easyflow-article-25005500"}
        self.assertFalse(article_ids & set(products["product_id"].astype(str)))
        self.assertFalse(article_ids & set(comparison["product_id"].astype(str)))
        self.assertEqual(len(products), 1)
        self.assertEqual(len(comparison), 1)
        self.assertTrue(bom.empty)

    def test_article_variant_products_enabled_promotes_only_expected_easyflow_articles(self):
        registry = pd.DataFrame([
            {
                "manufacturer": "aco",
                "product_id": "aco-assembled-easyflow-compact",
                "product_name": "ACO Easyflow base",
                "product_url": "https://example.test/easyflow/komplettablaeufe-aco-easyflow-dn-50/",
                "candidate_type": "drain",
                "system_role": "complete_system",
                "product_family": "easyflow",
            }
        ])
        variants = pd.DataFrame([
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "2500.55.00",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/komplettablaeufe-aco-easyflow-dn-50/",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": 1.5,
                "height_adj_min_mm": 15,
                "height_adj_max_mm": 96,
                "attribution_status": "candidate_variant",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "2500.05.00",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/einzelablaeufe-aco-easyflow-dn-50/",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": 1.0,
                "height_adj_min_mm": 7,
                "height_adj_max_mm": 75,
                "attribution_status": "candidate_variant",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "2500.00.00",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/einzelablaeufe-aco-easyflow-dn-50/",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": 1.0,
                "height_adj_min_mm": 7,
                "height_adj_max_mm": 75,
                "attribution_status": "candidate_variant",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "9010.88.00",
                "variant_type": "excluded_grate_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/grate/",
                "attribution_status": "excluded_not_body_variant",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "9999.99.99",
                "variant_type": "excluded_accessory_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/accessory/",
                "attribution_status": "excluded_not_body_variant",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "8888.88.88",
                "variant_type": "excluded_easyflowplus",
                "product_family": "easyflowplus",
                "source_url": "https://example.test/easyflowplus/",
                "attribution_status": "excluded_not_body_variant",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-compact",
                "article_number": "7777.77.77",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "",
                "attribution_status": "candidate_variant",
            },
        ])
        cfg = default_config()
        cfg.enable_article_variant_products = True

        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always", FutureWarning)
            with patch.dict(pipeline.CONNECTORS, {"aco": _FakeAcoConnector()}, clear=True), \
                 patch("tools.report_easyflow_article_variants.build_article_variants_dataframe", return_value=variants) as helper:
                products, comparison, _excluded, _evidence, bom = pipeline.run_update(registry, cfg)

        helper.assert_called_once()
        future_warnings = [warning for warning in caught_warnings if issubclass(warning.category, FutureWarning)]
        self.assertEqual(future_warnings, [])
        expected_ids = {
            "aco-easyflow-article-25005500",
            "aco-easyflow-article-25000500",
            "aco-easyflow-article-25000000",
        }
        rejected_ids = {
            "aco-easyflow-article-90108800",
            "aco-easyflow-article-99999999",
            "aco-easyflow-article-88888888",
            "aco-easyflow-article-77777777",
        }
        product_ids = set(products["product_id"].astype(str))
        comparison_ids = set(comparison["product_id"].astype(str))
        self.assertTrue(expected_ids <= product_ids)
        self.assertTrue(expected_ids <= comparison_ids)
        self.assertFalse(rejected_ids & product_ids)
        self.assertFalse(rejected_ids & comparison_ids)
        self.assertIn("aco-assembled-easyflow-compact", product_ids)
        article_rows = products[products["product_id"].astype(str).isin(expected_ids)].copy()
        self.assertEqual(set(article_rows["candidate_type"]), {"article_variant"})
        self.assertEqual(set(article_rows["product_family"]), {"easyflow_article_variant"})
        self.assertEqual(set(article_rows["classification_reason"]), {"source_backed_article_variant"})
        self.assertEqual(set(article_rows["base_product_id"]), {"aco-assembled-easyflow-compact"})
        self.assertEqual(set(article_rows["article_number"]), {"2500.55.00", "2500.05.00", "2500.00.00"})
        self.assertEqual(set(article_rows["outlet_dn"]), {"DN50"})
        self.assertFalse(article_rows["source_url"].astype(str).str.strip().eq("").any())
        self.assertTrue(bom.empty)

    def test_export_writes_source_checks_sheet_from_evidence(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)
            evidence_df = pd.DataFrame([{
                "manufacturer": "kaldewei",
                "product_id": "__source_check__",
                "label": "source_check:kaldewei-flow-page",
                "snippet": '{"manufacturer":"kaldewei","source_id":"kaldewei-flow-page","family":"flow","source_url":"u","source_type":"product_page","status_code":200,"final_url":"u","content_hash_sha256":"h","content_length":10,"baseline_hash_sha256":"h","baseline_content_length":10,"hash_changed":false,"length_changed":false,"expected_terms_found":"FLOWDRAIN","expected_terms_missing":"","new_source_candidate_count":0,"sample_new_source_candidates":"","review_required":"no","review_reason":"","checked_at":"2026-01-01T00:00:00+00:00","extraction_mode":"html_text","fetch_error":""}',
                "source": "u",
            }])
            export_excel(template, out, default_config(), evidence_df=evidence_df)
            rows = self._sheet_rows(out, "Source_Checks")
            self.assertEqual(rows[0][0:4], ("manufacturer", "source_id", "family", "source_url"))
            self.assertEqual(rows[1][0:3], ("kaldewei", "kaldewei-flow-page", "flow"))

    def test_export_writes_scoring_field_coverage_sheet(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)
            products = pd.DataFrame([{
                "manufacturer": "kaldewei", "product_id": "kaldewei-flowdrain-horizontal-regular",
                "product_name": "FLOWDRAIN regular", "candidate_type": "component", "complete_system": "component",
                "flow_rate_lps": 0.8, "height_adj_min_mm": 78, "height_adj_max_mm": 179
            }])
            comparison = pd.DataFrame([{
                "manufacturer": "kaldewei", "product_id": "kaldewei-assembled-flowline-zero__flowdrain-horizontal-regular",
                "product_name": "assembled", "candidate_type": "drain", "complete_system": "yes",
                "flow_rate_lps": 0.8
            }])
            export_excel(template, out, default_config(), products_df=products, comparison_df=comparison)
            rows = self._sheet_rows(out, "Scoring_Field_Coverage")
            self.assertEqual(rows[0][0], "manufacturer")
            self.assertTrue(any(r[1] == "kaldewei-assembled-flowline-zero__flowdrain-horizontal-regular" for r in rows[1:]))
            cov = pd.DataFrame(rows[1:], columns=rows[0])
            merged_row = cov[cov["product_id"] == "kaldewei-assembled-flowline-zero__flowdrain-horizontal-regular"].iloc[0]
            self.assertEqual(bool(merged_row["in_comparison"]), True)

    def test_export_materializes_components_from_excluded_when_products_have_none(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)
            products = pd.DataFrame([{"manufacturer": "aco", "product_id": "aco-drain-1", "candidate_type": "drain", "system_role": "drain_unit"}])
            comparison = products.copy()
            excluded = pd.DataFrame([
                {"manufacturer": "aco", "product_id": "aco-comp-grate", "candidate_type": "component", "system_role": "grate", "why_not_product_reason": "cover_only_component"},
                {"manufacturer": "aco", "product_id": "aco-comp-accessory", "candidate_type": "component", "system_role": "accessory", "why_not_product_reason": "accessory_only"},
            ])
            bom = pd.DataFrame([
                {"manufacturer": "aco", "product_id": "aco-drain-1", "component_id": "aco-comp-grate", "option_type": "compatible_grate", "option_role": "grate", "option_meta": "compatibility_confidence=implicit_family_level; explicit_article_matrix=false; source_limitation=grate compatibility is family-level and length/design based; no explicit article-to-article matrix found."},
                {"manufacturer": "aco", "product_id": "aco-drain-1", "component_id": "aco-comp-accessory", "option_type": "optional_accessory", "option_role": "accessory", "option_meta": "x"},
            ])
            export_excel(template, out, default_config(), products_df=products, comparison_df=comparison, excluded_df=excluded, bom_options_df=bom)
            comp_rows = self._sheet_rows(out, "Components")
            self.assertGreater(len(comp_rows), 1)
            comp = pd.DataFrame(comp_rows[1:], columns=comp_rows[0])
            self.assertTrue((comp["manufacturer"].astype(str).str.lower() == "aco").any())
            self.assertTrue(comp["candidate_type"].astype(str).str.lower().isin(["component", "base_set"]).any())
            self.assertTrue(comp["system_role"].astype(str).str.lower().isin(["grate", "accessory", "optional_accessory"]).any())
            prod = pd.DataFrame(self._sheet_rows(out, "Products")[1:], columns=self._sheet_rows(out, "Products")[0])
            cmp = pd.DataFrame(self._sheet_rows(out, "Comparison")[1:], columns=self._sheet_rows(out, "Comparison")[0])
            self.assertFalse(prod["system_role"].astype(str).str.lower().isin(["grate", "accessory", "optional_accessory"]).any())
            self.assertFalse(cmp["system_role"].astype(str).str.lower().isin(["grate", "accessory", "optional_accessory"]).any())

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

    def test_run_discovery_respects_selected_connectors(self):
        with patch.dict(pipeline.CONNECTORS, {"dallmer": _FakeDiscoverA(), "hansgrohe": _FakeDiscoverB()}, clear=True):
            reg, dbg = pipeline.run_discovery(selected_connectors=["hansgrohe"])
        self.assertEqual(reg["manufacturer"].tolist(), ["hansgrohe"])
        self.assertTrue(dbg.empty)

    def test_run_update_respects_selected_connectors(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "dallmer", "product_id": "D1", "product_name": "D", "product_url": "https://d.example/p", "candidate_type": "drain", "complete_system": "yes"},
                {"manufacturer": "hansgrohe", "product_id": "H1", "product_name": "H", "product_url": "https://h.example/p", "candidate_type": "drain", "complete_system": "yes"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"dallmer": _FakeConnectorYes(), "hansgrohe": _FakeConnectorYes()}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config(), selected_connectors=["hansgrohe"])
        self.assertEqual(products["manufacturer"].tolist(), ["hansgrohe"])
        self.assertEqual(comparison["manufacturer"].tolist(), ["hansgrohe"])
        self.assertTrue(excluded.empty)
        self.assertTrue((evidence["manufacturer"] == "hansgrohe").all())
        self.assertTrue(bom.empty)


    def test_kaldewei_comparison_eligibility_and_assembled_scores(self):
        rows, _ = kaldewei.discover_candidates()
        registry = pd.DataFrame(rows)
        with patch.dict(pipeline.CONNECTORS, {"kaldewei": kaldewei}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        kcomp = comparison[comparison["manufacturer"] == "kaldewei"]
        self.assertFalse(any("finish" in str(x) for x in kcomp["product_id"]))
        self.assertNotIn("kaldewei-flowdrain-horizontal-regular", set(kcomp["product_id"]))
        self.assertNotIn("kaldewei-flowdrain-horizontal-flat", set(kcomp["product_id"]))
        assembled = products[(products["manufacturer"] == "kaldewei") & (products["product_id"].astype(str).str.startswith("kaldewei-assembled-"))]
        self.assertEqual(len(assembled), 6)
        for col in ["param_score", "equiv_score", "system_score", "final_score"]:
            self.assertTrue(assembled[col].notna().all())
        reg = assembled[assembled["product_id"].astype(str).str.contains("regular")].iloc[0]
        flat = assembled[assembled["product_id"].astype(str).str.contains("flat")].iloc[0]
        self.assertEqual(float(reg.get("flow_rate_lps")), 0.8)
        self.assertEqual(float(flat.get("flow_rate_lps")), 0.63)
        self.assertGreater(float(reg.get("param_score")), float(flat.get("param_score")))
        self.assertEqual(float(reg.get("flow_rate_score")), 1.0)
        self.assertEqual(str(reg.get("flow_rate_pass_0_8_lps")), "yes")
        self.assertEqual(float(reg.get("height_adjustability_range_mm")), 101.0)
        self.assertEqual(float(reg.get("height_adjustability_score")), 1.0)
        self.assertAlmostEqual(float(reg.get("final_score")), 0.4117647058823529, 12)
        self.assertAlmostEqual(float(reg.get("final_score_pct")), float(reg.get("final_score")) * 100.0, 10)
        self.assertAlmostEqual(float(flat.get("flow_rate_score")), 0.7875, 4)
        self.assertEqual(str(flat.get("flow_rate_pass_0_8_lps")), "no")
        self.assertEqual(float(flat.get("height_adjustability_range_mm")), 20.0)
        self.assertEqual(float(flat.get("height_adjustability_score")), 0.2)
        self.assertAlmostEqual(float(flat.get("final_score")), 0.2551470588235294, 12)
        self.assertAlmostEqual(float(flat.get("final_score_pct")), float(flat.get("final_score")) * 100.0, 10)
        reg_cmp = comparison[comparison["product_id"] == reg["product_id"]].iloc[0]
        flat_cmp = comparison[comparison["product_id"] == flat["product_id"]].iloc[0]
        for col in ["final_score", "final_score_pct", "flow_rate_score", "flow_rate_pass_0_8_lps", "height_adjustability_range_mm", "height_adjustability_score"]:
            self.assertEqual(str(reg_cmp.get(col)), str(reg.get(col)))
            self.assertEqual(str(flat_cmp.get(col)), str(flat.get(col)))
        kprod = products[products["manufacturer"] == "kaldewei"]
        na_ids = ["kaldewei-flowpoint-zero", "kaldewei-flowdrain-horizontal-regular", "kaldewei-flowdrain-horizontal-flat", "kaldewei-ka-90-horizontal", "kaldewei-ka-120-horizontal", "kaldewei-ka-300-horizontal", "kaldewei-xetis-ka-200"]
        for pid in na_ids:
            row = kprod[kprod["product_id"] == pid].iloc[0]
            self.assertIn(str(row.get("selected_length_mm")), {"not_applicable", "", "nan", "None"})
        self.assertEqual(str(kprod[kprod["product_id"] == "kaldewei-nexsys"].iloc[0].get("complete_system")), "yes")
        self.assertTrue(str(kprod[kprod["product_id"] == "kaldewei-xetis-ka-200"].iloc[0].get("complete_system")) in {"configuration", "yes"})
        kev = evidence[evidence["manufacturer"] == "kaldewei"]
        self.assertFalse(any(str(x) == "0.0" for x in kev["snippet"]))
        tech_fields = ["flow_rate_lps", "water_seal_mm", "height_adj_min_mm", "height_adj_max_mm"]
        kprod_rows = products[products["manufacturer"] == "kaldewei"]
        for _, row in kprod_rows.iterrows():
            has_tech = any(str(row.get(f)) not in {"", "None", "nan"} for f in tech_fields)
            if has_tech:
                self.assertTrue(str(row.get("source_url") or row.get("sources") or "").strip() != "")
        assembled_rows = kprod_rows[kprod_rows["product_id"].astype(str).str.startswith("kaldewei-assembled-")]
        self.assertTrue((assembled_rows["sources"].astype(str).str.contains(",") | assembled_rows["sources"].astype(str).str.contains("kaldewei.com")).all())
        flow_ev = kev[(kev["field_name"] == "flow_rate_lps") & (kev["product_id"].isin(["kaldewei-flowdrain-horizontal-regular", "kaldewei-flowdrain-horizontal-flat"]))]
        self.assertTrue(any(ev == "0.8" for ev in flow_ev["extracted_value"].astype(str)))
        self.assertTrue(set(comparison["product_id"].astype(str)).issubset(set(products["product_id"].astype(str))))
        comp_ids = set(products[products["candidate_type"].astype(str).str.lower().isin(["component", "base_set"])]["product_id"].astype(str))
        self.assertTrue(set(comparison["product_id"].astype(str)).isdisjoint(comp_ids))

    def test_comparison_subset_products_and_scoring_regression(self):
        rows, _ = kaldewei.discover_candidates()
        registry = pd.DataFrame(rows)
        with patch.dict(pipeline.CONNECTORS, {"kaldewei": kaldewei}, clear=True):
            products, comparison, _excluded, _evidence, _bom = pipeline.run_update(registry, default_config())

        self.assertTrue(set(comparison["product_id"].astype(str)).issubset(set(products["product_id"].astype(str))))
        component_ids = set(products[products["candidate_type"].astype(str).str.lower().isin({"component", "base_set"})]["product_id"].astype(str))
        self.assertTrue(set(comparison["product_id"].astype(str)).isdisjoint(component_ids))
        self.assertFalse(any(pid in set(comparison["product_id"]) for pid in ["kaldewei-ka-90-horizontal", "kaldewei-ka-120-horizontal"]))
        self.assertTrue(any("kaldewei-assembled-flowline-zero" in str(pid) for pid in comparison["product_id"]))
        self.assertTrue(any("kaldewei-assembled-flowpoint-zero" in str(pid) for pid in comparison["product_id"]))
        self.assertFalse(any("finish" in str(pid) for pid in comparison["product_id"]))

        reg = products[products["product_id"] == "kaldewei-flowdrain-horizontal-regular"].iloc[0]
        flat = products[products["product_id"] == "kaldewei-flowdrain-horizontal-flat"].iloc[0]
        self.assertEqual(float(reg["flow_rate_score"]), 1.0)
        self.assertEqual(str(reg["flow_rate_pass_0_8_lps"]), "yes")
        self.assertEqual(float(reg["height_adjustability_range_mm"]), 101.0)
        self.assertEqual(float(reg["height_adjustability_score"]), 1.0)
        self.assertAlmostEqual(float(flat["flow_rate_score"]), 0.7875, 4)
        self.assertEqual(str(flat["flow_rate_pass_0_8_lps"]), "no")
        self.assertEqual(float(flat["height_adjustability_range_mm"]), 20.0)
        self.assertEqual(float(flat["height_adjustability_score"]), 0.2)
        self.assertAlmostEqual(float(reg["final_score_pct"]), float(reg["final_score"]) * 100.0, 6)

    def test_scoring_field_coverage_respects_cleaned_comparison_membership(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)
            rows, _ = kaldewei.discover_candidates()
            registry = pd.DataFrame(rows)
            with patch.dict(pipeline.CONNECTORS, {"kaldewei": kaldewei}, clear=True):
                products, comparison, _excluded, _evidence, _bom = pipeline.run_update(registry, default_config())
            export_excel(template, out, default_config(), products_df=products, comparison_df=comparison)
            cov = pd.DataFrame(self._sheet_rows(out, "Scoring_Field_Coverage")[1:], columns=self._sheet_rows(out, "Scoring_Field_Coverage")[0])
            comp_ids = set(comparison["product_id"].astype(str))
            self.assertTrue((cov[cov["in_comparison"] == True]["product_id"].astype(str).isin(comp_ids)).all())
            self.assertFalse(((cov["candidate_type"].astype(str).str.lower().isin(["component", "base_set"])) & (cov["in_comparison"] == True)).any())

    def test_kaldewei_ka90_variants_evidence_and_ka120_regression(self):
        rows, _ = kaldewei.discover_candidates()
        registry = pd.DataFrame(rows)
        with patch.dict(pipeline.CONNECTORS, {"kaldewei": kaldewei}, clear=True):
            products, comparison, _excluded, evidence, _bom = pipeline.run_update(registry, default_config())

        krows = products[products["manufacturer"] == "kaldewei"]
        ka90 = krows[krows["family"] == "ka_90"].copy()
        self.assertEqual(len(ka90), 3)
        self.assertTrue((ka90["candidate_type"] == "component").all())
        self.assertTrue((ka90["product_category"] == "tray_waste_fitting").all())
        self.assertTrue((ka90["system_role"] == "tray_waste_fitting").all())
        self.assertTrue((ka90["complete_system"] == "component").all())
        if "selected_length_mm" in ka90.columns:
            self.assertTrue((ka90["selected_length_mm"].astype(str).isin({"", "not_applicable", "nan"})).all())
        if "benchmark_eligible" in ka90.columns:
            self.assertTrue((ka90["benchmark_eligible"].astype(str).str.lower().isin({"false", "0"})).all())
        self.assertFalse(comparison["product_id"].isin(ka90["product_id"]).any())

        by_model = {str(r.model_number): r for _, r in ka90.iterrows()}
        self.assertEqual(str(by_model["4103"].outlet_dn), "DN50")
        self.assertEqual(float(by_model["4103"].flow_rate_lps), 0.71)
        self.assertEqual(str(by_model["4104"].outlet_dn), "DN40")
        self.assertEqual(float(by_model["4104"].flow_rate_lps), 0.68)
        self.assertEqual(str(by_model["4105"].outlet_dn), "DN50")
        self.assertEqual(float(by_model["4105"].flow_rate_lps), 1.22)

        self.assertEqual(
            {str(x) for x in ka90["article_number"].astype(str)},
            {"687772560999", "687772540999", "687772550999"},
        )
        self.assertEqual({int(float(x)) for x in ka90["water_seal_mm"]}, {30, 50})
        self.assertEqual({int(float(x)) for x in ka90["construction_height_mm"]}, {60, 80})

        kev = evidence[(evidence["manufacturer"] == "kaldewei") & (evidence["product_id"].isin(ka90["product_id"]))]
        for fld in ("model_number", "flow_rate_lps", "outlet_dn", "dn"):
            fld_rows = kev[kev["field_name"] == fld]
            self.assertFalse(fld_rows.empty)
            self.assertTrue(fld_rows["source_url"].astype(str).str.len().gt(0).all() | fld_rows["source_label"].astype(str).str.len().gt(0).all())
            self.assertTrue(fld_rows["evidence_type"].astype(str).str.len().gt(0).all())
            self.assertFalse(fld_rows["extracted_value"].astype(str).str.lower().isin({"", "nan", "none", "null", "not_applicable"}).any())
        ka90_note_rows = evidence[evidence["source_note"].astype(str).str.contains("KA90 value seeded from official Kaldewei KA90 technical source", na=False)]
        self.assertTrue((ka90_note_rows["product_id"].astype(str).str.contains("kaldewei-ka-90-")).all())

        ka120 = krows[krows["family"] == "ka_120"]
        self.assertEqual(set(ka120["model_number"].astype(str)), {"4106", "4107", "4108"})
        self.assertEqual(set(ka120["article_number"].astype(str)), {"687772530000", "687772510000", "687772520000"})
        self.assertEqual(set(ka120["outlet_dn"].astype(str)), {"DN50", "DN40"})
        self.assertEqual(set(round(float(x), 2) for x in ka120["flow_rate_lps"]), {0.85, 1.4})
        self.assertEqual(set(int(float(x)) for x in ka120["water_seal_mm"]), {30, 50})
        self.assertEqual(set(int(float(x)) for x in ka120["construction_height_mm"]), {63, 83})

    def test_scoring_coverage_flowdrain_and_construction_height_rule(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)
            products = pd.DataFrame([
                {"manufacturer":"test","product_id":"p-adjust","product_name":"Adjustable","candidate_type":"drain","complete_system":"yes","flow_rate_lps":0.8,"height_adj_min_mm":78,"height_adj_max_mm":179},
                {"manufacturer":"test","product_id":"p-fixed","product_name":"Fixed","candidate_type":"drain","complete_system":"yes","flow_rate_lps":0.7,"construction_height_mm":80},
            ])
            comparison = products.copy()
            export_excel(template, out, default_config(), products_df=products, comparison_df=comparison)
            cov = pd.DataFrame(self._sheet_rows(out, "Scoring_Field_Coverage")[1:], columns=self._sheet_rows(out, "Scoring_Field_Coverage")[0])
        reg = cov[cov["product_id"] == "p-adjust"].iloc[0]
        flat = cov[cov["product_id"] == "p-fixed"].iloc[0]
        self.assertEqual(bool(reg["has_flow_rate_lps"]), True)
        self.assertEqual(bool(reg["has_height_adjustability_data"]), True)
        self.assertEqual(bool(flat["has_flow_rate_lps"]), True)
        self.assertEqual(bool(flat["has_height_adjustability_data"]), False)
        rows, _ = kaldewei.discover_candidates()
        registry = pd.DataFrame(rows)
        with patch.dict(pipeline.CONNECTORS, {"kaldewei": kaldewei}, clear=True):
            _products, comparison, _excluded, _evidence, _bom = pipeline.run_update(registry, default_config())
        self.assertFalse(any(x in set(comparison["product_id"]) for x in ["kaldewei-ka-90-horizontal", "kaldewei-ka-120-horizontal"]))
    def test_aco_role_based_promotion_splits_products_and_components(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "aco", "product_id": "aco-90108544", "product_name": "ACO ShowerDrain C 1200 mm (Artikel-Nr. 90108544)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108544/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108554", "product_name": "ACO ShowerDrain C 1200 mm (Artikel-Nr. 90108554)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108554/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108524", "product_name": "ACO ShowerDrain C 1000 mm (Artikel-Nr. 90108524)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108524/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108534", "product_name": "ACO ShowerDrain C 1000 mm (Artikel-Nr. 90108534)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108534/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-comp-showerpoint", "product_name": "ACO ShowerPoint", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-showerpoint/", "candidate_type": "component", "complete_system": "component", "system_role": "complete_system"},
                {"manufacturer": "aco", "product_id": "aco-comp-passino", "product_name": "ACO Renovierungsablauf Passino", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-renovierungsablauf-passino/", "candidate_type": "component", "complete_system": "component", "system_role": "complete_system"},
                {"manufacturer": "aco", "product_id": "aco-comp-family", "product_name": "ACO ShowerDrain S+", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/", "candidate_type": "component", "complete_system": "component", "system_role": "configuration_family"},
                {"manufacturer": "aco", "product_id": "aco-comp-grate", "product_name": "ACO ShowerDrain C Designrost", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/designrost/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
                {"manufacturer": "aco", "product_id": "aco-comp-accessory", "product_name": "ACO ShowerStep", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/zubehoer/aco-showerstep/", "candidate_type": "component", "complete_system": "component", "system_role": "accessory"},
                {"manufacturer": "aco", "product_id": "aco-fp-public-designrost", "product_name": "Design-Roste zur ShowerDrain Public 80/110/X", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/reihenduschrinnen/aco-showerdrain-public-80/designrost/", "candidate_type": "component", "complete_system": "component", "system_role": "complete_system"},
                {"manufacturer": "aco", "product_id": "aco-fp-showerstep-keil", "product_name": "ACO ShowerStep – Gefällekeil", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/zubehoer/showerstep-gefaellekeil/", "candidate_type": "component", "complete_system": "component", "system_role": "complete_system"},
                {"manufacturer": "aco", "product_id": "aco-fp-aufsatz", "product_name": "Aufsatzstücke", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aufsatzstuecke/", "candidate_type": "component", "complete_system": "component", "system_role": "complete_system"},
                {"manufacturer": "aco", "product_id": "aco-fp-ablaufkoerper", "product_name": "Ablaufkörper Variant CR", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/ablaufkoerper-variant-cr/", "candidate_type": "component", "complete_system": "component", "system_role": "complete_system"},
                {"manufacturer": "aco", "product_id": "aco-fp-config-family", "product_name": "ACO ShowerDrain Public 80 Komplettablauf Familie", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/reihenduschrinnen/aco-showerdrain-public-80/", "candidate_type": "component", "complete_system": "component", "system_role": "configuration_family"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"aco": _FakeAcoConnector()}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

        drains = products[products["candidate_type"] == "drain"]
        components = products[products["candidate_type"] == "component"]
        excluded_ids = set(excluded["product_id"].astype(str).tolist())
        self.assertTrue({"aco-90108544", "aco-90108554", "aco-90108524", "aco-90108534"}.issubset(set(drains["product_id"].tolist())))
        self.assertIn("aco-comp-showerpoint", set(drains["product_id"].tolist()))
        self.assertIn("aco-comp-passino", set(drains["product_id"].tolist()))
        self.assertNotIn("aco-comp-family", set(components["product_id"].tolist()))
        self.assertNotIn("aco-comp-grate", set(components["product_id"].tolist()))
        self.assertNotIn("aco-comp-accessory", set(components["product_id"].tolist()))
        self.assertTrue({"aco-comp-family","aco-comp-grate","aco-comp-accessory","aco-fp-public-designrost","aco-fp-showerstep-keil","aco-fp-aufsatz","aco-fp-ablaufkoerper","aco-fp-config-family"}.issubset(excluded_ids))
        self.assertNotIn("aco-fp-public-designrost", set(drains["product_id"].tolist()))
        self.assertNotIn("aco-fp-showerstep-keil", set(drains["product_id"].tolist()))
        self.assertNotIn("aco-fp-aufsatz", set(drains["product_id"].tolist()))
        self.assertNotIn("aco-fp-ablaufkoerper", set(drains["product_id"].tolist()))
        self.assertNotIn("aco-fp-config-family", set(drains["product_id"].tolist()))
        excl_by_id = excluded.set_index("product_id")
        self.assertEqual(str(excl_by_id.loc["aco-comp-family", "why_not_product_reason"]), "configuration_family_not_final_product")
        self.assertEqual(str(excl_by_id.loc["aco-comp-grate", "why_not_product_reason"]), "cover_only_component")
        self.assertEqual(str(excl_by_id.loc["aco-comp-accessory", "why_not_product_reason"]), "accessory_only")
        self.assertEqual(str(excl_by_id.loc["aco-fp-public-designrost", "why_not_product_reason"]), "cover_only_component")
        self.assertEqual(str(excl_by_id.loc["aco-fp-showerstep-keil", "why_not_product_reason"]), "accessory_only")
        self.assertEqual(str(excl_by_id.loc["aco-fp-aufsatz", "why_not_product_reason"]), "accessory_only")
        self.assertEqual(str(excl_by_id.loc["aco-fp-ablaufkoerper", "why_not_product_reason"]), "incomplete_assembly")
        self.assertEqual(str(excl_by_id.loc["aco-fp-config-family", "why_not_product_reason"]), "configuration_family_not_final_product")
        self.assertFalse(((components["promote_to_product"] == "yes") & (components["promotion_reason"] == "default")).any())
        self.assertFalse(excluded.empty)
        self.assertFalse(bom.empty)
        aco_bom = bom[bom["manufacturer"] == "aco"]
        self.assertFalse(aco_bom.empty)
        self.assertTrue({"component_id", "option_type", "option_family", "option_role", "parent_family", "source_url", "option_meta"}.issubset(set(aco_bom.columns)))
        # ensure options are concise and cleaned (grate contract string is intentionally longer)
        short_meta = aco_bom[~((aco_bom["option_type"].astype(str) == "compatible_grate") & (aco_bom["option_role"].astype(str) == "grate"))]
        self.assertTrue((short_meta["option_meta"].astype(str).str.len() < 180).all())
        self.assertTrue((aco_bom["option_label"].astype(str).str.len() < 150).all())
        self.assertTrue((aco_bom["option_label"].astype(str).str.contains("wishlist|warenkorb|menge", case=False, regex=True) == False).all())
        # spot-check at least one showerdrain base->grate and one accessory option
        self.assertTrue(((aco_bom["option_type"] == "compatible_grate") & (aco_bom["option_role"] == "grate")).any())
        self.assertTrue(((aco_bom["option_type"] == "optional_accessory") & (aco_bom["option_role"] == "accessory")).any())
        aco_summary_labels = set(evidence[evidence["manufacturer"] == "aco"]["label"].tolist())
        self.assertIn("aco_candidates_by_role", aco_summary_labels)
        self.assertIn("aco_products_by_role", aco_summary_labels)
        self.assertIn("aco_components_by_role", aco_summary_labels)
        self.assertIn("aco_bom_options_count", aco_summary_labels)
        self.assertIn("aco_bom_options_by_family", aco_summary_labels)

    def test_viega_lone_entities_remain_components_not_products(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "v-498210", "product_name": "Advantix-Duschrinnen-Grundkörper 4982.10", "product_url": "https://v.example/4982-10.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "advantix_line"},
                {"manufacturer": "viega", "product_id": "v-498294", "product_name": "Advantix-Duschrinnen-Geruchverschluss 4982.94", "product_url": "https://v.example/4982-94.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "advantix_line"},
                {"manufacturer": "viega", "product_id": "v-493361", "product_name": "Advantix-Rost 4933.61", "product_url": "https://v.example/4933-61.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "advantix_line"},
                {"manufacturer": "viega", "product_id": "v-498291", "product_name": "Advantix-Verstellfußset 4982.91", "product_url": "https://v.example/4982-91.html", "candidate_type": "component", "complete_system": "yes", "system_role": "accessory", "discovery_seed_family": "advantix_line"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        self.assertFalse(products.empty)
        self.assertTrue((products["candidate_type"] == "component").all())
        self.assertTrue((products["promote_to_product"] == "no").all())
        self.assertTrue(comparison.empty)
        self.assertTrue(excluded.empty)
        self.assertIn("Viega promotion", evidence["label"].tolist())
        self.assertTrue(bom.empty)

    def test_aco_bom_family_aware_matching_for_easyflow_and_showerdrain(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "aco", "product_id": "aco-90108544", "product_name": "ACO ShowerDrain C 1200 mm (Artikel-Nr. 90108544)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108544/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108554", "product_name": "ACO ShowerDrain C 1200 mm (Artikel-Nr. 90108554)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108554/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108524", "product_name": "ACO ShowerDrain C 1000 mm (Artikel-Nr. 90108524)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108524/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108534", "product_name": "ACO ShowerDrain C 1000 mm (Artikel-Nr. 90108534)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108534/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-easyflowplus-complete", "product_name": "ACO EasyFlow+ Komplettablauf DN50", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus-komplettablauf-dn50/", "candidate_type": "component", "complete_system": "component", "system_role": "configuration_family"},
                {"manufacturer": "aco", "product_id": "aco-easyflowplus-body", "product_name": "ACO EasyFlow+ Einzelablauf DN50", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus-einzelablauf-dn50/", "candidate_type": "component", "complete_system": "component", "system_role": "drain_body"},
                {"manufacturer": "aco", "product_id": "aco-easyflowplus-grate", "product_name": "ACO EasyFlow+ Designrost", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-plus-designrost/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
                {"manufacturer": "aco", "product_id": "aco-easyflow-grate", "product_name": "ACO Easyflow Design-Roste", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-design-roste/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
                {"manufacturer": "aco", "product_id": "aco-easyflow-body", "product_name": "ACO Easyflow Einzelablauf DN50", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-einzelablauf-dn50/", "candidate_type": "component", "complete_system": "component", "system_role": "drain_body"},
                {"manufacturer": "aco", "product_id": "aco-easyflow-complete", "product_name": "ACO Easyflow Komplettablauf DN50", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-komplettablauf-dn50/", "candidate_type": "component", "complete_system": "component", "system_role": "configuration_family"},
                {"manufacturer": "aco", "product_id": "aco-easyflow-adapter", "product_name": "ACO Easyflow Aufsatzstück", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-aufsatzstueck/", "candidate_type": "component", "complete_system": "component", "system_role": "accessory"},
                {"manufacturer": "aco", "product_id": "aco-showerdrainc-body", "product_name": "ACO ShowerDrain C Rinnenkörper", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper/", "candidate_type": "component", "complete_system": "component", "system_role": "drain_body"},
                {"manufacturer": "aco", "product_id": "aco-showerdrainc-grate", "product_name": "ACO ShowerDrain C Design-Rost", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/design-rost/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
                {"manufacturer": "aco", "product_id": "aco-showerdraine-grate", "product_name": "ACO ShowerDrain E+ Design-Rost", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/design-rost/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
                {"manufacturer": "aco", "product_id": "aco-showerdrainm-grate", "product_name": "ACO ShowerDrain M+ Design-Rost", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-mplus/design-rost/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"aco": _FakeAcoConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

        drains = products[products["candidate_type"] == "drain"]
        self.assertIn("aco-easyflowplus-complete", set(drains["product_id"].tolist()))
        self.assertIn("aco-easyflow-complete", set(drains["product_id"].tolist()))
        self.assertTrue({"aco-90108544", "aco-90108554", "aco-90108524", "aco-90108534"}.issubset(set(drains["product_id"].tolist())))
        self.assertEqual(drains.set_index("product_id").loc["aco-easyflowplus-complete", "system_role"], "complete_system")
        self.assertEqual(drains.set_index("product_id").loc["aco-easyflow-complete", "system_role"], "complete_system")
        components = products[products["candidate_type"] == "component"].set_index("product_id")
        self.assertTrue(components.empty)
        excl = excluded.set_index("product_id")
        self.assertEqual(str(excl.loc["aco-easyflowplus-grate", "why_not_product_reason"]), "cover_only_component")
        self.assertEqual(str(excl.loc["aco-easyflow-adapter", "why_not_product_reason"]), "accessory_only")
        self.assertEqual(str(excl.loc["aco-easyflowplus-body", "why_not_product_reason"]), "incomplete_assembly")
        self.assertEqual(str(excl.loc["aco-easyflow-body", "why_not_product_reason"]), "incomplete_assembly")

        aco_bom = bom[bom["manufacturer"] == "aco"]
        self.assertTrue(((aco_bom["product_id"] == "aco-90108544") & (aco_bom["component_id"] == "aco-showerdrainc-grate") & (aco_bom["option_type"] == "compatible_grate")).any())
        self.assertTrue(((aco_bom["product_id"] == "aco-easyflowplus-body") & (aco_bom["component_id"] == "aco-easyflowplus-grate") & (aco_bom["option_type"] == "compatible_grate")).any())
        self.assertTrue(((aco_bom["product_id"] == "aco-easyflow-complete") & (aco_bom["component_id"] == "aco-easyflow-adapter") & (aco_bom["option_type"] == "optional_accessory")).any())
        self.assertTrue(((aco_bom["product_id"] == "aco-easyflow-complete") & (aco_bom["component_id"] == "aco-easyflow-grate") & (aco_bom["option_type"] == "compatible_grate")).any())
        self.assertTrue(((aco_bom["product_id"] == "aco-showerdrainc-body") & (aco_bom["component_id"] == "aco-showerdrainc-grate") & (aco_bom["option_type"] == "compatible_grate")).any())
        # no cross-family pairings
        self.assertFalse(((aco_bom["product_id"] == "aco-easyflowplus-body") & (aco_bom["component_id"] == "aco-showerdrainc-grate")).any())
        self.assertFalse(((aco_bom["product_id"] == "aco-easyflowplus-body") & (aco_bom["component_id"] == "aco-easyflow-adapter")).any())
        self.assertFalse(((aco_bom["product_id"] == "aco-easyflow-complete") & (aco_bom["component_id"] == "aco-easyflowplus-grate")).any())
        self.assertFalse(((aco_bom["product_id"] == "aco-90108544") & (aco_bom["component_id"] == "aco-showerdraine-grate")).any())
        self.assertFalse(((aco_bom["product_id"] == "aco-90108544") & (aco_bom["component_id"] == "aco-showerdrainm-grate")).any())
        self.assertEqual(str(excl.loc["aco-easyflow-adapter", "system_role"]), "accessory")
        self.assertTrue(((aco_bom["parent_family"] == "easyflow") & (aco_bom["option_family"] == "easyflow")).any())
        assembled = products[
            (products["manufacturer"] == "aco")
            & (products["promotion_reason"] == "assembled_from_bom")
        ]
        self.assertFalse(assembled.empty)
        self.assertTrue((assembled["system_role"] == "assembled_system").all())
        self.assertTrue((assembled["assembly_reason"] == "aco_bom_body_grate_assembly").all())
        self.assertTrue((assembled["product_id"].astype(str).str.startswith("aco-assembled-")).all())
        # allowed families only
        self.assertTrue(set(assembled["parent_family"].dropna().tolist()).issubset({"easyflow", "easyflowplus", "showerdrain_c"}))
        # accessory rows must not create assembled variants
        self.assertFalse(assembled["matched_component_ids"].astype(str).str.contains("adapter|aufsatz", case=False, regex=True).any())
        # cross-family forbidden
        self.assertFalse(((assembled["parent_family"] == "easyflow") & assembled["matched_component_ids"].astype(str).str.contains("easyflowplus")).any())
        self.assertFalse(((assembled["parent_family"] == "easyflowplus") & assembled["matched_component_ids"].astype(str).str.contains("easyflow-")).any())
        # technical inheritance (present in base connector fixtures)
        self.assertTrue((assembled["flow_rate_lps"].notna()).any())
        components_all = products[(products["manufacturer"] == "aco") & (products["candidate_type"] == "component")]
        self.assertFalse((components_all["promotion_reason"] == "assembled_from_bom").any())
        self.assertFalse((components_all["assembled_from_bom"].astype(str).str.lower() == "true").any())

        labels = set(evidence[evidence["manufacturer"] == "aco"]["label"].tolist())
        self.assertIn("aco_reference_v2_showerdrain_c_bom_count", labels)
        self.assertIn("aco_reference_v2_easyflowplus_products_count", labels)
        self.assertIn("aco_reference_v2_easyflow_products_count", labels)
        self.assertIn("aco_reference_v2_cross_family_rejected_count", labels)
        self.assertIn("sample_aco_reference_v2_easyflow_bom", labels)
        self.assertIn("aco_hash_like_ids_before_count", labels)
        self.assertIn("aco_hash_like_ids_after_count", labels)
        self.assertIn("aco_orphan_bom_references_count", labels)
        self.assertIn("aco_assembled_products_by_family", labels)
        self.assertIn("sample_aco_assembled_products", labels)
        self.assertIn("aco_assembled_products_accessory_combinations_skipped_count", labels)
        self.assertIn("aco_assembled_products_emitted_to_products_count", labels)
        self.assertIn("aco_assembled_products_left_in_components_count", labels)
        aco_ev = evidence[evidence["manufacturer"] == "aco"].set_index("label")
        self.assertEqual(str(aco_ev.loc["aco_hash_like_ids_after_count", "snippet"]), "0")
        self.assertEqual(str(aco_ev.loc["aco_orphan_bom_references_count", "snippet"]), "0")
        self.assertEqual(str(aco_ev.loc["aco_assembled_products_left_in_components_count", "snippet"]), "0")

    def test_easyflow_assembled_rows_inherit_ws_dn_on_returned_products(self):
        class _FakeEasyflowPartialConnector:
            @staticmethod
            def extract_parameters(url):
                if "komplettablaeufe-aco-easyflow-dn-50" in str(url or "").lower():
                    return {"water_seal_mm": 50.0, "outlet_dn": "DN50", "evidence": []}
                return {"evidence": []}

            @staticmethod
            def get_bom_options(url, params=None):
                return []

        registry = pd.DataFrame(
            [
                {
                    "manufacturer": "aco",
                    "product_id": "aco-easyflow-komplettablaeufe-aco-easyflow-dn-50",
                    "product_name": "ACO Easyflow Komplettabläufe ACO Easyflow DN 50",
                    "product_url": "https://example.test/aco-easyflow-komplettablaeufe-aco-easyflow-dn-50",
                    "candidate_type": "component",
                    "complete_system": "component",
                    "system_role": "configuration_family",
                    "product_family": "easyflow",
                },
                {
                    "manufacturer": "aco",
                    "product_id": "aco-easyflow-design-roste",
                    "product_name": "ACO Easyflow Design-Roste",
                    "product_url": "https://example.test/aco-easyflow-design-roste",
                    "candidate_type": "component",
                    "complete_system": "component",
                    "system_role": "grate",
                    "product_family": "easyflow",
                },
                {
                    "manufacturer": "aco",
                    "product_id": "aco-easyflow-design-roste-square",
                    "product_name": "ACO Easyflow Design-Roste square",
                    "product_url": "https://example.test/aco-easyflow-design-roste-square",
                    "candidate_type": "component",
                    "complete_system": "component",
                    "system_role": "grate",
                    "product_family": "easyflow",
                },
            ]
        )

        with patch.dict(pipeline.CONNECTORS, {"aco": _FakeEasyflowPartialConnector()}, clear=True):
            products, _comparison, _excluded, _evidence, _bom = pipeline.run_update(registry, default_config())

        easyflow_assembled = products[
            products["product_id"].astype(str).str.startswith(
                "aco-assembled-easyflow-aco-easyflow-komplettablaeufe-aco-easyflow-dn-50__"
            )
        ]
        self.assertEqual(len(easyflow_assembled), 2)
        self.assertTrue((easyflow_assembled["water_seal_mm"] == 50.0).all())
        self.assertEqual(set(easyflow_assembled["outlet_dn"].astype(str)), {"DN50"})
        self.assertTrue(easyflow_assembled["flow_rate_lps"].isna().all())
        self.assertTrue(easyflow_assembled["height_adj_min_mm"].isna().all())
        self.assertTrue(easyflow_assembled["height_adj_max_mm"].isna().all())


    def test_aco_hash_like_registry_ids_are_migrated_before_export(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "aco", "product_id": "aco-comp-7288454562667788658", "product_name": "ACO Easyflow Design-Roste", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-design-roste/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
                {"manufacturer": "aco", "product_id": "aco-comp-4293387199132084100", "product_name": "ACO Easyflow Aufsatzstücke Standard", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-easyflow-aufsatzstuecke-standard/", "candidate_type": "component", "complete_system": "component", "system_role": "accessory"},
                {"manufacturer": "aco", "product_id": "aco-90108544", "product_name": "ACO ShowerDrain C 1200 mm (Artikel-Nr. 90108544)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108544/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"aco": _FakeAcoConnector()}, clear=True):
            products, comparison, _excluded, evidence, _bom = pipeline.run_update(registry, default_config())
        aco_ids = set(products[products["manufacturer"] == "aco"]["product_id"].tolist())
        self.assertIn("aco-90108544", aco_ids)
        self.assertFalse(any(re.match(r"^aco-(?:comp|fam)-\\d+$", pid) for pid in aco_ids))
        non_component_aco_ids = set(
            products[
                (products["manufacturer"] == "aco")
                & (~products["candidate_type"].astype(str).str.lower().isin({"component", "base_set"}))
            ]["product_id"].tolist()
        )
        self.assertEqual(set(comparison["product_id"].tolist()), non_component_aco_ids)
        aco_ev = evidence[evidence["manufacturer"] == "aco"].set_index("label")
        self.assertGreaterEqual(int(aco_ev.loc["aco_stable_id_migration_count", "snippet"]), 1)
        self.assertEqual(str(aco_ev.loc["aco_hash_like_ids_after_count", "snippet"]), "0")
        self.assertEqual(str(aco_ev.loc["aco_hash_like_product_ids_after_count", "snippet"]), "0")
        self.assertEqual(str(aco_ev.loc["aco_hash_like_component_ids_after_count", "snippet"]), "0")
        self.assertIn("aco_hash_like_bom_product_refs_after_count", set(aco_ev.index))
        self.assertIn("aco_hash_like_bom_component_refs_after_count", set(aco_ev.index))

        with patch.dict(pipeline.CONNECTORS, {"aco": _FakeAcoConnector()}, clear=True):
            products2, comparison2, _excluded2, _evidence2, _bom2 = pipeline.run_update(registry, default_config())
        self.assertEqual(set(products2["product_id"].tolist()), aco_ids)
        self.assertEqual(set(comparison2["product_id"].tolist()), set(comparison["product_id"].tolist()))

    def test_viega_complete_assembly_promotes_body_to_product(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "v-498210", "product_name": "Advantix-Duschrinnen-Grundkörper 4982.10", "product_url": "https://v.example/4982-10.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "advantix_line"},
                {"manufacturer": "viega", "product_id": "v-498211", "product_name": "Advantix-Duschrinne 4982.11", "product_url": "https://v.example/4982-11.html", "candidate_type": "drain", "complete_system": "yes", "system_role": "complete_drain", "discovery_seed_family": "advantix_line"},
                {"manufacturer": "viega", "product_id": "v-493361", "product_name": "Advantix-Rost 4933.61", "product_url": "https://v.example/4982-61.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "advantix_line"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        self.assertGreaterEqual(len(products), 1)
        self.assertTrue((products["manufacturer"] == "viega").all())
        self.assertIn("yes", set(products["promote_to_product"].tolist()))
        promoted = products[products["promote_to_product"] == "yes"]
        self.assertTrue(all("rost" not in str(x).lower() for x in promoted["product_name"].tolist()))
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_kaldewei_baseline_products_components_bom_and_evidence(self):
        rows, _ = kaldewei.discover_candidates()
        registry = pd.DataFrame(rows)
        with patch.dict(pipeline.CONNECTORS, {"kaldewei": kaldewei}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        self.assertFalse(products.empty)
        self.assertTrue(excluded.empty)
        self.assertTrue((products["manufacturer"] == "kaldewei").all())
        self.assertIn("kaldewei-nexsys", set(products[products["candidate_type"] == "drain"]["product_id"].tolist()))
        self.assertIn("kaldewei-flowline-zero", set(products[products["candidate_type"] == "component"]["product_id"].tolist()))
        self.assertIn("kaldewei-flowpoint-zero", set(products[products["candidate_type"] == "component"]["product_id"].tolist()))
        self.assertIn("kaldewei-nexsys-design-cover-brushed", set(products[products["candidate_type"] == "component"]["product_id"].tolist()))
        finish_components = products[(products["manufacturer"] == "kaldewei") & (products["system_role"] == "finish_cover")]
        self.assertEqual(len(finish_components), 10)
        self.assertEqual(set(finish_components["finish_name"].tolist()), {"brushed steel", "brushed champagne", "brushed graphite", "alpine white matt", "black matt 100"})
        self.assertTrue({"930", "931", "932", "711", "676"}.issubset(set(finish_components["finish_code"].astype(str).tolist())))
        assembled = products[(products["manufacturer"] == "kaldewei") & (products["promotion_reason"] == "assembled_from_bom")]
        self.assertEqual(len(assembled), 6)
        self.assertEqual(
            set(assembled["product_family"].tolist()),
            {"flowline_zero", "flowpoint_zero", "nexsys"},
        )
        self.assertTrue((assembled["candidate_type"] == "drain").all())
        self.assertTrue((assembled["promote_to_product"] == "yes").all())
        self.assertEqual(
            set(assembled["trap_component_id"].tolist()),
            {
                "kaldewei-flowdrain-horizontal-regular",
                "kaldewei-flowdrain-horizontal-flat",
                "kaldewei-ka-4121",
                "kaldewei-ka-4122",
            },
        )
        self.assertTrue(((assembled["product_id"] == "kaldewei-assembled-flowline-zero__flowdrain-horizontal-regular") & (assembled["flow_rate_lps"] == 0.8)).any())
        self.assertTrue(((assembled["product_id"] == "kaldewei-assembled-flowpoint-zero__flowdrain-horizontal-flat") & (assembled["flow_rate_lps"] == 0.63)).any())
        self.assertTrue(((bom["product_id"] == "kaldewei-flowline-zero") & (bom["component_id"] == "kaldewei-flowdrain-horizontal-regular")).any())
        self.assertTrue(((bom["product_id"] == "kaldewei-flowpoint-zero") & (bom["component_id"] == "kaldewei-flowdrain-horizontal-regular") & (bom["parent_family"] == "flowpoint_zero")).any())
        self.assertTrue(((bom["product_id"] == "kaldewei-nexsys") & (bom["component_id"] == "kaldewei-ka-4121")).any())
        self.assertTrue(((bom["product_id"] == "kaldewei-nexsys") & (bom["component_id"] == "kaldewei-nexsys-design-cover-brushed") & (bom["option_type"] == "compatible_cover")).any())
        self.assertEqual(len(bom[(bom["product_id"] == "kaldewei-flowline-zero") & (bom["option_type"] == "compatible_finish")]), 5)
        self.assertEqual(len(bom[(bom["product_id"] == "kaldewei-flowpoint-zero") & (bom["option_type"] == "compatible_finish")]), 5)
        self.assertFalse(((bom["product_id"] == "kaldewei-ka-4121") | (bom["product_id"] == "kaldewei-ka-4122")).any())
        nexsys_assembled = assembled[assembled["product_id"].astype(str).str.contains("nexsys")]
        self.assertEqual(len(nexsys_assembled), 2)
        self.assertEqual(
            set(nexsys_assembled["product_id"].tolist()),
            {
                "kaldewei-assembled-nexsys__ka-4121",
                "kaldewei-assembled-nexsys__ka-4122",
            },
        )
        self.assertEqual(
            set(nexsys_assembled["trap_component_id"].tolist()),
            {"kaldewei-ka-4121", "kaldewei-ka-4122"},
        )
        self.assertTrue((nexsys_assembled["candidate_type"] == "drain").all())
        self.assertTrue((nexsys_assembled["system_role"] == "assembled_system").all())
        comp = products[products["candidate_type"] == "component"].set_index("product_id")
        self.assertEqual(float(comp.loc["kaldewei-flowdrain-horizontal-flat", "flow_rate_lps"]), 0.63)
        self.assertEqual(str(comp.loc["kaldewei-flowdrain-horizontal-flat", "outlet_dn"]), "DN40")
        self.assertEqual(float(comp.loc["kaldewei-ka-90-horizontal", "flow_rate_lps"]), 0.71)
        self.assertEqual(float(comp.loc["kaldewei-ka-90-flat", "flow_rate_lps"]), 0.68)
        self.assertEqual(float(comp.loc["kaldewei-ka-90-vertical", "flow_rate_lps"]), 1.22)
        ka90 = comp.loc[[x for x in comp.index if str(x).startswith("kaldewei-ka-90-")]]
        self.assertEqual(len(ka90), 3)
        self.assertTrue((ka90["product_category"].astype(str) == "tray_waste_fitting").all())
        self.assertTrue((ka90["system_role"].astype(str) == "tray_waste_fitting").all())
        self.assertTrue((ka90["complete_system"].astype(str) == "component").all())
        self.assertTrue((ka90["model_number"].astype(str).isin(["4103", "4104", "4105"])).all())
        self.assertTrue((ka90["outlet_orientation"].astype(str).str.strip() != "").all())
        self.assertTrue((ka90["outlet_dn"].astype(str).str.strip() != "").all())
        self.assertFalse(comparison[comparison["manufacturer"] == "kaldewei"]["product_id"].astype(str).str.startswith("kaldewei-ka-90-").any())
        self.assertEqual(float(comp.loc["kaldewei-ka-300-horizontal", "flow_rate_lps"]), 0.61)
        self.assertEqual(float(comp.loc["kaldewei-ka-300-flat", "flow_rate_lps"]), 0.57)
        self.assertEqual(str(comp.loc["kaldewei-ka-125-legacy", "product_family"]), "ka_125")
        ka120 = comp.loc[[x for x in comp.index if str(x).startswith("kaldewei-ka-120-")]]
        self.assertEqual(len(ka120), 3)
        self.assertTrue((ka120["product_category"].astype(str) == "tray_waste_fitting").all())
        self.assertTrue((ka120["system_role"].astype(str) == "tray_waste_fitting").all())
        self.assertTrue((ka120["complete_system"].astype(str) == "component").all())
        if "selected_length_mm" in ka120.columns:
            self.assertTrue((ka120["selected_length_mm"].astype(str).isin(["", "not_applicable", "nan", "None"])).all())
        self.assertIn(0.85, set(ka120["flow_rate_lps"].astype(float).tolist()))
        self.assertIn(1.4, set(ka120["flow_rate_lps"].astype(float).tolist()))
        self.assertTrue((ka120["outlet_orientation"].astype(str).str.strip() != "").all())
        self.assertTrue((ka120["outlet_dn"].astype(str).str.strip() != "").all())
        self.assertTrue((ka120["water_seal_mm"].notna()).all())
        self.assertTrue((ka120["model_number"].astype(str).str.strip() != "").all())
        self.assertTrue((ka120["article_number"].astype(str).str.strip() != "").all())
        self.assertEqual(set(ka120["article_number"].astype(str).tolist()), {"687772530000", "687772510000", "687772520000"})
        self.assertTrue(set(ka120["article_number"].astype(str).tolist()).isdisjoint({"687675", "687676", "687677"}))
        self.assertEqual(float(ka120.loc["kaldewei-ka-120-horizontal", "height_adj_min_mm"]), 83.0)
        self.assertEqual(float(ka120.loc["kaldewei-ka-120-flat", "height_adj_min_mm"]), 63.0)
        self.assertEqual(float(ka120.loc["kaldewei-ka-120-vertical", "height_adj_min_mm"]), 83.0)
        self.assertFalse(comparison[comparison["manufacturer"] == "kaldewei"]["product_id"].astype(str).str.startswith("kaldewei-ka-120-").any())
        drains = products[products["candidate_type"] == "drain"].set_index("product_id")
        self.assertEqual(str(drains.loc["kaldewei-nexsys", "promotion_reason"]), "integrated_shower_surface_system")
        self.assertEqual(
            str(drains.loc["kaldewei-xetis-ka-200", "promotion_reason"]),
            "xetis_configuration_with_ka200",
)
        labels = set(evidence[evidence["manufacturer"] == "kaldewei"]["label"].tolist())
        self.assertIn("kaldewei_registry_candidates_count", labels)
        self.assertIn("kaldewei_final_rows_count", labels)
        self.assertIn("kaldewei_bom_options_count", labels)
        self.assertIn("sample_kaldewei_bom_options", labels)
        self.assertIn("kaldewei_assembled_products_created_count", labels)
        self.assertIn("kaldewei_nexsys_design_covers_count", labels)
        self.assertIn("kaldewei_flow_finish_components_count", labels)
        ev = evidence[evidence["manufacturer"] == "kaldewei"].set_index("label")
        self.assertEqual(str(ev.loc["kaldewei_registry_candidates_count", "snippet"]), str(len(registry)))
        self.assertEqual(str(ev.loc["kaldewei_final_rows_count", "snippet"]), str(len(products)))
        self.assertEqual(str(ev.loc["kaldewei_assembled_products_created_count", "snippet"]), "6")
        self.assertEqual(str(ev.loc["kaldewei_assembled_products_left_in_components_count", "snippet"]), "0")
        self.assertEqual(str(ev.loc["kaldewei_nexsys_drain_sets_count", "snippet"]), "2")
        self.assertEqual(str(ev.loc["kaldewei_flowline_finish_components_count", "snippet"]), "5")
        self.assertEqual(str(ev.loc["kaldewei_flowpoint_finish_components_count", "snippet"]), "5")
        ka120_evidence = evidence[
            (evidence["manufacturer"] == "kaldewei")
            & (evidence["product_id"].astype(str).str.startswith("kaldewei-ka-120-"))
            & (evidence["field_name"] == "flow_rate_lps")
        ]
        self.assertFalse(ka120_evidence.empty)
        self.assertTrue(((ka120_evidence["source_url"].astype(str).str.strip() != "") | (ka120_evidence["source_label"].astype(str).str.strip() != "")).any())
        ka90_evidence = evidence[
            (evidence["manufacturer"] == "kaldewei")
            & (evidence["product_id"].astype(str).str.startswith("kaldewei-ka-90-"))
            & (evidence["field_name"].isin(["model_number", "flow_rate_lps", "outlet_dn", "dn"]))
        ]
        self.assertFalse(ka90_evidence.empty)
        self.assertTrue(((ka90_evidence["source_url"].astype(str).str.strip() != "") | (ka90_evidence["source_label"].astype(str).str.strip() != "")).all())
        kaldewei_evidence = evidence[evidence["manufacturer"] == "kaldewei"].copy()
        technical_ev = kaldewei_evidence[
            ~kaldewei_evidence["field_name"].astype(str).str.lower().isin(["", "nan", "none"])
        ]
        self.assertFalse(technical_ev["extracted_value"].astype(str).str.lower().isin(["", "nan", "none"]).any())
        ka120_note_mask = kaldewei_evidence["source_note"].astype(str).str.contains("KA120 value seeded|KA 120 value seeded|official Kaldewei KA120 technical sheet", case=False, regex=True)
        self.assertTrue((kaldewei_evidence[ka120_note_mask]["product_id"].astype(str).str.startswith("kaldewei-ka-120-")).all())
        non_ka120 = kaldewei_evidence[~kaldewei_evidence["product_id"].astype(str).str.startswith("kaldewei-ka-120-")]
        self.assertFalse(non_ka120["source_note"].astype(str).str.contains("KA120 value seeded|KA 120 value seeded|official Kaldewei KA120 technical sheet", case=False, regex=True).any())
        ka90_note_mask = kaldewei_evidence["source_note"].astype(str).str.contains("KA90 value seeded|official Kaldewei KA90 technical source", case=False, regex=True)
        self.assertTrue((kaldewei_evidence[ka90_note_mask]["product_id"].astype(str).str.startswith("kaldewei-ka-90-")).all())
        self.assertFalse((evidence[(evidence["manufacturer"] == "kaldewei") & (evidence["product_id"].astype(str).str.startswith("kaldewei-ka-120-"))]["snippet"].astype(str) == "0.0").any())
        self.assertTrue(
            (
                (evidence["manufacturer"] == "kaldewei")
                & (evidence["product_id"].astype(str).str.startswith("kaldewei-ka-120-"))
                & (evidence["field_name"] == "article_number")
            ).any()
        )
        text_cols = [c for c in ["promotion_reason", "why_not_product_reason", "assembly_reason", "current_status", "compatibility_caution", "matched_component_ids", "source_url"] if c in products.columns]
        for c in text_cols:
            self.assertFalse(products[c].astype(str).str.lower().str.contains("^nan$|^none$", regex=True).any())

    def test_should_emit_evidence_value_helper(self):
        self.assertFalse(pipeline._should_emit_evidence_value(None))
        self.assertFalse(pipeline._should_emit_evidence_value(float("nan")))
        self.assertFalse(pipeline._should_emit_evidence_value("nan"))
        self.assertFalse(pipeline._should_emit_evidence_value(""))
        self.assertFalse(pipeline._should_emit_evidence_value("not_applicable"))
        self.assertTrue(pipeline._should_emit_evidence_value(0.85))
        self.assertTrue(pipeline._should_emit_evidence_value("DN50"))
        self.assertTrue(pipeline._should_emit_evidence_value("687772530000"))

    def test_viega_badablauf_pages_are_drain_body_not_accessory(self):
        for name in [
            "Advantix Top-Badablauf 4914-20",
            "Advantix Top Badablauf 4914-20",
            "Advantix-Badablauf 4980-60",
            "Advantix-Badablauf 4980-61",
            "Advantix-Badablauf 4980-63",
            "Advantix Top-Bodenablauf 4914-11",
            "Advantix Top-Bodenablauf 4914-21",
            "Advantix-Bodenablauf-Grundkörper 4951-15",
            "Advantix-Bodenablauf-Grundkörper 4955-15",
            "Advantix-Bodenablauf-Grundkörper 4955-25",
        ]:
            role = pipeline._infer_viega_role({"system_role": "accessory", "product_name": name, "product_url": f"https://v.example/{name.replace(' ', '-')}.html"})
            self.assertEqual(role, "base_set")

    def test_viega_drain_body_pages_use_incomplete_assembly_not_non_promotable_accessory(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "v-491420", "product_name": "Advantix Top-Badablauf 4914-20", "product_url": "https://v.example/4914-20.html", "candidate_type": "component", "complete_system": "yes", "system_role": "accessory", "discovery_seed_family": "advantix_floor"},
                {"manufacturer": "viega", "product_id": "v-498060", "product_name": "Advantix-Badablauf 4980-60", "product_url": "https://v.example/4980-60.html", "candidate_type": "component", "complete_system": "yes", "system_role": "accessory", "discovery_seed_family": "advantix_floor"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        self.assertFalse(products.empty)
        self.assertTrue((products["promote_to_product"] == "no").all())
        self.assertTrue((products["why_not_product_reason"] == "incomplete_assembly").all())
        self.assertNotIn("non_promotable_accessory", set(products["why_not_product_reason"].tolist()))
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_viega_explicit_override_ids_force_base_set_incomplete_assembly(self):
        rows = []
        for pid, name in [
            ("viega-491420", "Advantix Top-Badablauf 4914-20"),
            ("viega-498060", "Advantix-Badablauf 4980-60"),
            ("viega-498061", "Advantix-Badablauf 4980-61"),
            ("viega-498063", "Advantix-Badablauf 4980-63"),
            ("viega-495120", "Advantix-Bodenablauf 4951-20"),
            ("viega-495115", "Advantix-Bodenablauf-Grundkörper 4951-15"),
            ("viega-495515", "Advantix-Bodenablauf-Grundkörper 4955-15"),
            ("viega-495525", "Advantix-Bodenablauf-Grundkörper 4955-25"),
            ("viega-491411", "Advantix Top-Bodenablauf 4914-11"),
            ("viega-491421", "Advantix Top-Bodenablauf 4914-21"),
        ]:
            rows.append(
                {
                    "manufacturer": "viega",
                    "product_id": pid,
                    "product_name": name,
                    "product_url": f"https://v.example/{pid}.html",
                    "candidate_type": "component",
                    "complete_system": "yes",
                    "system_role": "accessory",
                    "discovery_seed_family": "advantix_floor",
                }
            )
        registry = pd.DataFrame(rows)
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        self.assertEqual(len(products), 10)
        self.assertTrue((products["promote_to_product"] == "no").all())
        self.assertTrue((products["why_not_product_reason"] == "incomplete_assembly").all())
        self.assertNotIn("non_promotable_accessory", set(products["why_not_product_reason"].tolist()))
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_viega_tray_tempoplex_pairing_promotes_only_synthetic_complete_system(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640", "product_name": "Tempoplex-Abdeckhaube 6964.0", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        standalone = products[products["product_id"].isin(["viega-69631", "viega-69640"])]
        self.assertTrue((standalone["promote_to_product"] == "no").all())
        paired = products[products["promotion_reason"] == "tray_base_with_cover_pairing"]
        self.assertEqual(len(paired), 1)
        self.assertEqual(paired.iloc[0]["promote_to_product"], "yes")
        self.assertEqual(paired.iloc[0]["pairing_reason"], "compatible_cover_match")
        self.assertIn("tray_complete_systems_created_count", set(evidence["label"].tolist()))
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_viega_tray_domoplex_base_without_cover_stays_incomplete(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-692821", "product_name": "Domoplex-Ablauf 6928.21 Funktionseinheit ohne Abdeckhaube", "product_url": "https://v.example/Domoplex-Ablauf-6928-21.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "domoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, _comparison, excluded, _evidence, bom = pipeline.run_update(registry, default_config())
        self.assertEqual(len(products), 1)
        self.assertEqual(products.iloc[0]["promote_to_product"], "no")
        self.assertEqual(products.iloc[0]["promotion_reason"], "incomplete_assembly")
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_varioplex_complete_drain_can_still_promote_when_not_incomplete_function_unit(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-777711", "product_name": "Varioplex-Ablauf 7777.11", "product_url": "https://v.example/Varioplex-Ablauf-7777-11.html", "candidate_type": "drain", "complete_system": "yes", "system_role": "complete_drain", "discovery_seed_family": "varioplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, _comparison, excluded, _evidence, bom = pipeline.run_update(registry, default_config())
        self.assertEqual(len(products), 1)
        self.assertEqual(products.iloc[0]["promote_to_product"], "yes")
        self.assertEqual(products.iloc[0]["promotion_reason"], "promoted_complete_assembly")
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_tray_pairing_rejects_ersatz_cover_candidates(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69695-r", "product_name": "Tempoplex-Dichtung 6969.5 Ersatzteil", "product_url": "https://v.example/Ersatzteile/Tempoplex-Dichtung-6969-5.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        self.assertFalse((products["promotion_reason"] == "tray_base_with_cover_pairing").any())
        summary = evidence[evidence["label"] == "rejected_ersatzteile_cover_count"]["snippet"].tolist()
        self.assertTrue(summary and int(summary[0]) >= 1)
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_tempoplex_cover_variants_are_parsed_and_pairing_emits_per_variant_products(self):
        class _FakeVariantConnector(_FakeViegaConnector):
            @staticmethod
            def extract_parameters(url):
                base = _FakeViegaConnector.extract_parameters(url)
                if "abdeckhaube-6964-0" in url.lower():
                    base["article_rows_json"] = (
                        '[{"article_no":"649 982 *)","variant_label":"Kunststoff verchromt","_row_text":"Kunststoff verchromt 649 982 *)"},'
                        '{"article_no":"649 982","variant_label":"Kunststoff verchromt","_row_text":"Kunststoff verchromt 649 982"},'
                        '{"article_no":"806 132","variant_label":"Kunststoff schwarz matt","_row_text":"Kunststoff schwarz matt 806 132"},'
                        '{"article_no":"775 070 1) siehe auch 775 087 775 094","variant_label":"Kunststoff Sonderfarbe","_row_text":"Kunststoff Sonderfarbe 775 070 1) siehe auch 775 087 775 094"},'
                        '{"article_no":"775 087 1) siehe auch 775 070 775 094","variant_label":"Kunststoff Metallfarbe","_row_text":"Kunststoff Metallfarbe 775 087 1) siehe auch 775 070 775 094"},'
                        '{"article_no":"775 094 1) siehe auch 775 070 775 087","variant_label":"vergoldet","_row_text":"vergoldet 775 094 1) siehe auch 775 070 775 087"},'
                        '{"article_no":"649 982 806 132","variant_label":"BAD CONCAT","_row_text":"This is a malformed concatenated pseudo-row with two article numbers 649 982 and 806 132"}]'
                    )
                return base

        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640", "product_name": "Tempoplex-Abdeckhaube 6964.0", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeVariantConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        paired = products[products["promotion_reason"] == "tray_base_with_cover_pairing"]
        self.assertEqual(len(paired), 5)
        self.assertIn("viega-69631__649982", set(paired["product_id"].tolist()))
        self.assertIn("viega-69631__806132", set(paired["product_id"].tolist()))
        self.assertIn("viega-69631__775070", set(paired["product_id"].tolist()))
        self.assertIn("viega-69631__775087", set(paired["product_id"].tolist()))
        self.assertIn("viega-69631__775094", set(paired["product_id"].tolist()))
        p649 = paired[paired["product_id"] == "viega-69631__649982"].iloc[0]
        self.assertEqual(p649["cover_article_no"], "649982")
        self.assertEqual(p649["diameter_mm"], 115)
        self.assertEqual(p649["compatible_outlet_size"], "D90")
        variant_components = products[products["promotion_reason"] == "cover_only_component"]
        self.assertEqual(len(variant_components), 5)
        c649 = variant_components[variant_components["product_id"] == "viega-69640__649982"].iloc[0]
        self.assertEqual(c649["diameter_mm"], 115)
        self.assertEqual(c649["compatible_outlet_size"], "D90")
        variant_count = evidence[evidence["label"] == "tray_cover_variant_count"]["snippet"].tolist()
        self.assertTrue(variant_count and int(variant_count[0]) >= 5)
        sample_rows = evidence[evidence["label"] == "sample_cover_variant_rows"]["snippet"].tolist()
        self.assertTrue(sample_rows and "649982" in sample_rows[0])
        rejected = evidence[evidence["label"] == "rejected_malformed_cover_rows_count"]["snippet"].tolist()
        self.assertTrue(rejected and int(rejected[0]) >= 1)
        deduped = evidence[evidence["label"] == "deduplicated_cover_variant_rows_count"]["snippet"].tolist()
        self.assertTrue(deduped and int(deduped[0]) >= 1)
        normalized = evidence[evidence["label"] == "normalized_article_numbers"]["snippet"].tolist()
        self.assertTrue(normalized and "649982" in normalized[0])
        accepted_6964 = evidence[evidence["label"] == "sample_6964_rows_accepted"]["snippet"].tolist()
        self.assertTrue(accepted_6964 and "775070" in " ".join(accepted_6964))
        paired_valid = evidence[evidence["label"] == "paired_products_created_from_valid_variants_count"]["snippet"].tolist()
        self.assertTrue(paired_valid and int(paired_valid[0]) >= 5)
        tempoplex_pairs = evidence[evidence["label"] == "tempoplex_products_created_from_cover_variants_count"]["snippet"].tolist()
        self.assertTrue(tempoplex_pairs and int(tempoplex_pairs[0]) >= 5)
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_tempoplex_deterministic_pairing_fix_allows_6963_1_with_6964_0_across_tempoplex_aliases(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640", "product_name": "Tempoplex-Plus-Abdeckhaube 6964.0", "product_url": "https://v.example/Tempoplex-Plus-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex_plus"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        paired = products[products["promotion_reason"] == "tray_base_with_cover_pairing"]
        self.assertEqual(len(paired), 1)
        fix = evidence[evidence["label"] == "tempoplex_pairing_fix_applied"]["snippet"].tolist()
        self.assertTrue(fix and int(fix[0]) >= 1)
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_tempoplex_6964_explicit_fallback_recovers_missing_775_variants(self):
        class _FakeFallbackConnector(_FakeViegaConnector):
            @staticmethod
            def extract_parameters(url):
                base = _FakeViegaConnector.extract_parameters(url)
                if "abdeckhaube-6964-0" in url.lower():
                    base["article_rows_json"] = (
                        '[{"article_no":"649 982","variant_label":"Kunststoff verchromt","_row_text":"Kunststoff verchromt 649 982"},'
                        '{"article_no":"806 132","variant_label":"Kunststoff schwarz matt","_row_text":"Kunststoff schwarz matt 806 132"}]'
                    )
                return base

        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640", "product_name": "Tempoplex-Abdeckhaube 6964.0", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeFallbackConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        paired_ids = set(products[products["promotion_reason"] == "tray_base_with_cover_pairing"]["product_id"].tolist())
        component_ids = set(products[products["promotion_reason"] == "cover_only_component"]["product_id"].tolist())
        self.assertIn("viega-69631__649982", paired_ids)
        self.assertIn("viega-69631__806132", paired_ids)
        self.assertIn("viega-69631__775070", paired_ids)
        self.assertIn("viega-69631__775087", paired_ids)
        self.assertIn("viega-69631__775094", paired_ids)
        self.assertIn("viega-69640__775070", component_ids)
        self.assertIn("viega-69640__775087", component_ids)
        self.assertIn("viega-69640__775094", component_ids)
        explicit_cnt = evidence[evidence["label"] == "explicit_tempoplex_6964_seed_applied_count"]["snippet"].tolist()
        self.assertTrue(explicit_cnt and int(explicit_cnt[0]) >= 3)
        explicit_articles = evidence[evidence["label"] == "explicit_tempoplex_6964_seed_articles"]["snippet"].tolist()
        self.assertTrue(explicit_articles and "775070" in explicit_articles[0] and "775087" in explicit_articles[0] and "775094" in explicit_articles[0])
        sample_explicit = evidence[evidence["label"] == "sample_explicit_tempoplex_6964_seed_rows"]["snippet"].tolist()
        self.assertTrue(sample_explicit and "775070" in sample_explicit[0])
        seeded_opts = bom[(bom["option_group"] == "cover_variant") & (bom["option_sku"].isin(["775070", "775087", "775094"]))]
        self.assertEqual(set(seeded_opts["option_sku"].tolist()), {"775070", "775087", "775094"})
        self.assertTrue(excluded.empty)
        self.assertFalse(bom.empty)

    def test_tempoplex_final_fallback_emits_pair_when_family_hints_are_missing(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "unknown"},
                {"manufacturer": "viega", "product_id": "viega-69640", "product_name": "Tempoplex-Abdeckhaube 6964.0", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "unknown"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        paired = products[products["pairing_reason"] == "tempoplex_6963_1_to_6964_0_final_fallback"]
        self.assertEqual(len(paired), 1)
        self.assertIn("viega-69631", paired.iloc[0]["matched_component_ids"])
        self.assertIn("viega-69640", paired.iloc[0]["matched_component_ids"])
        fix = evidence[evidence["label"] == "tempoplex_pairing_fix_applied"]["snippet"].tolist()
        self.assertTrue(fix and int(fix[0]) >= 1)
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_late_stage_seed_works_when_only_6964_anchor_variants_exist(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640__649982", "product_name": "Tempoplex-Abdeckhaube 6964.0 [649982]", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640__806132", "product_name": "Tempoplex-Abdeckhaube 6964.0 [806132]", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeViegaConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        ids = set(products["product_id"].tolist())
        self.assertIn("viega-69640__775070", ids)
        self.assertIn("viega-69640__775087", ids)
        self.assertIn("viega-69640__775094", ids)
        self.assertIn("viega-69631__775070", ids)
        self.assertIn("viega-69631__775087", ids)
        self.assertIn("viega-69631__775094", ids)
        seed_cnt = evidence[evidence["label"] == "explicit_tempoplex_6964_seed_applied_count"]["snippet"].tolist()
        self.assertTrue(seed_cnt and int(seed_cnt[0]) >= 3)
        self.assertTrue(excluded.empty)
        self.assertFalse(bom.empty)

    def test_paired_tray_product_inherits_hydraulic_fields_from_base_set(self):
        class _FakeInheritanceConnector(_FakeViegaConnector):
            @staticmethod
            def extract_parameters(url):
                if "ablauf-6963-1" in url.lower():
                    return {
                        "flow_rate_lps": 0.72,
                        "outlet_dn": "DN50",
                        "flow_rate_raw_text": "Ablaufleistung 0,72 l/s",
                        "material_detail": "Kunststoff",
                        "evidence": [],
                    }
                return {
                    "flow_rate_lps": None,
                    "outlet_dn": None,
                    "material_detail": "Edelstahl",
                    "evidence": [],
                }

        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640", "product_name": "Tempoplex-Abdeckhaube 6964.0", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeInheritanceConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        paired = products[products["promotion_reason"] == "tray_base_with_cover_pairing"]
        self.assertGreaterEqual(len(paired), 1)
        row = paired.iloc[0]
        self.assertEqual(row["flow_rate_lps"], 0.72)
        self.assertEqual(row["outlet_dn"], "DN50")
        self.assertEqual(row["flow_rate_raw_text"], "Ablaufleistung 0,72 l/s")
        self.assertIn("viega-69631", row["matched_component_ids"])
        self.assertIn("viega-69640", row["matched_component_ids"])
        inh = evidence[evidence["label"] == "paired_product_inheritance_applied_count"]["snippet"].tolist()
        self.assertTrue(inh and int(inh[0]) >= 1)
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)

    def test_viega_cleanup_preserves_products_and_cleans_bom_and_accessories(self):
        class _FakeCleanupConnector(_FakeViegaConnector):
            @staticmethod
            def extract_parameters(url):
                base = _FakeViegaConnector.extract_parameters(url)
                if "abdeckhaube-6964-0" in url.lower():
                    base["article_rows_json"] = (
                        '[{"article_no":"649 982","variant_label":"Kunststoff verchromt","_row_text":"Kunststoff verchromt 649 982"},'
                        '{"article_no":"806 132","variant_label":"Kunststoff schwarz matt","_row_text":"Kunststoff schwarz matt 806 132"}]'
                    )
                return base

            @staticmethod
            def get_bom_options(url, params=None):
                return [
                    {"option_group": "cover_variant", "option_label": "Kunststoff verchromt", "option_sku": "649 982", "option_meta": "clean row 649 982"},
                    {"option_group": "cover_variant", "option_label": "Kunststoff verchromt", "option_sku": "649982", "option_meta": "clean row 649 982"},
                    {"option_group": "cover_variant", "option_label": "wishlist plus minus", "option_sku": "775 070", "option_meta": "in den warenkorb menge wishlist " * 20},
                ]

        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-69631", "product_name": "Tempoplex-Ablauf 6963.1", "product_url": "https://v.example/Tempoplex-Ablauf-6963-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-69640", "product_name": "Tempoplex-Abdeckhaube 6964.0", "product_url": "https://v.example/Tempoplex-Abdeckhaube-6964-0.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex"},
                {"manufacturer": "viega", "product_id": "viega-acc-1", "product_name": "Montageset 123", "product_url": "https://v.example/Montageset-123.html", "candidate_type": "component", "complete_system": "yes", "system_role": "component", "discovery_seed_family": "tempoplex"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeCleanupConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

        paired_ids = set(products[products["promotion_reason"] == "tray_base_with_cover_pairing"]["product_id"].tolist())
        self.assertIn("viega-69631__649982", paired_ids)
        self.assertIn("viega-69631__806132", paired_ids)
        self.assertIn("viega-69631__775070", paired_ids)
        self.assertIn("viega-69631__775087", paired_ids)
        self.assertIn("viega-69631__775094", paired_ids)

        acc = products[products["product_id"] == "viega-acc-1"].iloc[0]
        self.assertEqual(acc["system_role"], "accessory")
        self.assertEqual(acc["promote_to_product"], "no")

        self.assertEqual(len(bom[(bom["product_id"] == "viega-69640") & (bom["option_sku"] == "649982")]), 1)
        self.assertFalse(any("warenkorb" in str(x).lower() for x in bom["option_meta"].tolist()))
        dedup = evidence[evidence["label"] == "bom_options_deduplicated_count"]["snippet"].tolist()
        removed = evidence[evidence["label"] == "malformed_bom_options_removed_count"]["snippet"].tolist()
        self.assertTrue(dedup and int(dedup[0]) >= 1)
        self.assertTrue(removed and int(removed[0]) >= 1)
        self.assertTrue(excluded.empty)

    def test_domoplex_and_tempoplex_plus_variant_rows_emit_per_variant_products(self):
        class _FakeMultiFamilyVariantConnector(_FakeViegaConnector):
            @staticmethod
            def extract_parameters(url):
                base = _FakeViegaConnector.extract_parameters(url)
                u = url.lower()
                if "domoplex-abdeckhaube" in u:
                    base["article_rows_json"] = '[{"article_no":"123 456","variant_label":"Domoplex Chrom","_row_text":"Domoplex Chrom 123 456"}]'
                if "tempoplex-plus-abdeckhaube" in u:
                    base["article_rows_json"] = '[{"article_no":"654 321","variant_label":"Plus Schwarz","_row_text":"Plus Schwarz 654 321"}]'
                return base

        registry = pd.DataFrame(
            [
                {"manufacturer": "viega", "product_id": "viega-692821", "product_name": "Domoplex-Ablauf 6928.21", "product_url": "https://v.example/Domoplex-Ablauf-6928-21.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "domoplex"},
                {"manufacturer": "viega", "product_id": "viega-domo-cover", "product_name": "Domoplex-Abdeckhaube passend für 6928.21", "product_url": "https://v.example/Domoplex-Abdeckhaube-9999-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "domoplex"},
                {"manufacturer": "viega", "product_id": "viega-plus-base", "product_name": "Tempoplex-Plus-Ablauf 1111.11", "product_url": "https://v.example/Tempoplex-Plus-Ablauf-1111-11.html", "candidate_type": "component", "complete_system": "yes", "system_role": "base_set", "discovery_seed_family": "tempoplex_plus"},
                {"manufacturer": "viega", "product_id": "viega-plus-cover", "product_name": "Tempoplex-Plus-Abdeckhaube 1111.11", "product_url": "https://v.example/Tempoplex-Plus-Abdeckhaube-1111-1.html", "candidate_type": "component", "complete_system": "yes", "system_role": "cover", "discovery_seed_family": "tempoplex_plus"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"viega": _FakeMultiFamilyVariantConnector()}, clear=True):
            products, _comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())
        paired_ids = set(products[products["promotion_reason"] == "tray_base_with_cover_pairing"]["product_id"].tolist())
        self.assertIn("viega-692821__123456", paired_ids)
        self.assertIn("viega-plus-base__654321", paired_ids)
        dcnt = evidence[evidence["label"] == "domoplex_cover_variant_rows_parsed_count"]["snippet"].tolist()
        pcnt = evidence[evidence["label"] == "tempoplex_plus_cover_variant_rows_parsed_count"]["snippet"].tolist()
        self.assertTrue(dcnt and int(dcnt[0]) >= 1)
        self.assertTrue(pcnt and int(pcnt[0]) >= 1)
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)


    def test_exported_xlsx_mplus_final_assemblies_are_blocked_and_validate(self):
        from tools import validate_xlsx_export

        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "mplus_export.xlsx"
            self._make_template(template)

            products = pd.DataFrame([{"manufacturer": "aco", "product_id": "aco-regular-product"}])
            export_excel(template, out, default_config(), products_df=products, comparison_df=products)

            with pd.ExcelFile(out, engine="openpyxl") as xls:
                final_assemblies = pd.read_excel(xls, sheet_name="Final_Assemblies")
            mplus = final_assemblies[
                final_assemblies["product_id"].fillna("").astype(str).str.startswith("aco-assembled-showerdrain-mplus-")
            ]
            self.assertEqual(len(mplus), 4)
            self.assertTrue(mplus["assembled_from_bom"].eq(True).all())
            self.assertTrue(mplus["ready_for_benchmark"].eq(False).all())
            self.assertTrue(mplus["ready_for_customer_view"].eq(False).all())
            self.assertTrue((mplus["flow_rate_lps"].isna() | mplus["flow_rate_lps"].fillna("").eq("")).all())
            self.assertTrue(mplus["flow_rate_status"].eq("conditional").all())
            self.assertTrue(mplus["data_quality_status"].eq("conditional_parameter_available_production_blocked").all())
            self.assertTrue(mplus["blocked_reason"].str.contains("blocked_pending_conditional_parameter_scoring").all())

            wb = openpyxl.load_workbook(out, read_only=True, data_only=True)
            try:
                ws = wb["Final_Assemblies"]
                headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
                indexes = {name: headers.index(name) for name in [
                    "product_id",
                    "assembled_from_bom",
                    "ready_for_benchmark",
                    "ready_for_customer_view",
                    "flow_rate_status",
                    "data_quality_status",
                ]}
                raw_mplus_rows = []
                for row in ws.iter_rows(min_row=2, values_only=True):
                    product_id = str(row[indexes["product_id"]] or "")
                    if product_id.startswith("aco-assembled-showerdrain-mplus-"):
                        raw_mplus_rows.append({name: row[index] for name, index in indexes.items()})
            finally:
                wb.close()
            print("M+ Final_Assemblies raw workbook values:", raw_mplus_rows)
            self.assertEqual(len(raw_mplus_rows), 4)
            for raw_row in raw_mplus_rows:
                self.assertIs(raw_row["assembled_from_bom"], True)
                self.assertIs(raw_row["ready_for_benchmark"], False)
                self.assertIs(raw_row["ready_for_customer_view"], False)
                self.assertEqual(raw_row["flow_rate_status"], "conditional")
                self.assertEqual(raw_row["data_quality_status"], "conditional_parameter_available_production_blocked")

            counts = {
                "Products": 5,
                "Comparison": 5,
                "Scoring_Field_Coverage": 5,
                "Candidates_All": 0,
                "Components": 0,
                "BOM_Options": 0,
                "Final_Assemblies": 4,
                "Final_Set_Details": 4,
                "Mplus_Compound_Mappings": 4,
                "Eplus_Proposal_Mappings": 3,
                "Conditional_Technical_Values": 8,
                "Article_Variants": 0,
            }
            with (
                patch.object(validate_xlsx_export, "EXPECTED_SHEET_COUNTS", counts),
                patch.object(validate_xlsx_export, "COMPONENTS_MIN_ROWS", 0),
                patch.object(validate_xlsx_export, "EXPECTED_BOM_OPTION_TYPE_COUNTS", {"optional_accessory": 0, "compatible_grate": 0}),
                patch.object(validate_xlsx_export, "EXPECTED_ASSEMBLED_PREFIX_COUNTS", {
                    "aco-assembled-showerdrain-splus": 0,
                    "aco-assembled-showerdrain-c": 0,
                    "aco-assembled-showerdrain-mplus": 4,
                    "aco-assembled-showerdrain-eplus": 0,
                    "aco-assembled-showerdrain-b": 0,
                    "aco-assembled-showerdrain-cplus": 0,
                }),
                patch.object(validate_xlsx_export, "EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS", {"showerdrain_mplus": 4}),
                patch.object(validate_xlsx_export, "EXPECTED_FINAL_ASSEMBLIES_STATUS_COUNTS", {
                    "complete": 0,
                    "partial": 0,
                    "conditional_parameter_available_production_blocked": 4,
                    "missing": 0,
                }),
                patch.object(validate_xlsx_export, "EXPECTED_FINAL_SET_DETAILS_ROW_COUNT", 4),
                patch.object(validate_xlsx_export, "EXPECTED_FINAL_SET_DETAILS_FAMILY_COUNTS", {"showerdrain_mplus": 4}),
                patch.object(validate_xlsx_export, "EXPECTED_FINAL_SET_DETAILS_READY_COUNTS", {True: 0, False: 4}),
                patch.object(validate_xlsx_export, "CPLUS_EXPECTED", {}),
                patch.object(validate_xlsx_export, "EASYFLOW_WS50_DN50_EXPECTED_ARTICLES", set()),
            ):
                self.assertEqual(validate_xlsx_export.main([str(out)]), 0)

    def test_export_writes_final_assemblies_sheet_from_products_only(self):
        with tempfile.TemporaryDirectory() as td:
            template = Path(td) / "template.xlsx"
            out = Path(td) / "out.xlsx"
            self._make_template(template)

            products = pd.DataFrame([
                {
                    "manufacturer": "aco",
                    "product_id": "aco-assembled-easyflow-compact",
                    "product_name": "Easyflow assembled compact",
                    "water_seal_mm": 50.0,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": None,
                    "height_adj_min_mm": None,
                    "height_adj_max_mm": None,
                },
                {
                    "manufacturer": "aco",
                    "product_id": "aco-assembled-easyflowplus-variant",
                    "product_name": "Easyflow Plus assembled",
                    "water_seal_mm": 50.0,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 0.6,
                    "height_adj_min_mm": 65,
                    "height_adj_max_mm": 95,
                },
                {
                    "manufacturer": "aco",
                    "product_id": "aco-assembled-showerdrain-c-variant",
                    "product_name": "ShowerDrain C assembled",
                    "water_seal_mm": 50.0,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 0.8,
                    "height_adj_min_mm": 57,
                    "height_adj_max_mm": 128,
                },
                {
                    "manufacturer": "aco",
                    "product_id": "aco-assembled-showerdrain-splus-variant",
                    "product_name": "ShowerDrain S+ assembled",
                    "water_seal_mm": 50.0,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 0.8,
                    "height_adj_min_mm": 57,
                    "height_adj_max_mm": 128,
                },
                {
                    "manufacturer": "aco",
                    "product_id": "aco-assembled-unexpected-family",
                    "product_name": "Unexpected assembled",
                    "water_seal_mm": 50.0,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 0.8,
                    "height_adj_min_mm": 57,
                    "height_adj_max_mm": 128,
                },
                {
                    "manufacturer": "aco",
                    "product_id": "aco-regular-product",
                    "product_name": "Regular drain",
                    "water_seal_mm": 50.0,
                    "outlet_dn": "DN50",
                    "flow_rate_lps": 0.8,
                    "height_adj_min_mm": 57,
                    "height_adj_max_mm": 128,
                },
            ])

            export_excel(template, out, default_config(), products_df=products, comparison_df=products)

            wb = openpyxl.load_workbook(out, read_only=True, data_only=True)
            try:
                self.assertIn("Final_Assemblies", wb.sheetnames)
                self.assertIn("Final_Set_Details", wb.sheetnames)
                self.assertIn("Products", wb.sheetnames)
                self.assertIn("Comparison", wb.sheetnames)
            finally:
                wb.close()

            product_rows = self._sheet_rows(out, "Products")
            final_rows = self._sheet_rows(out, "Final_Assemblies")
            detail_rows = self._sheet_rows(out, "Final_Set_Details")
            self.assertEqual(len(product_rows) - 1, len(products) + 4)
            self.assertEqual(len(final_rows) - 1, 9)
            self.assertEqual(len(detail_rows) - 1, 9)

            headers = list(final_rows[0])
            self.assertIn("assembled_family", headers)
            self.assertIn("is_complete_technical_data", headers)
            self.assertIn("missing_technical_fields", headers)
            self.assertIn("data_quality_status", headers)
            self.assertIn("source_status_note", headers)
            for col in products.columns:
                self.assertIn(col, headers)

            final_df = pd.DataFrame(final_rows[1:], columns=headers)
            self.assertEqual(
                final_df["assembled_family"].tolist(),
                ["easyflow", "easyflowplus", "showerdrain_c", "showerdrain_splus", "unknown", "showerdrain_mplus", "showerdrain_mplus", "showerdrain_mplus", "showerdrain_mplus"],
            )
            self.assertNotIn("aco-regular-product", set(final_df["product_id"]))

            easyflow = final_df[final_df["assembled_family"] == "easyflow"].iloc[0]
            self.assertEqual(easyflow["water_seal_mm"], 50.0)
            self.assertEqual(easyflow["outlet_dn"], "DN50")
            self.assertTrue(pd.isna(easyflow["flow_rate_lps"]))
            self.assertTrue(pd.isna(easyflow["height_adj_min_mm"]))
            self.assertTrue(pd.isna(easyflow["height_adj_max_mm"]))
            self.assertEqual(easyflow["is_complete_technical_data"], False)
            self.assertEqual(
                easyflow["missing_technical_fields"],
                "flow_rate_lps,height_adj_min_mm,height_adj_max_mm",
            )
            self.assertEqual(easyflow["data_quality_status"], "partial")
            self.assertIn(
                "ambiguous at current article/variant granularity",
                easyflow["source_status_note"],
            )

            complete_rows = final_df[final_df["assembled_family"].isin(["easyflowplus", "showerdrain_c", "showerdrain_splus", "unknown"])]
            self.assertTrue((complete_rows["is_complete_technical_data"] == True).all())
            self.assertTrue((complete_rows["data_quality_status"] == "complete").all())
            self.assertTrue((complete_rows["missing_technical_fields"].fillna("") == "").all())

            detail_headers = list(detail_rows[0])
            detail_df = pd.DataFrame(detail_rows[1:], columns=detail_headers)
            self.assertEqual(set(detail_df["assembled_product_id"]), set(final_df["product_id"]))
            self.assertEqual(detail_df["ready_for_benchmark"].value_counts().to_dict(), {True: 4, False: 5})
            detail_easyflow = detail_df[detail_df["assembled_family"] == "easyflow"].iloc[0]
            self.assertEqual(detail_easyflow["set_id"], detail_easyflow["assembled_product_id"])
            self.assertEqual(detail_easyflow["ready_for_benchmark"], False)
            self.assertEqual(detail_easyflow["ready_for_customer_view"], False)
            self.assertEqual(
                detail_easyflow["blocked_reason"],
                "flow/height ambiguous at current article/variant granularity",
            )
            self.assertEqual(detail_easyflow["article_variant_status"], "multiple_candidate_articles")
            complete_detail_rows = detail_df[detail_df["assembled_family"].isin(["easyflowplus", "showerdrain_c", "showerdrain_splus", "unknown"])]
            self.assertTrue((complete_detail_rows["ready_for_benchmark"] == True).all())
            self.assertTrue((complete_detail_rows["ready_for_customer_view"] == True).all())
            self.assertTrue((complete_detail_rows["article_variant_status"] == "not_required").all())
            self.assertTrue((complete_detail_rows["blocked_reason"].fillna("") == "").all())
            mplus_rows = final_df[final_df["assembled_family"] == "showerdrain_mplus"]
            self.assertTrue((mplus_rows["ready_for_benchmark"] == False).all())
            self.assertTrue((mplus_rows["ready_for_customer_view"] == False).all())
            self.assertTrue((mplus_rows["product_family"] == "showerdrain_mplus").all())
            self.assertTrue((mplus_rows["family"] == "showerdrain_mplus").all())
            self.assertTrue((mplus_rows["assembled_from_bom"] == True).all())
            self.assertTrue((mplus_rows["flow_rate_status"] == "conditional").all())
            self.assertTrue((mplus_rows["flow_rate_lps"].fillna("") == "").all())
            mplus_detail_rows = detail_df[detail_df["assembled_family"] == "showerdrain_mplus"]
            self.assertTrue((mplus_detail_rows["ready_for_benchmark"] == False).all())
            self.assertTrue((mplus_detail_rows["ready_for_customer_view"] == False).all())
            self.assertTrue((mplus_detail_rows["blocked_reason"] == "blocked_pending_conditional_parameter_scoring").all())

if __name__ == "__main__":
    unittest.main()
