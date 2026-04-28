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

    def test_aco_role_based_promotion_splits_products_and_components(self):
        registry = pd.DataFrame(
            [
                {"manufacturer": "aco", "product_id": "aco-90108544", "product_name": "ACO ShowerDrain C 1200 mm (Artikel-Nr. 90108544)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108544/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108554", "product_name": "ACO ShowerDrain C 1200 mm (Artikel-Nr. 90108554)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108554/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108524", "product_name": "ACO ShowerDrain C 1000 mm (Artikel-Nr. 90108524)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108524/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-90108534", "product_name": "ACO ShowerDrain C 1000 mm (Artikel-Nr. 90108534)", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-90108534/", "candidate_type": "drain", "complete_system": "yes", "system_role": "drain_unit", "classification_reason": "article_row_variant"},
                {"manufacturer": "aco", "product_id": "aco-comp-showerpoint", "product_name": "ACO ShowerPoint", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/badablaeufe/aco-showerpoint/", "candidate_type": "component", "complete_system": "component", "system_role": "complete_system"},
                {"manufacturer": "aco", "product_id": "aco-comp-family", "product_name": "ACO ShowerDrain S+", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/", "candidate_type": "component", "complete_system": "component", "system_role": "configuration_family"},
                {"manufacturer": "aco", "product_id": "aco-comp-grate", "product_name": "ACO ShowerDrain C Designrost", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/designrost/", "candidate_type": "component", "complete_system": "component", "system_role": "grate"},
                {"manufacturer": "aco", "product_id": "aco-comp-accessory", "product_name": "ACO ShowerStep", "product_url": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/zubehoer/aco-showerstep/", "candidate_type": "component", "complete_system": "component", "system_role": "accessory"},
            ]
        )
        with patch.dict(pipeline.CONNECTORS, {"aco": _FakeAcoConnector()}, clear=True):
            products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

        drains = products[products["candidate_type"] == "drain"]
        components = products[products["candidate_type"] == "component"]
        self.assertTrue({"aco-90108544", "aco-90108554", "aco-90108524", "aco-90108534"}.issubset(set(drains["product_id"].tolist())))
        self.assertIn("aco-comp-showerpoint", set(drains["product_id"].tolist()))
        self.assertIn("aco-comp-family", set(components["product_id"].tolist()))
        self.assertIn("aco-comp-grate", set(components["product_id"].tolist()))
        self.assertIn("aco-comp-accessory", set(components["product_id"].tolist()))
        self.assertEqual(
            components.set_index("product_id").loc["aco-comp-family", "why_not_product_reason"],
            "configuration_family_not_final_product",
        )
        self.assertEqual(
            components.set_index("product_id").loc["aco-comp-grate", "why_not_product_reason"],
            "cover_only_component",
        )
        self.assertEqual(
            components.set_index("product_id").loc["aco-comp-accessory", "why_not_product_reason"],
            "accessory_only",
        )
        self.assertFalse(((components["promote_to_product"] == "yes") & (components["promotion_reason"] == "default")).any())
        self.assertTrue(excluded.empty)
        self.assertTrue(bom.empty)
        aco_summary_labels = set(evidence[evidence["manufacturer"] == "aco"]["label"].tolist())
        self.assertIn("aco_candidates_by_role", aco_summary_labels)
        self.assertIn("aco_products_by_role", aco_summary_labels)
        self.assertIn("aco_components_by_role", aco_summary_labels)

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
        self.assertFalse(comparison.empty)
        self.assertTrue(excluded.empty)
        self.assertIn("Viega promotion", evidence["label"].tolist())
        self.assertTrue(bom.empty)

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


if __name__ == "__main__":
    unittest.main()
