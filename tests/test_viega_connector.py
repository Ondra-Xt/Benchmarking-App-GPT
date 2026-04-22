import json
import unittest
from unittest.mock import patch

from src.connectors import viega


class ViegaExtractionRegressionTests(unittest.TestCase):
    # golden classification samples (family-first + entity-type)
    GOLDEN_CLASSIFICATION = [
        # positive keep
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html", "Advantix-Duschrinne 4983.10", "complete_drain"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe/Advantix-Bodenablauf-1234-10.html", "Advantix-Bodenablauf 1234.10", "base_set"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Ablauf-6963-1.html", "Tempoplex-Ablauf 6963.1", "base_set"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Domoplex/Domoplex-Ablauf-1111-11.html", "Domoplex-Ablauf 1111.11", "base_set"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Rost-4933-61.html", "Advantix-Rost 4933.61", "cover"),
        # negative reject
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Dichtung-1111-11.html", "Tempoplex-Dichtung", "accessory"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Montageset-1111-11.html", "Tempoplex-Montageset", "accessory"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-O-Ring-1111-11.html", "Advantix-O-Ring", "accessory"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Glocke-1111-11.html", "Advantix-Glocke", "accessory"),
        ("https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Verstellfussset-1111-11.html", "Advantix-Verstellfußset", "accessory"),
    ]
    def _run_extract(self, slug: str, html_main: str):
        url = f"https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/{slug}.html"
        html = f"<html><body><main>{html_main}</main></body></html>"
        with patch.object(viega, "_safe_get_text", return_value=(200, url, html, "")):
            return viega.extract_parameters(url)

    def test_4983_10_complete_drain_extracts_flow_pdf_and_article_rows(self):
        params = self._run_extract(
            "Advantix-Duschrinne-4983-10",
            """
            <h1>Advantix Duschrinne 4983.10</h1>
            <a href="/docs/4983_10_datenblatt.pdf">Technische Daten / Datenblatt</a>
            Material Edelstahl 1.4301.
            Ablaufleistung Anstauhöhe 10 mm 0,5 l/s.
            Ablaufleistung Anstauhöhe 20 mm 0,55 l/s.
            güteüberwacht nach DIN EN 1253.
            Ablauf DN40 drehbar, Übergangsstück auf DN50.
            <table>
              <tr><th>L</th><th>Artikel</th></tr>
              <tr><td>1200</td><td>4983.10</td></tr>
            </table>
            """,
        )
        self.assertTrue(params["pdf_url"].endswith(".pdf"))
        self.assertIsNotNone(params["article_rows_json"])
        self.assertEqual(params["flow_rate_lps"], 0.55)
        self.assertEqual(params["flow_rate_lps_10mm"], 0.5)
        self.assertEqual(params["flow_rate_lps_20mm"], 0.55)
        self.assertEqual(params["outlet_dn_default"], "DN50")

    def test_4981_10_rich_table_maps_bh_dn_and_colours(self):
        params = self._run_extract(
            "Advantix-Cleviva-Duschrinne-4981-10",
            """
            <h1>Advantix Cleviva-Duschrinne 4981.10</h1>
            Material Edelstahl 1.4301.
            Ablaufleistung Anstauhöhe 10 mm 0,5-0,65 l/s.
            Ablaufleistung Anstauhöhe 20 mm 0,55-0,7 l/s.
            Abdichtungsmanschette werkseitig vormontiert.
            <table>
              <tr><th>L</th><th>BH</th><th>DN</th><th>Ausführung</th><th>VE</th><th>Artikel</th></tr>
              <tr><td>1200</td><td>70-95</td><td>DN 50</td><td>Edelstahl</td><td>1</td><td>4981.10</td></tr>
            </table>
            """,
        )
        self.assertEqual(params["resolved_length_mm"], 1200)
        self.assertEqual(params["height_adj_min_mm"], 70)
        self.assertEqual(params["height_adj_max_mm"], 95)
        self.assertEqual(params["outlet_dn"], "DN50")
        self.assertEqual(params["sealing_fleece_preassembled"], "yes")
        self.assertEqual(params["colours_count"], 1)

    def test_4982_10_base_body_is_component_candidate(self):
        self.assertEqual(
            viega._classify_candidate(
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen-Grundkoerper-4982-10.html",
                "Advantix-Duschrinnen-Grundkörper 4982.10",
                "Grundkörper mit L und Artikel",
            ),
            "component",
        )

    def test_4981_90_vertical_outlet_hint(self):
        params = self._run_extract(
            "Advantix-Cleviva-Duschrinnen-Ablauf-4981-90",
            """
            <h1>Advantix Cleviva-Duschrinnen-Ablauf 4981.90</h1>
            senkrecht.
            Ablaufleistung Anstauhöhe 10 mm 0,5 l/s.
            Ablaufleistung Anstauhöhe 20 mm 0,7 l/s.
            <table>
              <tr><th>DN</th><th>Artikel</th></tr>
              <tr><td>DN 50</td><td>4981.90</td></tr>
            </table>
            """,
        )
        self.assertEqual(params["outlet_direction_hint"], "vertical")
        self.assertEqual(params["flow_rate_lps"], 0.7)
        self.assertEqual(params["outlet_dn_default"], "DN50")

    def test_tempoplex_is_classified_as_base_set_taxonomy(self):
        cand_type, drain_category, system_role, complete_system = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Badewannen-und-Duschwannenablaeufe/Tempoplex/Tempoplex-Ablauf-6963-1.html",
            "Tempoplex-Ablauf 6963.1",
            "Tempoplex Duschwannenablauf",
        )
        self.assertEqual(cand_type, "component")
        self.assertEqual(drain_category, "shower_tray_drain")
        self.assertEqual(system_role, "base_set")
        self.assertEqual(complete_system, "component")

    def test_taxonomy_examples_for_floor_cover_accessory(self):
        c1 = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe/Advantix-Bodenablauf-1234-10.html",
            "Advantix Bodenablauf 1234.10",
            "Bodenablauf",
        )
        self.assertEqual(c1[1], "floor_drain")

        c2 = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Rost-9999-10.html",
            "Rost für Advantix-Duschrinne",
            "Rost",
        )
        self.assertEqual(c2[2], "cover")
        self.assertEqual(c2[0], "component")

        c3 = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Zubehoer/Tool-1111-10.html",
            "Montagewerkzeug",
            "Zubehör",
        )
        self.assertEqual(c3[1], "accessory")
        self.assertEqual(c3[2], "accessory")

    def test_discovery_spans_advantix_and_tempoplex_seeds(self):
        tempoplex_url = "https://www.viega.de/de/produkte/Katalog/Badewannen-und-Duschwannenablaeufe/Tempoplex/Tempoplex-Ablauf-6963-1.html"
        advantix_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html"

        def fake_get(url, timeout=35):
            if url == tempoplex_url:
                html = "<html><body><main><h1>Tempoplex-Ablauf 6963.1</h1></main></body></html>"
                return 200, url, html, ""
            if url == advantix_url:
                html = "<html><body><main><h1>Advantix Duschrinne 4983.10</h1></main></body></html>"
                return 200, url, html, ""
            return 200, url, "<html><body><a href='x'>seed</a></body></html>", ""

        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(
            viega,
            "_crawl_category_pages",
            return_value={
                tempoplex_url: {
                    "raw_discovered_href": "/de/produkte/Katalog/Badewannen-und-Duschwannenablaeufe/Tempoplex/Tempoplex-Ablauf-6963-1.html",
                    "normalized_detail_url": tempoplex_url,
                    "href_source_page": "seed-page",
                    "was_synthetic_url": False,
                },
                advantix_url: {
                    "raw_discovered_href": "/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html",
                    "normalized_detail_url": advantix_url,
                    "href_source_page": "seed-page",
                    "was_synthetic_url": False,
                },
            },
        ):
            rows, _dbg = viega.discover_candidates(1200, 100)

        urls = {r["product_url"] for r in rows}
        self.assertIn(tempoplex_url, urls)
        self.assertTrue(any("advantix-duschrinne-4983-10.html" in u.lower() for u in urls))
        by_url = {r["product_url"]: r for r in rows}
        self.assertEqual(by_url[tempoplex_url]["drain_category"], "shower_tray_drain")
        adv_row = next(r for r in rows if "advantix-duschrinne-4983-10.html" in r["product_url"].lower())
        self.assertEqual(adv_row["drain_category"], "line_channel")
        self.assertIn("discovery_seed_family", by_url[tempoplex_url])
        self.assertFalse(by_url[tempoplex_url]["was_synthetic_url"])
        self.assertEqual(by_url[tempoplex_url]["normalized_detail_url"], tempoplex_url)
        summary = _dbg[-1]
        self.assertIn("canonical_seed_urls", summary)
        self.assertIn("discovered_category_links", summary)
        self.assertIn("discovered_detail_links", summary)
        self.assertIn("dead_seed_urls", summary)
        self.assertIn("accepted_product_links", summary)

    def test_real_href_is_preferred_over_synthetic_seed_url(self):
        real_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen/Advantix-Duschrinnen-Einbauhoehe-ab-95/Advantix-Duschrinne-4983-10.html"
        synthetic_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html"

        def fake_get(url, timeout=35):
            if url in {real_url, synthetic_url}:
                return 200, url, "<html><body><main><h1>Advantix Duschrinne 4983.10</h1></main></body></html>", ""
            return 200, url, "<html><body>seed</body></html>", ""

        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(
            viega,
            "_crawl_category_pages",
            return_value={
                real_url: {
                    "raw_discovered_href": "/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen/Advantix-Duschrinnen-Einbauhoehe-ab-95/Advantix-Duschrinne-4983-10.html",
                    "normalized_detail_url": real_url,
                    "href_source_page": "real-listing",
                    "was_synthetic_url": False,
                }
            },
        ), patch.object(viega, "DETAIL_SEEDS", [synthetic_url]):
            rows, _ = viega.discover_candidates(1200, 100)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["product_url"], real_url)
        self.assertFalse(rows[0]["was_synthetic_url"])

    def test_discovery_filters_spare_parts_and_unrelated_branches(self):
        good1 = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html"
        good2 = "https://www.viega.de/de/produkte/Katalog/Badewannen-und-Duschwannenablaeufe/Tempoplex/Tempoplex-Ablauf-6963-1.html"
        bad_spare = "https://www.viega.de/de/produkte/Katalog/Badewannen-und-Duschwannenablaeufe/Tempoplex/Tempoplex-Dichtung-6961-95.html"
        bad_unrel = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Rueckstauverschluesse/Rueckstauverschluss-1111-11.html"

        def fake_get(url, timeout=35):
            if url == good1:
                return 200, url, "<html><body><main><h1>Advantix-Duschrinne 4983.10</h1></main></body></html>", ""
            if url == good2:
                return 200, url, "<html><body><main><h1>Tempoplex-Ablauf 6963.1</h1></main></body></html>", ""
            if url == bad_spare:
                return 200, url, "<html><body><main><h1>Tempoplex-Dichtung 6961.95</h1></main></body></html>", ""
            if url == bad_unrel:
                return 200, url, "<html><body><main><h1>Rückstauverschluss 1111.11</h1></main></body></html>", ""
            return 200, url, "<html><body>seed</body></html>", ""

        crawl_map = {
            u: {"raw_discovered_href": u, "normalized_detail_url": u, "href_source_page": "seed", "was_synthetic_url": False}
            for u in [good1, good2, bad_spare, bad_unrel]
        }
        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(
            viega, "_crawl_category_pages", return_value=crawl_map
        ), patch.object(viega, "DETAIL_SEEDS", []):
            rows, dbg = viega.discover_candidates(1200, 100)

        urls = {r["product_url"] for r in rows}
        self.assertIn(good1, urls)
        self.assertIn(good2, urls)
        self.assertNotIn(bad_spare, urls)
        self.assertNotIn(bad_unrel, urls)
        summary = dbg[-1]
        self.assertGreaterEqual(summary["rejected_spare_parts_count"] + summary["rejected_accessory_gate_count"], 1)
        self.assertGreaterEqual(summary["rejected_unrelated_branch_count"], 1)

    def test_expected_classification_examples_and_spare_rejections(self):
        cleviva = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Cleviva-Duschrinnen/Einbauhoehe-ab-95-mm/Advantix-Cleviva-Duschrinne-4981-10.html",
            "Advantix Cleviva-Duschrinne 4981.10",
            "Duschrinne",
        )
        self.assertEqual(cleviva[1], "line_channel")
        self.assertEqual(cleviva[2], "complete_drain")

        adv_4983 = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen/Advantix-Duschrinnen-Einbauhoehe-ab-95/Advantix-Duschrinne-4983-10.html",
            "Advantix-Duschrinne 4983.10",
            "Duschrinne",
        )
        self.assertEqual(adv_4983[1], "line_channel")

        tempoplex = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Ablauf-6963-1.html",
            "Tempoplex-Ablauf 6963.1",
            "Duschwannengarnituren",
        )
        self.assertEqual(tempoplex[1], "shower_tray_drain")
        self.assertEqual(tempoplex[2], "base_set")

        floor = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe/Advantix-Bodenablauf-1234-10.html",
            "Advantix-Bodenablauf 1234.10",
            "Bodenablauf",
        )
        self.assertEqual(floor[1], "floor_drain")

        self.assertTrue(
            viega._is_spare_part_like(
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Dichtung-1111-11.html",
                "Tempoplex-Dichtung",
                "",
                "accessory",
            )
        )
        self.assertTrue(
            viega._is_spare_part_like(
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Sicherungsverschluss-2222-22.html",
                "Sicherungsverschluss",
                "",
                "accessory",
            )
        )
        self.assertTrue(
            viega._is_unrelated_branch(
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ersatzteile-fuer-Advantix-Systeme-und-Rueckstauverschluesse/Ersatzteil-3333-33.html"
            )
        )

    def test_verstellfussset_is_accessory_not_complete_drain(self):
        c = viega._derive_taxonomy(
            "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen-Verstellfussset-4982-90.html",
            "Advantix-Duschrinnen-Verstellfußset 4982.90",
            "Verstellfußset",
        )
        self.assertEqual(c[2], "accessory")
        self.assertEqual(c[0], "component")
        self.assertTrue(
            viega._is_mounting_accessory_like(
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen-Verstellfussset-4982-90.html",
                "Advantix-Duschrinnen-Verstellfußset 4982.90",
                "Verstellfußset",
            )
        )

    def test_category_link_filter_keeps_shower_families_and_drops_noise(self):
        html = """
        <html><body>
          <a href="/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen.html">line</a>
          <a href="/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe.html">floor</a>
          <a href="/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex.html">tempoplex</a>
          <a href="/de/produkte/Katalog/Entwaesserungstechnik/Badewannengarnituren/Multiplex.html">multiplex</a>
          <a href="/de/produkte/Katalog/Entwaesserungstechnik/Rotaplex.html">rotaplex</a>
          <a href="/de/produkte/entwaesserungstechnik/im-bad/highlight.html">highlight</a>
          <a href="/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen.html#anchor">anchor</a>
          <a href="/de/downloads/viega-zertifikat.pdf">pdf</a>
        </body></html>
        """
        links, stats, fam = viega._extract_category_links_from_sortiment(html, "https://www.viega.de")
        joined = " ".join(sorted(links)).lower()
        self.assertIn("advantix-duschrinnen", joined)
        self.assertIn("advantix-bodenablaeufe", joined)
        self.assertIn("tempoplex", joined)
        self.assertNotIn("multiplex", joined)
        self.assertNotIn("rotaplex", joined)
        self.assertGreaterEqual(stats["dropped_bathtub"], 2)
        self.assertGreaterEqual(stats["dropped_highlight"], 1)
        self.assertGreaterEqual(stats["dropped_anchor"], 1)
        self.assertIn("advantix_line", fam)

    def test_golden_family_first_entity_classification_samples(self):
        for url, title, expected_role in self.GOLDEN_CLASSIFICATION:
            fam = viega._classify_family(url, title, "", "")
            role = viega._classify_entity_type(url, title, "", fam)
            self.assertEqual(role, expected_role, msg=f"{url} classified as {role}, expected {expected_role}")

    def test_golden_overrides_keep_critical_pages_non_accessory(self):
        cases = [
            (
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen/Advantix-Duschrinnen-Einbauhoehe-ab-95/Advantix-Duschrinne-4983-10.html",
                "Advantix-Duschrinne 4983.10",
                "advantix_line",
            ),
            (
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe/Abdichtung-konventionell/Brandschutz-R120/Advantix-Bodenablauf-4951-20.html",
                "Advantix-Bodenablauf 4951.20",
                "advantix_floor",
            ),
            (
                "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Ablauf-6963-1.html",
                "Tempoplex-Ablauf 6963.1",
                "tempoplex",
            ),
        ]
        noisy_flat = "Ersatzteil Wartung Technische Daten EN 1253 Ablaufleistung 0,8 l/s DN 50"
        for url, title, family in cases:
            role, reason, _pos, _neg = viega._classify_entity_type_with_reason(url, title, noisy_flat, family)
            self.assertIn(reason, {"golden_url_override_line_drain", "golden_url_override_floor_drain", "golden_url_override_base_set", "golden_url_override_shower_tray_base_set", "tray_ablauf_base_unit_default"})
            self.assertNotEqual(role, "accessory")

    def test_good_drain_pages_ignore_accessory_words_in_surrounding_flat_text(self):
        noisy_flat = "Empfohlenes Zubehör: Verstellfußset, Stopfen, Montageset"
        line_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html"
        floor_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe/Advantix-Bodenablauf-4951-20.html"
        for url, title, fam in [
            (line_url, "Advantix-Duschrinne 4983.10", "advantix_line"),
            (floor_url, "Advantix-Bodenablauf 4951.20", "advantix_floor"),
        ]:
            role, _reason, _pos, _neg = viega._classify_entity_type_with_reason(url, title, noisy_flat, fam)
            self.assertNotEqual(role, "accessory")

    def test_true_accessory_remains_accessory(self):
        url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Verstellfussset-1111-11.html"
        fam = viega._classify_family(url, "Advantix-Verstellfußset", "", "")
        role, reason, _pos, _neg = viega._classify_entity_type_with_reason(url, "Advantix-Verstellfußset", "Montageset", fam)
        self.assertEqual(role, "accessory")
        self.assertEqual(reason, "strong_negative_accessory_match")

    def test_accessory_gate_blocks_obvious_service_parts(self):
        for title in [
            "Advantix-Abdichtungsmanschette",
            "Advantix-Tauchrohrset",
            "Advantix-Montagekleber",
            "Advantix-Abdichtungsband",
            "Advantix-Reduzierstück",
            "Advantix-Einleger",
        ]:
            url = f"https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/{title.replace(' ', '-')}-1111-11.html"
            fam = viega._classify_family(url, title, "", "")
            role, _reason, _pos, _neg = viega._classify_entity_type_with_reason(url, title, "Technische Daten", fam)
            cat = viega._drain_category_from_family_and_text(fam, f"{url} {title}", role)
            self.assertTrue(viega._is_strict_accessory_gate_hit(url, title, "", role, cat))

    def test_known_golden_parameter_rescue_floor_flow_and_tempoplex_dn(self):
        floor_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Bodenablaeufe/Advantix-Bodenablauf-4951-20.html"
        tray_url = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Ablauf-6963-1.html"
        floor_html = "<html><body><main><h1>Advantix-Bodenablauf 4951.20</h1><p>Leistung 0,6 l/s</p></main></body></html>"
        tray_html = "<html><body><main><h1>Tempoplex-Ablauf 6963.1</h1><p>Ablauf für Duschwanne</p></main></body></html>"

        def fake_get(url, timeout=35):
            if "4951-20" in url:
                return 200, url, floor_html, ""
            if "6963-1" in url:
                return 200, url, tray_html, ""
            return 404, url, "", "not found"

        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(viega, "_extract_pdf_candidates", return_value=[]):
            floor = viega.extract_parameters(floor_url)
            tray = viega.extract_parameters(tray_url)
        self.assertIsNotNone(floor["flow_rate_lps"])
        self.assertEqual(tray["outlet_dn_default"], "DN50")

    def test_cover_is_suppressed_when_base_or_drain_exists_in_same_family(self):
        drain = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinne-4983-10.html"
        base = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Duschrinnen-Grundkoerper-4982-10.html"
        cover = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Rost-4933-61.html"

        def fake_get(url, timeout=35):
            if url == drain:
                return 200, url, "<html><body><main><h1>Advantix-Duschrinne 4983.10</h1></main></body></html>", ""
            if url == base:
                return 200, url, "<html><body><main><h1>Advantix-Duschrinnen-Grundkörper 4982.10</h1></main></body></html>", ""
            if url == cover:
                return 200, url, "<html><body><main><h1>Advantix-Rost 4933.61</h1></main></body></html>", ""
            return 200, url, "<html><body>seed</body></html>", ""

        crawl_map = {
            u: {"raw_discovered_href": u, "normalized_detail_url": u, "href_source_page": "seed", "was_synthetic_url": False}
            for u in [drain, base, cover]
        }
        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(
            viega, "_crawl_category_pages", return_value=crawl_map
        ), patch.object(viega, "DETAIL_SEEDS", []):
            rows, dbg = viega.discover_candidates(1200, 100)

        urls = {r["product_url"] for r in rows}
        self.assertIn(drain, urls)
        self.assertIn(base, urls)
        self.assertNotIn(cover, urls)
        self.assertEqual(dbg[-1]["accepted_cover_count"], 0)

    def test_tray_family_keeps_cover_for_pairing(self):
        base = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Ablauf-6963-1.html"
        cover = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Ablaeufe-fuer-Bade--und-Duschwannen/Tempoplex/Tempoplex-Abdeckhaube-6964-0.html"

        def fake_get(url, timeout=35):
            if url == base:
                return 200, url, "<html><body><main><h1>Tempoplex-Ablauf 6963.1</h1><p>Funktionseinheit ohne Abdeckhaube</p></main></body></html>", ""
            if url == cover:
                return 200, url, "<html><body><main><h1>Tempoplex-Abdeckhaube 6964.0</h1></main></body></html>", ""
            return 200, url, "<html><body>seed</body></html>", ""

        crawl_map = {
            u: {"raw_discovered_href": u, "normalized_detail_url": u, "href_source_page": "seed", "was_synthetic_url": False}
            for u in [base, cover]
        }
        with patch.object(viega, "_safe_get_text", side_effect=fake_get), patch.object(
            viega, "_crawl_category_pages", return_value=crawl_map
        ), patch.object(viega, "DETAIL_SEEDS", []):
            rows, dbg = viega.discover_candidates(1200, 100)

        urls = {r["product_url"] for r in rows}
        self.assertIn(base, urls)
        self.assertIn(cover, urls)
        self.assertGreaterEqual(dbg[-1]["accepted_cover_count"], 1)


if __name__ == "__main__":
    unittest.main()
