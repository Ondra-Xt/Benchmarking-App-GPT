import unittest

from src import excel_export


class ExcelExportSanitizeTests(unittest.TestCase):
    def test_sanitize_removes_illegal_control_chars(self):
        dirty = "abc\x12\x16\x11def"
        self.assertEqual(excel_export._sanitize_excel_string(dirty), "abcdef")

    def test_sanitize_keeps_tab_newline_carriage_return(self):
        txt = "a\tb\nc\rd"
        self.assertEqual(excel_export._sanitize_excel_string(txt), txt)

    def test_to_excel_cell_sanitizes_json_text(self):
        val = {"x": "abc\x12def"}
        out = excel_export._to_excel_cell(val)
        self.assertIn("abcdef", out)
        self.assertNotIn("\x12", out)


if __name__ == "__main__":
    unittest.main()

