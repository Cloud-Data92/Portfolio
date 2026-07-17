"""Unit tests for the ETL transform layer. Run: python3 -m unittest test_etl.py -v"""

import unittest
from datetime import date

from etl import parse_date, parse_price, parse_quantity, normalize_store, transform_row


class TestParseDate(unittest.TestCase):
    def test_iso_format(self):
        self.assertEqual(parse_date("2025-03-14"), date(2025, 3, 14))

    def test_us_format(self):
        self.assertEqual(parse_date("03/14/2025"), date(2025, 3, 14))

    def test_day_month_format(self):
        self.assertEqual(parse_date("14-Mar-2025"), date(2025, 3, 14))

    def test_garbage_raises(self):
        with self.assertRaises(ValueError):
            parse_date("last Tuesday")


class TestParsePrice(unittest.TestCase):
    def test_plain(self):
        self.assertEqual(parse_price("24.99"), 24.99)

    def test_dollar_sign(self):
        self.assertEqual(parse_price("$189.00"), 189.0)

    def test_whitespace(self):
        self.assertEqual(parse_price(" 8.49 "), 8.49)

    def test_negative_raises(self):
        with self.assertRaises(ValueError):
            parse_price("-5.00")


class TestParseQuantity(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(parse_quantity("3"), 3)

    def test_zero_raises(self):
        with self.assertRaises(ValueError):
            parse_quantity("0")

    def test_text_raises(self):
        with self.assertRaises(ValueError):
            parse_quantity("N/A")


class TestNormalizeStore(unittest.TestCase):
    def test_upper(self):
        self.assertEqual(normalize_store("ATLANTA"), "Atlanta")

    def test_lower(self):
        self.assertEqual(normalize_store("atlanta"), "Atlanta")

    def test_padded(self):
        self.assertEqual(normalize_store("  Marietta "), "Marietta")

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            normalize_store("   ")


class TestTransformRow(unittest.TestCase):
    def _row(self, **overrides):
        base = {
            "order_id": "50001", "order_date": "2025-02-01", "store": "SMYRNA",
            "sku": "SKU-1001", "product_name": "Wireless Mouse",
            "category": "Electronics", "quantity": "2", "unit_price": "$24.99",
        }
        base.update(overrides)
        return base

    def test_happy_path(self):
        out = transform_row(self._row())
        self.assertEqual(out["store"], "Smyrna")
        self.assertEqual(out["unit_price"], 24.99)
        self.assertEqual(out["order_date"], date(2025, 2, 1))

    def test_missing_sku_rejected(self):
        with self.assertRaises(ValueError):
            transform_row(self._row(sku=""))

    def test_bad_quantity_rejected(self):
        with self.assertRaises(ValueError):
            transform_row(self._row(quantity="-2"))


if __name__ == "__main__":
    unittest.main()
