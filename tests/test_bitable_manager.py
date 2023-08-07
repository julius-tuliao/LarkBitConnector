# tests/test_bitable_manager.py
import unittest
from src.bitable_manager import BitableManager

class TestBitableManager(unittest.TestCase):

    def setUp(self):
        self.bitable = BitableManager()

    def test_process_dl_date_handles_alpha(self):
        result = self.bitable.process_dl_date("someAlphaString")
        self.assertEqual(result, 1568470400000)  # Default date for null date in CRM

    def test_process_dl_date_handles_valid_date(self):
        result = self.bitable.process_dl_date("2021-08-01")
        expected = 1627795200000  # Equivalent timestamp for 2021-08-01
        self.assertEqual(result, expected)


# python -m unittest discover tests
