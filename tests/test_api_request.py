# tests/test_api_request.py
import unittest
from src.api_request import APIRequest

class TestAPIRequest(unittest.TestCase):

    def setUp(self):
        self.api = APIRequest()

    def test_unsupported_method_raises_exception(self):
        with self.assertRaises(ValueError):
            self.api.send_request('UNSUPPORTED_METHOD', 'dummy_url', {})
