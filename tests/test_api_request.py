# tests/test_api_request.py
import unittest
from lark.api_request import APIRequest
from lark.lark_authenticator import LarkAuthenticator

class TestAPIRequest(unittest.TestCase):

    def setUp(self):
        self.auth = LarkAuthenticator()
        self.api = APIRequest()

    def test_unsupported_method_raises_exception(self):
        with self.assertRaises(ValueError):
            self.api.send_request('UNSUPPORTED_METHOD', 'dummy_url', {})
