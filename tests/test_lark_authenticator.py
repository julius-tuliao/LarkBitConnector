# tests/test_lark_authenticator.py
import unittest
from src.lark_authenticator import LarkAuthenticator

class TestLarkAuthenticator(unittest.TestCase):

    def setUp(self):
        self.auth = LarkAuthenticator()

    def test_get_app_access_token_returns_token(self):
        token = self.auth.get_tenant_access_token()
        self.assertIsNotNone(token)
