# tests/test_api_request.py
import unittest
from db.mysql_database import Database

class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.database = Database()
        


    def test_database_connect_select(self):
        
        if self.database.connect():
        # SELECT Query example
            leads = self.database.execute_select("Select * From leads Limit 1")
            self.assertIsNotNone(leads)
