from lambda_function import DynaMagic
from moto import mock_dynamodb2
import unittest


class TestDynamoDb(unittest.TestCase):
    
    @mock_dynamodb2
    def test_create_table(self):
        dynamo = DynaMagic("test_table")
        self.assertEqual(dynamo.create_table()["status_code"], 200)
    
    @mock_dynamodb2
    def test_creating_duplicate_table(self):
        dynamo = DynaMagic("test_table")
        dynamo.create_table()
        self.assertEqual(dynamo.create_table()["status_code"], 400)


if __name__ == "__main__":
    unittest.main()
