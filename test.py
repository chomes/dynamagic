from modules.schema import Schema
from moto import mock_dynamodb2
from lambda_function import DynaMagic
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


class TestSchema(unittest.TestCase):

   

    def test_correct_keys(self):
        schema = Schema(data={"customerId": "1482328791", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(schema.keys_validator()["status_code"], 200)
    
    def test_too_many_keys(self):
        schema = Schema(data={"customerId": "1482328791", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda", "favourite_comics": ["Batman", "Flash", "Green Lantern"]})
        self.assertEqual(schema.keys_validator()["status_code"], 400)
    
    def test_too_few_keys(self):
        schema = Schema(data={"customerId": "1482328791", "name": "James Joseph", "address": "Jeff Bezos Candy land road"})
        self.assertEqual(schema.keys_validator()["status_code"], 400)


if __name__ == "__main__":
    unittest.main()
