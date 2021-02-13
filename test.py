from modules.schema import Schema
from moto import mock_dynamodb2
from lambda_function import DynaMagic
import unittest


class TestDynamoDb(unittest.TestCase):
    
    @mock_dynamodb2
    def test_create_table(self):
        dynamo = DynaMagic("test_table")
        self.assertEqual(dynamo.create_table(), {"status_code": 200, 
            "message": "Table test_table has been created!"})
    
    @mock_dynamodb2
    def test_creating_duplicate_table(self):
        dynamo = DynaMagic("test_table")
        dynamo.create_table()
        self.assertEqual(dynamo.create_table(), {"status_code": 400, "message": "Table test_table already exists, not creating"})
    
    @mock_dynamodb2
    def test_adding_item(self):
        dynamo = DynaMagic("test_table")
        dynamo.create_table()
        self.assertEqual(dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"}), {"status_code": 200,
            "message": "The items were added successfully!"})



class TestSchema(unittest.TestCase):

    def test_correct_keys(self):
        schema = Schema(data={"CustomerId": "1482328791", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(schema.keys_validator(), {"status_code": 200, "message": "Keys Validated"})
    
    def test_too_many_keys(self):
        schema = Schema(data={"CustomerId": "1482328791", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda", "favourite_comics": ["Batman", "Flash", "Green Lantern"]})
        self.assertEqual(schema.keys_validator()["status_code"], 400)
    
    def test_too_few_keys(self):
        schema = Schema(data={"CustomerId": "1482328791", "name": "James Joseph", "address": "Jeff Bezos Candy land road"})
        self.assertEqual(schema.keys_validator()["status_code"], 400)

    def test_id_too_short(self):
        schema = Schema(data={"CustomerId": "1482", "name": "James Joseph", "address": "Jeff Bezos Candy land road"})
        self.assertEqual(schema.data_entegrity(), 
        {"status_code": 400, "message": "The customer ID is shorter then 10 digits long, please try again"})
    
    def test_id_too_long(self):
        schema = Schema(data={"CustomerId": "1482328791288", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(schema.data_entegrity(), 
        {"status_code": 400, "message": "The customer ID is longer then 10 digits long, please try again"})
    
    def test_id_is_int(self):
        schema = Schema(data={"CustomerId": 1482328790, "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(schema.data_entegrity(),
         {"status_code": 400, "message": "The customer ID cannot be an int, please make this a string"})
    
    def test_id_is_number(self):
        schema = Schema(data={"CustomerId": "1482fallen", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(schema.data_entegrity(), {"status_code": 400, "message": "The CustomerId is not a number, please check this and make sure the data is correct"})

    def test_data_validation(self):
        schema = Schema(data={"CustomerId": "1482328790", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(schema.data_entegrity(), {"status_code": 200, "message": "Data has been validated"})
    
    def test_data_not_validated(self):
        schema = Schema(data={"CustomerId": "1482328790", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": 32, "car": "Black Skoda"})
        self.assertEqual(schema.data_entegrity()["status_code"], 400)


if __name__ == "__main__":
    unittest.main()
