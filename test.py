from modules.schema import Schema
from moto import mock_dynamodb2
from lambda_function import DynaMagic
from typing import List
import unittest


class TestDynamoDb(unittest.TestCase):
    
    @mock_dynamodb2
    def test_create_table(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        self.assertEqual(dynamo.create_table(), {"status_code": 200, 
            "message": "Table test_table has been created!"})
    
    @mock_dynamodb2
    def test_creating_duplicate_table(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        self.assertEqual(dynamo.create_table(), {"status_code": 400, "message": "Table test_table already exists, not creating"})
    
    @mock_dynamodb2
    def test_table_exists(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        self.assertEqual(dynamo.validate_table_exists(), {"status_code": 200, "message": "This table exists"})
    
    @mock_dynamodb2
    def test_table_is_invalid(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        self.assertEqual(dynamo.validate_table_exists(), {"status_code": 400, "message": "The table doesn't exist, please try again with the correct name"})

    @mock_dynamodb2
    def test_adding_item(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        self.assertEqual(dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"}), {"status_code": 200,
            "message": "The items were added successfully!"})
                
    @mock_dynamodb2
    def test_updating_item(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(dynamo.update_item({"CustomerId": "1482328791", "age": "42", "car": "Blue Mercedes"}), 
        {"status_code": 200, "message": "Updated the items successfully"})
    
    @mock_dynamodb2
    def test_invalid_item(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(dynamo.update_item({"CustomerId": "4241848170", "age": "42", "car": "Blue Mercedes"}), 
        {"status_code": 400, "message": "Failed to retrieve the item, please check the key and try again"})
    
    @mock_dynamodb2
    def test_updating_same_data(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(dynamo.update_item(data={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32"}), {"status_code": 400, "message": "There is no new data to process, please provide new information and try again"})

    @mock_dynamodb2
    def test_deleting_item(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(dynamo.delete_item(key="1482328791"), {"status_code": 200, "message": "The item has been deleted successfully"})
    
    @mock_dynamodb2
    def test_wrong_key_deleting_item(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(dynamo.delete_item(key="1482328800"), {"status_code": 400, "message": "The item does not exist, please check the ID and try again"})
    
    @mock_dynamodb2
    def test_getting_table_items(self):
        dynamo = DynaMagic(table="test_table", region="eu-west-2")
        dynamo.create_table()
        dynamo.add_item(new_item={"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        dynamo.add_item(new_item={"CustomerId": "1482324001", "name": "James Joseph", "address": "Oscar Wild landing view",
    "age": "40", "car": "Blue BMW"})
        self.assertEqual(dynamo.get_items(), 
        [{"CustomerId": "1482328791", "name": "John Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"}, {"CustomerId": "1482324001", "name": "James Joseph", "address": "Oscar Wild landing view",
    "age": "40", "car": "Blue BMW"}])

    
    
class TestSchema(unittest.TestCase):

    def test_correct_keys(self):
        schema = Schema(data={"CustomerId": "1482328791", "name": "James Joseph", "address": "Jeff Bezos Candy land road",
    "age": "32", "car": "Black Skoda"})
        self.assertEqual(schema.keys_validator(), {"status_code": 200, "message": "Keys Validated"})

    def test_modular_keys(self):
        schema = Schema(data={"CustomerId": "1482328791", "name": "James Joseph", "age": "42"})
        self.assertEqual(schema.keys_validator(full_validate=False), {"status_code": 200, "message": "Keys Validated"})

    def test_invalid_modular_keys(self):
        schema = Schema(data={"CustomerId": "1482328791", "name": "James Joseph", "age": "42", "comics": ["Flash", "batman"]})
        self.assertEqual(schema.keys_validator(full_validate=False)["status_code"], 400 )

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
    
    def test_id_is_not_number(self):
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
