from dynamagic.modules.validation import Validation

import boto3
from moto import mock_dynamodb2
import unittest

class TestValidation(unittest.TestCase):
    @mock_dynamodb2
    def create_table(self):
        client = boto3.client('dynamodb', region_name='eu-west-2')
        client.create_table(
                TableName='test_table',
            ProvisionedThroughput={
                'ReadCapacityUnits': 140,
                'WriteCapacityUnits': 140
            },
            AttributeDefinitions=[
                {
                    'AttributeName': 'CustomerId',
                    'AttributeType': 'S'
                }
            ],
            KeySchema=[
            {
                    'AttributeName': 'CustomerId',
                    'KeyType': 'HASH'
                } 
            ],
            BillingMode='PROVISIONED')
        client.get_waiter('table_exists').wait(TableName='test_table',
        WaiterConfig={'Delay': 2, 'MaxAttempts': 5})
    
    def test_creating_validation_class(self):
        validation = Validation()
        self.assertIsInstance(validation, Validation)
    
    def test_validation_class_empty_aws_region(self):
        validation = Validation()
        self.assertEqual(validation.aws_region, None)
    
    def test_validation_class_aws_region_string(self):
        validation = Validation(aws_region='eu-west-2')
        self.assertIsInstance(validation.aws_region, str)

    @mock_dynamodb2
    def test_validate_dynamodb_table_exists(self):
        self.create_table()
        validation: Validation = Validation(aws_region='eu-west-2')
        self.assertEqual(validation.validate_dynamodb_table_exists(dynamodb_table='test_table'), {'status_code': 200, 'message': 'This table exists'})
    
    @mock_dynamodb2
    def test_validate_dynamodb_table_does_not_exist(self):
        validation: Validation = Validation(aws_region='eu-west-2')
        self.assertEqual(validation.validate_dynamodb_table_exists(dynamodb_table='test_table'), {'status_code': 400, 'message': 'The table does not exist, please try again with the correct name'})
    
    def test_validate_dynamodb_item_exists(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_dynamodb_item_exists(table_item={'Item': {'CustomerId': {'S': '1029481090'}}, 'age': {'S': '32'}, 'address': {'S': '39 Candyland Road'}}),
        {'status_code': 200, 'message': 'This item exists'})
    def test_validation_dynamodb_item_does_not_exist(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_dynamodb_item_exists(table_item={'Response': '200'}), {'status_code': 400, 'message': 'This item does not exist'})
    def test_validation_schema_new_item(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validation_schema(validation_type='new_item'), validation.new_item_schema)
    
    def test_validation_schema_updated_item(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validation_schema(validation_type='update_item'), validation.update_item_schema)
    
    def test_validation_schema_delete_item(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validation_schema(validation_type='delete_item'), validation.dynamodb_key_schema)
    
    def test_failed_to_grab_validation_Schema(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validation_schema(validation_type='not a schema'), {'status_code': 400, 
        'message': 'You did not specify a validation type when using validation schema, please choose either new_item or updated_item'})
    
    def test_validate_item_data_entegrity_new_item(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_data_entegrity(dynnamodb_schema=validation.new_item_schema, 
        unvalidated_item={'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road',
    'age': '32', 'car': 'Black Skoda'}), {'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road',
    'age': '32', 'car': 'Black Skoda'})

    def test_validate_item_data_entegrity_update_item(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_data_entegrity(dynnamodb_schema=validation.update_item_schema,
        unvalidated_item={'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road'}),
        {'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road'})
    
    def test_validate_item_data_entegrity_delete_item(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_data_entegrity(dynnamodb_schema=validation.dynamodb_key_schema,
        unvalidated_item={'CustomerId': '1482328791'}), {'CustomerId': '1482328791'})
    
    def test_validate_item_data_entegrity_int_to_string(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_data_entegrity(dynnamodb_schema=validation.new_item_schema, 
        unvalidated_item={'CustomerId': 1482328791, 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road',
    'age': 32, 'car': 'Black Skoda'}), {'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road',
    'age': '32', 'car': 'Black Skoda'})

    def test_validate_item_data_entegrity_incorrect_key(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_data_entegrity(dynnamodb_schema=validation.update_item_schema,
        unvalidated_item={'CustomerId': '148232879', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road'}), 
        {'status_code': 400, 'message': 'Either the CustomerID is not 10 characters long or it is not in numerical form, please format and try again.'})
    
    def test_validate_item_data_entegrity_missing_key(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_data_entegrity(dynnamodb_schema=validation.new_item_schema, 
        unvalidated_item={'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road',
    'age': '32'}), {'status_code': 400, 'message': '''You are missing 'car' in your schema, please add it and try again'''})

    def test_validate_item_Data_entegrity_wrong_key(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_data_entegrity(dynnamodb_schema=validation.update_item_schema,
        unvalidated_item={'CustomerId': '1482328790', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road', 'comics': 'Batman'}), 
        {'status_code': 400, 'message': ''''comics' is not a key you can use, please try again'''})
    
    def test_validate_item_to_db_format(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_to_db_format(dynamodb_item={'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road',
        'age': '32', 'car': 'Black Skoda'}), {'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}})
    
    def test_validate_item_to_readable_format(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_item_to_readable_format(dynamodb_item={'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}}), {'CustomerId': '1482328791', 'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road',
        'age': '32', 'car': 'Black Skoda'})

    def test_validate_new_attributes_exist(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_new_attributes_exist(item_attributes={'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road'}),
        {'name': 'James Joseph', 'address': 'Jeff Bezos Candy land road'})
    
    def test_validate_new_attributes_do_not_exist(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_new_attributes_exist(item_attributes={}), {'status_code': 400, 
        'message': 'All data was the same, cancelling operation, please provide new data and update the item again'})
    
    def test_validate_attributes_updated(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_attributes_updated(response={'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}}, validated_new_attributes={'age': {'S': '32'}, 
        'car': {'S': 'Black Skoda'}}), {"status_code": 200, "message": "Updated the items successfully"})
    
    def test_validate_attributes_not_updated(self):
        validation: Validation = Validation()
        self.assertEqual(validation.validate_attributes_updated(response={'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}}, validated_new_attributes={'age': {'S': '35'}, 
        'car': {'S': 'Blue BMW'}}), {"status_code": 400, "message": "The update_item method did not succeed as expected, please trouble shoot."})


if __name__ == '__main__':
    unittest.main()
