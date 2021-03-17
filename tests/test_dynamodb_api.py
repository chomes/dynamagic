from dynamagic.modules.dynamodb_api import DynamodbApi
import boto3
from moto import mock_dynamodb2
import unittest

class TestDynamodbApi(unittest.TestCase):
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
    
    @mock_dynamodb2
    def test_create_item(self):
        self.create_table()
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.add_item(dynamodb_item={'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}}), 
        {'status_code': 200, 'message': 'Item has been added successfully'})

    @mock_dynamodb2 
    def test_failing_to_create_item(self):
        self.create_table()
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.add_item(dynamodb_item={'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}}),
        {'status_code': 400, 'message': 'Failed to add the item, a key was not provided'})

    def test_remove_duplicate_attributes(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.remove_duplicated_attributes(new_attributes={'address': 'Jeff Bezos Candy land road',
    'age': '32'}, old_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {'address': 'Jeff Bezos Candy land road',
    'age': '32'})

    def test_remove_duplicate_attributes_wrong_key(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.remove_duplicated_attributes(new_attributes={'address': 'Jeff Bezos Candy land road',
    'age': '32', 'comics': 'Batman'}, old_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})

    def test_remove_all_duplicate_attributes(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.remove_duplicated_attributes(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}, old_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {})

    def test_generate_update_expression(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.generate_update_expression(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}), 'SET #AD = :ad, #AG = :ag')

    def test_generate_update_expression_with_wrong_key(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.generate_update_expression(new_attributes={'address': 'Rochet trust drive',
    'age': '35', 'comic': 'Batman'}), {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})

    def test_generate_expression_attribute_names(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.generate_expression_attribute_names(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {'#AD': 'address', '#AG': 'age'})

    def test_generate_expression_attribute_names_with_wrong_key(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.generate_expression_attribute_names(new_attributes={'address': 'Rochet trust drive',
    'age': '35', 'comic': 'Batman'}), {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})

    def test_generate_expression_attribute_values(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.generate_expression_attribute_values(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}, dynamodb_validation_format_mapper={'CustomerId': {'dynamodb_type': 'S'}, 'name': {'dynamodb_type': 'S'},
        'address': {'dynamodb_type': 'S'},  'age': {'dynamodb_type': 'S'}, 'car': {'dynamodb_type': 'S'}}),
        {':ad': {'S': 'Rochet trust drive'}, ':ag': {'S': '35'}})
    
    def test_generate_expression_attribute_values_with_wrong_key(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(dynamodb_api.generate_expression_attribute_values(new_attributes={'address': 'Rochet trust drive',
    'age': '35', 'comic': 'Batman'}, dynamodb_validation_format_mapper={'CustomerId': {'dynamodb_type': 'S'}, 'name': {'dynamodb_type': 'S'},
        'address': {'dynamodb_type': 'S'},  'age': {'dynamodb_type': 'S'}, 'car': {'dynamodb_type': 'S'}}), 
        {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})
    
    @mock_dynamodb2
    def test_push_update(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.create_table()
        client = boto3.client('dynamodb', region_name='eu-west-2')
        client.put_item(TableName='test_table',
        Item={'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}})
        self.assertEqual(dynamodb_api.push_update(key={'CustomerId': {'S': '1482328791'}},
        update_expression='SET #AD = :ad, #AG = :ag',
        expression_attribute_names={'#AD': 'address', '#AG': 'age'},
        expression_attribute_values={':ad': {'S': 'Rochet trust drive'}, ':ag': {'S': '35'}})['Attributes'], {'address': {'S': 'Rochet trust drive'},
        'age': {'S': '35'}})

    @mock_dynamodb2    
    def test_get_item(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.create_table()
        client = boto3.client('dynamodb', region_name='eu-west-2')
        client.put_item(TableName='test_table',
        Item={'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}})
        self.assertEqual(dynamodb_api.get_item(key={'CustomerId': {'S': '1482328791'}}),
        {'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}})    

    @mock_dynamodb2
    def test_get_invalid_item(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.create_table()
        self.assertEqual(dynamodb_api.get_item(key={'CustomerId': {'S': '1482328791'}}), 
        {"status_code": 400, "message": "This key is invalid, please try again with a valid key"})
    
    @mock_dynamodb2
    def test_get_items(self):
        dynamodb_api: DynamodbApi = DynamodbApi(aws_region='eu-west-2', dynamodb_table='test_table')
        self.create_table()
        client = boto3.client('dynamodb', region_name='eu-west-2')
        client.put_item(TableName='test_table',
        Item={'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}})
        client.put_item(TableName='test_table',
        Item={'CustomerId': {'S': '1482328721'}, 'name': {'S': 'John Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '38'}, 'car': {'S': 'Blue BMW'}})
        self.assertEqual(dynamodb_api.get_items(), [{'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}},
        {'CustomerId': {'S': '1482328721'}, 'name': {'S': 'John Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '38'}, 'car': {'S': 'Blue BMW'}}])


    

        





if __name__ == '__main__':
    unittest.main()
