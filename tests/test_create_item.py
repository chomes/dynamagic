from dynamagic.modules.dynamodb.create_item import CreateItem
import boto3
from moto import mock_dynamodb2
import unittest


class TestCreateItem(unittest.TestCase):
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
        create_item = CreateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(create_item.add_item(dynamodb_item={'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}}), 
        {'status_code': 200, 'message': 'Item has been added successfully'})

    @mock_dynamodb2 
    def test_failing_to_create_item(self):
        self.create_table()
        create_item = CreateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(create_item.add_item(dynamodb_item={'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}}),
        {'status_code': 400, 'message': 'Failed to add the item, a key was not provided'})
