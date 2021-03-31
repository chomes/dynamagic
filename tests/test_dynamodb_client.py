import boto3
from moto import mock_dynamodb2
from dynamagic.dynamodb_client import DynamodbClient
import unittest

class TestDynamoDBClient(unittest.TestCase):
    @mock_dynamodb2
    @staticmethod
    def create_table():
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
    
    def test_validate_data(self):
        dynamodb_client: DynamodbClient = DynamodbClient(aws_region='eu-west-2',
         dynamodb_table='test_table')
        self.assertEqual(dynamodb_client.validate_data(validation_type='read_item',
         unvalidated_data={'CustomerId' :'2010482012'}), {'CustomerId' :'2010482012'})


if __name__ == '__main__':
    unittest.main()