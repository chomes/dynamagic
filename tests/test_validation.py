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
    
    @mock_dynamodb2
    def test_table_exists(self):
        self.create_table()
        validation: Validation = Validation(aws_region='eu-west-2')
        self.assertEqual(validation.validate_dynamodb_table_exists(dynamodb_table='test_table'), {'status_code': 200, 'message': 'This table exists'})
    
    @mock_dynamodb2
    def test_table_does_not_exist(self):
        validation: Validation = Validation(aws_region='eu-west-2')
        self.assertEqual(validation.validate_dynamodb_table_exists(dynamodb_table='test_table'), {'status_code': 400, 'message': 'The table does not exist, please try again with the correct name'})



if __name__ == '__main__':
    unittest.main()
