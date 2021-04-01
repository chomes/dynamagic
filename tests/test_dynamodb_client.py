import boto3
from moto import mock_dynamodb2
from dynamagic.dynamodb_client import DynamodbClient
import unittest

class TestDynamoDBClient(unittest.TestCase):
    @staticmethod
    @mock_dynamodb2
    def create_table():
        client = boto3.client("dynamodb", region_name="eu-west-2")
        client.create_table(
                TableName="test_table",
            ProvisionedThroughput={
                "ReadCapacityUnits": 140,
                "WriteCapacityUnits": 140
            },
            AttributeDefinitions=[
                {
                    "AttributeName": "CustomerId",
                    "AttributeType": "S"
                }
            ],
            KeySchema=[
            {
                    "AttributeName": "CustomerId",
                    "KeyType": "HASH"
                } 
            ],
            BillingMode="PROVISIONED")
        client.get_waiter("table_exists").wait(TableName="test_table",
        WaiterConfig={"Delay": 2, "MaxAttempts": 5})
    
    def test_validate_data(self):
        dynamodb_client: DynamodbClient = DynamodbClient(aws_region="eu-west-2",
         dynamodb_table="test_table")
        self.assertEqual(dynamodb_client.validate_data(validation_type="read_item",
         unvalidated_data={"CustomerId" :"2010482012"}), {"CustomerId" :"2010482012"})

    @mock_dynamodb2
    def test_create_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(aws_region="eu-west-2",
        dynamodb_table="test_table")
        self.create_table()
        self.assertEqual(dynamodb_client.create_item(dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            }), {"status_code": 200,
             "message": "Created new item with key: 1482328791"})
    
    @mock_dynamodb2
    def test_failing_creating_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(aws_region="eu-west-2",
        dynamodb_table="test_table")
        self.create_table()
        self.assertEqual(dynamodb_client.create_item(dynamodb_item={
                "CustomerId": "148232879",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            }), {"status_code": 400,
             "message": "DynamoDb Key 'CustomerId' is either not 10 characters long or cannot convert to an int, please try again"})
    
    @mock_dynamodb2
    def test_delete_existing_attributes(self):
        dynamodb_client: DynamodbClient = DynamodbClient(aws_region="eu-west-2",
        dynamodb_table="test_table")
        self.create_table()
        dynamodb_client.create_item(dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            })
        self.assertEqual(dynamodb_client.delete_existing_attributes(key={"CustomerId": {"S": "1482328791"}}, validated_attributes={
            "address": "Foreign road",
            "age": "42",
            "car": "Black Skoda",
        }), {"address": "Foreign road",
            "age": "42"})
    
    def test_generating_expressions(self):
        dynamodb_client: DynamodbClient = DynamodbClient(aws_region="eu-west-2",
        dynamodb_table="test_table")
        self.assertEqual(dynamodb_client.generate_expressions(confirmed_new_attributes={
            "address": "Foreign road",
            "age": "42"}), ("SET #AD = :ad, #AG = :ag",
             {"#AD": "address", "#AG": "age"},
             {
                    ":ad": {"S": "Foreign road"},
                    ":ag": {"S": "42"},
                }))
        

if __name__ == "__main__":
    unittest.main()