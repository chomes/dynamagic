import boto3
from moto import mock_dynamodb2
from dynamagic.dynamodb_client import DynamodbClient
from dynamagic.modules.exceptions import ValidationFailedAttributesUpdateError
import unittest

schema_template = {
    "key_name": "CustomerId",
    "key_type": str,
    "name": str,
    "address": str,
    "age": str,
    "car": str,
}


class TestDynamoDBClient(unittest.TestCase):
    @staticmethod
    def generate_schema_template():
        return {
            "key_name": "CustomerId",
            "key_type": str,
            "name": str,
            "address": str,
            "age": str,
            "car": str,
        }

    @staticmethod
    @mock_dynamodb2
    def create_table():
        client = boto3.client("dynamodb", region_name="eu-west-2")
        client.create_table(
            TableName="test_table",
            ProvisionedThroughput={"ReadCapacityUnits": 140, "WriteCapacityUnits": 140},
            AttributeDefinitions=[
                {"AttributeName": "CustomerId", "AttributeType": "S"}
            ],
            KeySchema=[{"AttributeName": "CustomerId", "KeyType": "HASH"}],
            BillingMode="PROVISIONED",
        )
        client.get_waiter("table_exists").wait(
            TableName="test_table", WaiterConfig={"Delay": 2, "MaxAttempts": 5}
        )

    def test_validate_data(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            dynamodb_client.validate_data(
                validation_type="read_item",
                unvalidated_data={"CustomerId": "2010482012"},
            ),
            {"CustomerId": "2010482012"},
        )

    @mock_dynamodb2
    def test_create_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        self.assertEqual(
            dynamodb_client.create_item(
                dynamodb_item={
                    "CustomerId": "1482328791",
                    "name": "James Joseph",
                    "address": "Jeff Bezos Candy land road",
                    "age": "32",
                    "car": "Black Skoda",
                }
            ),
            {"statusCode": 200, "body": "Created new item with key: 1482328791"},
        )

    @mock_dynamodb2
    def test_failing_creating_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        self.assertEqual(
            dynamodb_client.create_item(
                dynamodb_item={
                    "CustomerId": "148232879",
                    "address": "Jeff Bezos Candy land road",
                    "age": "32",
                    "car": "Black Skoda",
                }
            ),
            {
                "statusCode": 400,
                "body": f"Key 'name' was missing from the schema from this data, please try again",
            },
        )

    @mock_dynamodb2
    def test_delete_existing_attributes(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        dynamodb_client.create_item(
            dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            }
        )
        self.assertEqual(
            dynamodb_client.delete_existing_attributes(
                key={"CustomerId": {"S": "1482328791"}},
                validated_attributes={
                    "address": "Foreign road",
                    "age": "42",
                    "car": "Black Skoda",
                },
            ),
            {"address": "Foreign road", "age": "42"},
        )

    def test_generating_expressions(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            dynamodb_client.generate_expressions(
                confirmed_new_attributes={"address": "Foreign road", "age": "42"}
            ),
            (
                "SET #A = :a, #G = :g",
                {"#A": "address", "#G": "age"},
                {
                    ":a": {"S": "Foreign road"},
                    ":g": {"S": "42"},
                },
            ),
        )

    def test_confirm_item_updated(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.assertTrue(
            dynamodb_client.confirm_item_updated(
                update_response={
                    "Attributes": {"age": {"S": "32"}, "car": {"S": "Black Skoda"}}
                },
                confirmed_new_attributes={"age": "32", "car": "Black Skoda"},
            )
        )

    def test_confirm_item_raising_exception(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        with self.assertRaises(ValidationFailedAttributesUpdateError):
            dynamodb_client.confirm_item_updated(
                update_response={},
                confirmed_new_attributes={"age": "32", "car": "Black Skoda"},
            )

    @mock_dynamodb2
    def test_update_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        dynamodb_client.create_item(
            dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            }
        )
        self.assertEqual(
            dynamodb_client.update_item(
                dynamodb_attributes={
                    "CustomerId": "1482328791",
                    "age": "45",
                    "car": "Blue BMW",
                }
            ),
            {
                "statusCode": 200,
                "body": "Item with the key provided has been updated successfully",
            },
        )

    @mock_dynamodb2
    def test_failing_update_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            dynamodb_client.update_item(dynamodb_attributes={"Test_key": 4828201}),
            {
                "statusCode": 400,
                "body": "Key 'CustomerId' was missing from the schema from this data, please try again",
            },
        )

    @mock_dynamodb2
    def test_fetching_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        dynamodb_client.create_item(
            dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            }
        )
        self.assertEqual(
            dynamodb_client.fetch_item(key={"CustomerId": "1482328791"}),
            {
                "statusCode": 200,
                "body": {
                    "CustomerId": "1482328791",
                    "name": "James Joseph",
                    "address": "Jeff Bezos Candy land road",
                    "age": "32",
                    "car": "Black Skoda",
                },
            },
        )

    def test_failing_to_fetch_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            dynamodb_client.fetch_item(key={"Test_key": 4820203}),
            {
                "statusCode": 400,
                "body": "Key 'CustomerId' was missing from the schema from this data, please try again",
            },
        )

    @mock_dynamodb2
    def test_fetch_items(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        dynamodb_client.create_item(
            dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            }
        )
        dynamodb_client.create_item(
            dynamodb_item={
                "CustomerId": "1482322421",
                "name": "John Joseph",
                "address": "Hilltop Valley",
                "age": "37",
                "car": "Black Ford",
            }
        )
        self.assertEqual(
            dynamodb_client.fetch_items(),
            {
                "statusCode": 200,
                "body": [
                    {
                        "CustomerId": "1482328791",
                        "name": "James Joseph",
                        "address": "Jeff Bezos Candy land road",
                        "age": "32",
                        "car": "Black Skoda",
                    },
                    {
                        "CustomerId": "1482322421",
                        "name": "John Joseph",
                        "address": "Hilltop Valley",
                        "age": "37",
                        "car": "Black Ford",
                    },
                ],
            },
        )

    @mock_dynamodb2
    def test_failing_to_fetch_items(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            dynamodb_client.fetch_items(),
            {
                "statusCode": 400,
                "body": "Either the table does not exist or there are no items populated yet, "
                "please check and try again",
            },
        )

    @mock_dynamodb2
    def test_deleting_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        dynamodb_client.create_item(
            dynamodb_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            }
        )
        self.assertEqual(
            dynamodb_client.delete_item(key={"CustomerId": "1482328791"}),
            {
                "statusCode": 200,
                "body": "Item with key: 1482328791 has been deleted",
            },
        )

    @mock_dynamodb2
    def test_failing_to_delete_item(self):
        dynamodb_client: DynamodbClient = DynamodbClient(
            dynamodb_table="test_table", table_schema=self.generate_schema_template()
        )
        self.create_table()
        self.assertEqual(
            dynamodb_client.delete_item(key={"Name": "148232879"}),
            {
                "statusCode": 400,
                "body": "Key 'CustomerId' was missing from the schema from this data, please try again",
            },
        )


if __name__ == "__main__":
    unittest.main()