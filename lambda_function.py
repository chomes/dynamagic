import boto3
import botostubs


class DynaMagic():
    def __init__(self, table: str):
        self.table = table
        self.client: botostubs.DynamoDB = boto3.client("dynamodb", region_name="eu-west-2")

    def create_table(self) -> bool:
        """
        Creates a new table in DynamoDB for users to input their data into
        Return: prints a response and returns boolean
        """
        validate_existing_table = self.client.list_tables()["TableNames"]
        if not self.table in validate_existing_table:
            pass
        else:
            return {"status_code": 400, "message": f"Table {self.table} already exists, not creating"}

        create_table = self.client.create_table(
            TableName=self.table,
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
        self.client.get_waiter("table_exists").wait(TableName=self.table,
        WaiterConfig={"Delay": 5, "MaxAttempts": 5})
        tables = self.client.list_tables()["TableNames"]
        if create_table["TableDescription"]["TableName"] in tables:
            return {"status_code": 200, 
            "message": f"Table {create_table['TableDescription']['TableName']} has been created!"}
        else:
            return {"status_code": 400, 
            "message": f"Table {create_table['TableDescription']['TableName']} did not create, please try again"}