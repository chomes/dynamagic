import boto3
from modules.schema import Schema
from time import sleep

class DynaMagic():
    def __init__(self, table: str):
        self.table = table
        self.client = boto3.client("dynamodb", region_name="eu-west-2")

    def create_table(self) -> dict:
        """Creates a table for DynamoDB 

        Returns:
            dict: Response code of the action
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
    
    def add_item(self, new_item: dict) -> dict:
        """Creates a new item into the database, this data is validated before being added using the modules/schema.py
        Users must provide the exact fields in this schema for this to be added which are:
        CustomerId: 10 number string
        name: Full name of person
        address: Location of person
        age: Age of the person
        car: Type of car

        Args:
            new_item (dict): The required key, value fields in the description in dict form.

        Returns:
            dict: Response code of the result of the action
        """
        schema = Schema(data=new_item)
        key_validator = schema.keys_validator() 
        if key_validator["status_code"] == 400:
            return key_validator
        
        data_validator = schema.data_entegrity()
        if data_validator["status_code"] == 400:
            return data_validator
        
        validated_data = {
                "CustomerId": {"S": new_item["CustomerId"]},
                "name": {"S": new_item["name"]},
                "address": {"S": new_item["address"]},
                "age": {"S": new_item["age"]},
                "car": {"S": new_item["car"]}}
        
        self.client.put_item(
            TableName=self.table,
            Item=validated_data
        )
        sleep(1)
        confirm_item = self.client.get_item(TableName=self.table,
        Key={
            "CustomerId": {"S": new_item["CustomerId"]}
        })

        try:
            data_check = confirm_item["Item"]
        except KeyError:
            return {"status_code": 400, "message": "The key was not created, please try again"}
        
        wrong_data = dict()
        for key, value in data_check.items():
            if value == validated_data[key]:
                pass
            else:
                wrong_data[key] = value
                pass
        
        if len(wrong_data) > 0:
            return {"status_code": 400, 
            "message": f"Something went wrong and the data doesn't seem to match up, please check the table.  Incorrect data: {wrong_data}"}
        else:
            return {"status_code": 200,
            "message": "The items were added successfully!"}
