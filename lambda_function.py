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
        schema = Schema()
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
                "AttributeType": schema.valid_schema["CustomerId"]["dynamo_type"]
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
        
        validated_data = dict()
        for key, value in new_item.items():
            validated_data.update({key: {schema.valid_schema[key]["dynamo_type"]: value}})
       
        self.client.put_item(
            TableName=self.table,
            Item=validated_data
        )
        sleep(1)
        confirm_item = self.client.get_item(TableName=self.table,
        Key={
            "CustomerId": {schema.valid_schema["CustomerId"]["dynamo_type"]: new_item["CustomerId"]}
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


    def update_item(self, data: dict) -> dict:
        """Update an item on the database that already exists.  Must provide data based on the modules/schema for this to be successful.
        The method will check that the keys exist in the database and if not will fail, it will then also check that any of the attributes of the item exist in the schema.
        If not it will then fail.  It will then check if the data is actually new or existing, if anything is existing it will skip out that data and only update information based on new data.

        Args:
            data (dict): Users must provide a minimum of two bits of information
            * CustomerId - the unique ID to search for information
            * Attribute - Any attribute that you want to update.

        Returns:
            dict: Result of the action being successful or unsuccessful.
        """
        schema = Schema(data)
        data_validator = schema.data_entegrity()
        if data_validator["status_code"] == 200:
            pass
        else:
            return data_validator
        
        customer_id = data.pop("CustomerId")
        old_item = self.client.get_item(
            TableName=self.table,
            Key={
                "CustomerId": {schema.valid_schema["CustomerId"]["dynamo_type"]: customer_id}
                }
        )
        
        try:
            old_item = old_item["Item"]
        except KeyError:
            return {"status_code": 400, "message": "Failed to retrieve the item, please check the key and try again"}
        
        validating_keys = schema.keys_validator(full_validate=False)
        if validating_keys["status_code"] != 200:
            return validating_keys
        
        duplicate_data_keys = [same_data for same_data in data.keys() if data[same_data] == old_item[same_data][schema.valid_schema[same_data]["dynamo_type"]]]
        for duplicate_data in duplicate_data_keys:
            data.pop(duplicate_data)
        
        if len(data) == 0:
            return {"status_code": 400, "message": "There is no new data to process, please provide new information and try again"}
        
        update_expression = str()
        for key in data.keys():
            if update_expression.startswith("SET"):
                pass
            else:
                update_expression = f"SET {schema.expression_mapping[key]['expression']} = {schema.expression_mapping[key]['attribute_name']}"
            
            if update_expression.endswith(schema.expression_mapping[key]["attribute_name"]):
                continue
            else:
                update_expression += f", {schema.expression_mapping[key]['expression']} = {schema.expression_mapping[key]['attribute_name']}"
        
        expression_attribute_names = {f"{schema.expression_mapping[key]['expression']}": key for key in data.keys()}
        expression_attribute_values = {f"{schema.expression_mapping[key]['attribute_name']}": {schema.valid_schema[key]["dynamo_type"]: data[key]} for key in data.keys()}

        response = self.client.update_item(
            TableName=self.table,
            Key={
                "CustomerId": {schema.valid_schema["CustomerId"]["dynamo_type"]: customer_id}
            },
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        validate_response = {key: {schema.valid_schema[key]["dynamo_type"]: value} for key, value in data.items()}
        try:
            if response["Attributes"] == validate_response:
                return {"status_code": 200, "message": "Updated the items successfully"}
            else:
                return {"status_code": 400, "message": "The update_item method did not succeed as expected, please trouble shoot."}
        except KeyError:
            return {"status_code": 400, "message": "The update_item method did not succeed as expected, please trouble shoot."}
            

    def delete_item(self, key: str) -> dict:
        """Delete an item from the database.  This will validate that the item doesn't exist before and after.

        Args:
            key (str): Provide the key for the table to find the item

        Returns:
            dict: Message to confirm if the item was deleted or not
        """
        schema = Schema({"CustomerId": key})
        key_validation = schema.data_entegrity()
        if key_validation["status_code"] != 200:
            return key_validation

        item_validation = self.client.get_item(TableName=self.table, Key={
            "CustomerId": {schema.valid_schema["CustomerId"]["dynamo_type"]: key}
        })
        
        try:
            item_validation["Item"] 
        except KeyError:
            return {"status_code": 400, "message": "The item does not exist, please check the ID and try again"}
        
        self.client.delete_item(TableName=self.table, Key={
            "CustomerId": {schema.valid_schema["CustomerId"]["dynamo_type"]: key}
        })

        delete_validation = self.client.get_item(TableName=self.table, Key={
            "CustomerId": {schema.valid_schema["CustomerId"]["dynamo_type"]: key}
        })

        try:
            delete_validation["Item"]
            return {"status_code": 400, "message": "The item still exists and was not deleted successfully, please try again"}
        except KeyError:
            return {"status_code": 200, "message": "The item has been deleted successfully"}

    
    # Method for searching all items on the table
        # Present the data in a more human readable way
    # Method for getting an item from the database
    # Method for querying items based on field names