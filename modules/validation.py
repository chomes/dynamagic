from schema import Schema, And, SchemaError, SchemaMissingKeyError, SchemaWrongKeyError, Use, Optional
from typing import Dict
import boto3


class Validation:
    def __init__(self, aws_region: str):
        self.new_item_schema = Schema({'CustomerId': And(Use(str), lambda client_id_length: len(client_id_length) == 10, lambda client_id: int(client_id)),
        'name': And(Use(str)), 'address': And(Use(str)), 'age': And(Use(str)), 'car': And(Use(str))})
        self.update_item_schema = Schema({'CustomerId': And(Use(str), lambda client_id_length: len(client_id_length) == 10),
        Optional('name'): And(Use(str)), Optional('address'): And(Use(str)), Optional('age'): And(Use(str)),
        Optional('car'): And(Use(str))})
        self.dynamodb_format_mapper = {'CustomerId': {'dynamodb_type': 'S'}, 'name': {'dynamodb_type': 'S'},
        'address': {'dynamodb_type': 'S'},  'age': {'dynamodb_type': 'S'}, 'car': {'dynamodb_type': 'S'}}
        self.dynamodb_key_schema = Schema({'CustomerId': And(Use(str), lambda client_id_length: len(client_id_length) == 10)})
        self.aws_region = aws_region


    




    # check the data input is correct
    # make sure it's the correct format and change it to the correct format
    # Find the errors and catch the exceptions and generate the messages
    # Validate the item exists
    # Validate table exists

    def validate_dynamodb_table_exists(self, dynamodb_table: str) -> Dict[str, int]:
        """Checks if table exists before proceeding with actions

        Returns:
            dict: status code and message if valid or not
        """
        try:
            dynamodb = boto3.client('dynamodb', region_name=self.aws_region)
            dynamodb.scan(TableName=dynamodb_table)
            return {'status_code': 200, 'message': 'This table exists'}
        except self.client.exceptions.ResourceNotFoundException:
            return {'status_code': 400, 'message': 'The table does not exist, please try again with the correct name'}
    
    def validate_dynamodb_item_exists(self, table_item: dict) -> Dict[str, int]:
        """Validates the item grabbed exists

        Args:
            table_item (dict): Response from dynamodb when fetching an item

        Returns:
            Dict[str, int]: status code
        """
        try:
            table_item['Item']
            return {'status_code': 200, 'message': 'This item exists'}
        except KeyError:
            return {'status_code': 400, 'message': 'This item does not exist'}

    def validation_schema(self, validation_type: str) -> Schema or Dict[str, int]:
        """Grab a schema based on the validation you need to do

        Args:
            validation_type (str): Provide a string with a validation type [new_item, update_item, delete_item]

        Returns:
            Schema or Dict[str, int]: Return the schema you requested or a status code if you didn't give one of the 3 names.
        """
        if validation_type == 'new_item':
            return self.new_item_schema
        elif validation_type == 'update_item':
            return self.update_item_schema
        elif validation_type == 'delete_item' or validation_type == 'read_item':
            return self.dynamodb_key_schema
        else:
            return {'status_code': 400, 'message': 'You did not specify a validation type when using validation schema, please choose either new_item or updated_item'}

    def validate_item_data_entegrity(self, dynnamodb_schema: Schema, unvalidated_item: dict) -> Dict[str, int]:
        """Validate the item for use on dynamodb based on the schema used

        Args:
            dynnamodb_schema (Schema): Provide a schema that will returned using validation_schema method
            unvalidated_item (dict): Dict of the item you want to validate the data of

        Returns:
            Dict[str, int]: Data will be validated or will have a status code
        """
        try:
            return dynnamodb_schema.validate(unvalidated_item)
        except SchemaError:
            return {'status_code': 400, 'message': 'Either the CustomerID is not 10 characters long or it is not in numerical form, please format and try again.'}
        except SchemaWrongKeyError as e:
            return {'status_code': 400, 'message': f'{str(e).split()[2]} is not a key you can use, please try again'}
        except SchemaMissingKeyError as e:
            return {'status_code': 400, 'message': f'You are missing {str(e).split()[2]} in your schema, please add it and try again'}
    
    def validate_item_to_db_format(self, dynamodb_item: dict) -> Dict[str]:
        """Generate the item to a format that Dynamo DB will accept when adding it to the database

        Args:
            dynamodb_item (dict): The item with all it's attributes inside it.

        Returns:
            Dict[str]: Converted dictionary
        """
        return {attribute: {self.dynamodb_format_mapper[attribute]['dynamodb_type']: value} for attribute, value in dynamodb_item.items()}
    
    def validate_item_to_readable_format(self, dynamodb_item: dict) -> Dict[str]:
        """Generate the item from Dynamo DB format to a standard dict for readability

        Args:
            dynamodb_item (dict): Dict item from dynamoDB

        Returns:
            Dict[str]: Formatted dictionary
        """
        return {attribute: value[self.dynamodb_format_mapper[attribute]['dynamodb_type']] for attribute, value in dynamodb_item.items()}
