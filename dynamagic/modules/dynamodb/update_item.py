import boto3
from typing import Dict

class UpdateItem:
    def __init__(self, aws_region: str, dynamodb_table: str) -> None:
        self.aws_region = aws_region
        self.client = boto3.client('dynamodb', region_name=self.aws_region)
        self.dynamodb_table = dynamodb_table
        self.expression_mapping: dict = {'name': {'expression_attribute_name': '#N', 'expression_attribute_var': ":n"},
        'address': {'expression_attribute_name': '#AD', 'expression_attribute_var': ':ad'},
        'age': {'expression_attribute_name': '#AG', 'expression_attribute_var': ':ag'},
        'car': {'expression_attribute_name': '#C', 'expression_attribute_var': ':c'}} 
    
    def remove_duplicated_attributes(self, new_attributes: dict, old_attributes: dict) -> Dict[str, str]:
        """Designed to remove duplicated attributes from the item you are updating so we only update what is required

        Args:
            new_attributes (dict): The updated attributes we want to add to the database
            old_attributes (dict): The old attributes for the item on the database, please run validation method validate_item_to_readable_format
            before using this otherwise it will not work.

        Returns:
            Dict[str, str]: Returns either a dict with attributes in it or a blank dict.
        """
        try:
            duplicate_attributes = list()
            for attribute, value in new_attributes.items():
                if value == old_attributes[attribute]:
                    duplicate_attributes.append(attribute)
            
            for duplicate_attribute in duplicate_attributes:
                new_attributes.pop(duplicate_attribute)
            
            return new_attributes
        except KeyError:
            return {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'}
    
    def generate_update_expression(self, new_attributes: dict) -> str or Dict[str, int]:
        """Generate update_expression string to update the item into the database

        Args:
            new_attributes (dict): New attributes that will be updated

        Returns:
            str: Pieced together string that includes the new attribute_names and their variable names
        """
        update_expression = str()
        try:
            for attribute in new_attributes.keys():
                if update_expression.startswith('SET'):
                    pass
                else:
                    update_expression = f'SET {self.expression_mapping[attribute]["expression_attribute_name"]} = {self.expression_mapping[attribute]["expression_attribute_var"]}'

                if update_expression.endswith(self.expression_mapping[attribute]['expression_attribute_var']):
                    continue
                else:
                    update_expression += f', {self.expression_mapping[attribute]["expression_attribute_name"]} = {self.expression_mapping[attribute]["expression_attribute_var"]}'
            
            return update_expression
        except KeyError:
            return {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'}

    def generate_expression_attribute_names(self, new_attributes: dict) -> Dict[str, int]:
        """Generate a dict with expression attribute names

        Args:
            new_attributes (dict):  New attributes that will be updated

        Returns:
            Dict[str, str]: Dictionary of the attribute names generated
        """
        try:
            return {self.expression_mapping[attribute]['expression_attribute_name']: attribute for attribute in new_attributes.keys()}
        except KeyError:
            return {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'}
    
    def generate_expression_attribute_values(self, new_attributes: dict, dynamodb_format_mapper: dict) -> Dict[str, int]:
        """Generate a dict with expression attribute values to be used to update the item

        Args:
            new_attributes (dict): New attributes that will be updated
            dynamodb_format_mapper (dict): The format mapper from the validation class

        Returns:
            Dict[str, str]: Dictionary with attribute values generated
        """
        try:
            return {self.expression_mapping[attribute]['expression_attribute_var']: {dynamodb_format_mapper[attribute]['dynamodb_type']: value} for attribute,value in new_attributes.items()}
        except KeyError:
            return {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'}


    def push_update(self, key: dict, update_expression: str, expression_attribute_names: dict, expression_attribute_values: dict) -> Dict[str, str]:
        """Update the updated details into the dynamodb database

        Args:
            key (dict): Dict of the key in a dynamodb format
            update_expression (str): Formatted update expression, use the generate_expression method to make this
            expression_attribute_names (dict): The attribute names generated by the generate_expression_attribute_names method
            expression_attribute_values (dict): The values of the attributes to be updated in using the generate_expression_attribute_values

        Returns:
            Dict[str, str]: Response from dynamodb 
        """

        response: dict = self.client.update_item(
            TableName=self.dynamodb_table,
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        return response
