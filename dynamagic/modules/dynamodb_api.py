from botocore.exceptions import ParamValidationError
from typing import Dict, List, Union
import boto3
from dynamagic.modules.exceptions import (
    DynamoDbWrongKeyError,
    ValidationIncorrectAttributeError,
    DynamoDbInvalidTableError,
    DynamoDbWrongKeyFormatError,
)


class DynamodbApi:
    def __init__(self, aws_region: str, dynamodb_table: str) -> None:
        self.aws_region = aws_region
        self.client = boto3.client("dynamodb", region_name=aws_region)
        self.dynamodb_table = dynamodb_table
        self.expression_mapping: Dict[str, Dict[str, str]] = {
            "name": {
                "expression_attribute_name": "#N",
                "expression_attribute_var": ":n",
            },
            "address": {
                "expression_attribute_name": "#AD",
                "expression_attribute_var": ":ad",
            },
            "age": {
                "expression_attribute_name": "#AG",
                "expression_attribute_var": ":ag",
            },
            "car": {
                "expression_attribute_name": "#C",
                "expression_attribute_var": ":c",
            },
        }

    def add_item(
        self, dynamodb_item: Dict[str, Dict[str, str]]
    ) -> Union[bool, Exception]:
        try:
            self.client.put_item(TableName=self.dynamodb_table, Item=dynamodb_item)
            return True
        except self.client.exceptions.ClientError as error:
            raise DynamoDbWrongKeyError from error
        except ParamValidationError as error:
            raise DynamoDbWrongKeyFormatError from error

    @staticmethod
    def remove_duplicated_attributes(
        new_attributes: Dict[str, str], old_attributes: Dict[str, str]
    ) -> Union[Dict[str, str], Exception]:
        try:
            duplicate_attributes = list()
            for attribute, value in new_attributes.items():
                if value == old_attributes[attribute]:
                    duplicate_attributes.append(attribute)

            for duplicate_attribute in duplicate_attributes:
                new_attributes.pop(duplicate_attribute)

            return new_attributes
        except KeyError as error:
            raise ValidationIncorrectAttributeError(data=error) from error

    def generate_update_expression(
        self, new_attributes: dict
    ) -> str or Dict[str, str] or Exception:
        update_expression = str()
        try:
            for attribute in new_attributes.keys():
                if update_expression.startswith("SET"):
                    pass
                else:
                    update_expression = f"SET {self.expression_mapping[attribute]['expression_attribute_name']} = {self.expression_mapping[attribute]['expression_attribute_var']}"

                if update_expression.endswith(
                    self.expression_mapping[attribute]["expression_attribute_var"]
                ):
                    continue
                else:
                    update_expression += f", {self.expression_mapping[attribute]['expression_attribute_name']} = {self.expression_mapping[attribute]['expression_attribute_var']}"

            return update_expression
        except KeyError as error:
            raise ValidationIncorrectAttributeError(data=error) from error

    def generate_expression_attribute_names(
        self, new_attributes: dict
    ) -> Union[Dict[str, str], Exception]:
        try:
            return {
                self.expression_mapping[attribute][
                    "expression_attribute_name"
                ]: attribute
                for attribute in new_attributes.keys()
            }
        except KeyError as error:
            raise ValidationIncorrectAttributeError(data=error) from error

    def generate_expression_attribute_values(
        self, new_attributes: dict, dynamodb_validation_format_mapper: dict
    ) -> Union[Dict[str, str], Exception]:
        """Generate a dict with expression attribute values to be used to update the item

        Args:
            new_attributes (dict): New attributes that will be updated
            dynamodb_validation_format_mapper (dict): The format mapper from the validation class

        Returns:
            Dict[str, str]: Dictionary with attribute values generated
        """
        try:
            return {
                self.expression_mapping[attribute]["expression_attribute_var"]: {
                    dynamodb_validation_format_mapper[attribute]["dynamodb_type"]: value
                }
                for attribute, value in new_attributes.items()
            }
        except KeyError as error:
            raise ValidationIncorrectAttributeError(data=error) from error

    def push_update(
        self,
        key: dict,
        update_expression: str,
        expression_attribute_names: dict,
        expression_attribute_values: dict,
    ) -> Dict[str, str]:
        response: dict = self.client.update_item(
            TableName=self.dynamodb_table,
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )
        return response

    def get_item(self, key: dict) -> Union[Dict[str, str], Exception]:
        try:
            return self.client.get_item(TableName=self.dynamodb_table, Key=key)["Item"]
        except KeyError as error:
            raise DynamoDbWrongKeyError from error

    def get_items(self) -> Union[List[Dict[str, str]], Exception]:
        try:
            return self.client.scan(TableName=self.dynamodb_table)["Items"]
        except self.client.exceptions.ResourceNotFoundException as error:
            raise DynamoDbInvalidTableError from error

    def remove_item(self, key: str) -> Union[bool, Exception]:
        try:
            self.client.delete_item(TableName=self.dynamodb_table, Key=key)
            return True
        except ParamValidationError as error:
            raise DynamoDbWrongKeyFormatError from error
