from dynamagic.modules.dynamodb_api import DynamodbApi
from dynamagic.modules.validation import Validation
from typing import Dict, List
import dynamagic.modules.exceptions
import dynamagic.modules.exceptions as dynamodb_exceptions


class DynamodbClient(DynamodbApi):
    def __init__(self, aws_region: str, dynamodb_table: str):
        super.__init__(aws_region=aws_region, dynamodb_table=dynamodb_table)
        self.validation = Validation()
    
    def validate_data(self, validation_type: str, unvalidated_data: Dict[str, str] or str) -> Dict[str, str]:
            validation_schema: Schema = self.validation.validation_schema(validation_type=validation_type)
            return self.validation.validate_item_data_entegrity(validation_schema=validation_Schema, 
            unvalidated_data=unvalidated_data)


    def create_item(self, dynamodb_item: Dict[str, str]) -> Dict[str, int]:
        try:
            validation_schema: Schema = self.validation.validation_schema(validation_type='new_item')
            validated_item: Dict[str, str] = self.validate_data(validation_type='new_item', unvalidated_data=dynamodb_item)
            formated_db_item: Dict[str, dict] = self.validation.validate_item_to_db_format(validated_item)
            self.add_item(dynamodb_item=formated_db_item)
            return {'status_code': 200, 'message': f'Created new item with key: {validated_item["CustomerId"]}'}
        except dynamodb_exceptions.DynamoDbWrongKeyError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationWrongSchemaTypeError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationWrongKeyError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationMissingKeyError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationIncorrectKeyTypeError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.DynamoDbInvalidTableError as error:
            return {'status_Code': 400, 'message': str(error)}

    def delete_existing_attributes(self, key: str, validated_attributes: Dict[str, str]) -> Dict[str, str]:
        existing_attributes: Dict[str, dict] = self.get_item(key=key)
        converted_existing_item: Dict[str, str] = self.validation.validate_item_to_readable_format(dynamodb_item=existing_attributes)
        return self.remove_duplicated_attributes(new_attributes=validated_attributes,
            old_attributes=existing_attributes)

    def generate_expressions(self, confirmed_new_attributes: Dict[str, str]) -> tuple:
            update_expression: str = self.generate_update_expression(new_attributes=confirmed_new_attributes)
            expression_attribute_names: Dict[str, str] = self.generate_expression_attribute_names(new_attributes=confirmed_new_attributes)
            expression_attribute_values: Dict[str, str] = self.generate_expression_attribute_values(new_attributes=confirmed_new_attributes, 
            dynamodb_validation_format_mapper=self.validation.dynamodb_format_mapper)
            return update_expression, expression_attribute_names, expression_attribute_values
    
    def confirm_item_updated(self, update_response: Dict[str, str], confirmed_new_attributes: Dict[str, str]) -> bool:
            converted_attributes = self.validation.validate_item_to_db_format(dynamodb_item=confirmed_new_attributes)
            return self.validation.validate_attributes_updated(response=update_response, validated_new_attributes=converted_attributes)
   
    def update_item(self, dynamodb_attributes: Dict[str, str]) ->[str, int]:
        try:
            validated_attributes: Dict[str, str] = self.validate_data(validation_type='update_item', unvalidated_data=dynamodb_attributes)
            key = validated_attributes.pop('CustomerId')
            removed_duplicated_attributes: Dict[str, str] = self.delete_existing_attributes(key=key, validated_attributes=validated_attributes)
            validated_new_attributes: Dict[str, str] = self.validation.validate_new_attributes_exist(item_attributes=removed_duplicated_attributes)
            update_expression, expression_attribute_names, expression_attribute_values = self.generate_expressions(confirmed_new_attributes=confirmed_new_attributes)
            update_response: Dict[str, str] = self.push_update(key=key, update_expression=update_expression, 
            expression_attribute_names=expression_attribute_names, expression_attribute_values=expression_attribute_values)
            self.confirm_item_updated(update_response=update_response, confirmed_new_attributes=confirmed_new_attributes)
            return {'staus_code': 200, 'message': f'Item with the key: {key} has been updated successfully'}
        except dynamodb_exceptions.DynamoDbWrongKeyError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationWrongSchemaTypeError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationWrongKeyError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationMissingKeyError as error:
            return {'status_code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationIncorrectKeyTypeError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationIncorrectAttributeError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationIncorrectAttributesError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationFailedAttributesUpdateError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.DynamoDbInvalidTableError as error:
            return {'status_Code': 400, 'message': str(error)}

    def fetch_item(self, key: str) -> Dict[str, int, dict]:
        try:
            validated_key = self.validate_data(validation_type='read_item', unvalidated_data=key)
            unformated_item: Dict[str, str] = self.get_item(key=validated_key)
            formated_item: Dict[str, str] = self.validation.validate_item_to_readable_format(dynamodb_item=unformatted_item)
            return {'status_code': 200, 'message': formated_item}
        except dynamodb_exceptions.DynamoDbWrongKeyError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.DynamoDbInvalidTableError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationIncorrectKeyTypeError as error:
            return {'status_Code': 400, 'message': str(error)}
    
    def get_items(self) -> Dict[str, int, List[Dict[str, str]]]:
        try:
            unformated_table_items: List[Dict[str, str]] = self.get_items()
            formated_table_items: List[Dict[str, str]] = [self.validation.validate_item_to_readable_format(table_item) for table_item in unformated_table_items]
            return {'status_code': 200, 'message': formated_table_items}
        except dynamodb_exceptions.DynamoDbWrongKeyError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.DynamoDbInvalidTableError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationIncorrectKeyTypeError as error:
            return {'status_Code': 400, 'message': str(error)}

  
    def delete_item(self, key: str) -> Dict[str, int]:
        try:
            validated_key = self.validate_data(validation_type='delete_item', unvalidated_data=key)
            self.remove_item(key=validated_key)
            return {'status_code': 200, 'message': f'Item with key: {validated_key} has been deleted'}
        except dynamodb_exceptions.DynamoDbWrongKeyError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.DynamoDbInvalidTableError as error:
            return {'status_Code': 400, 'message': str(error)}
        except dynamodb_exceptions.ValidationIncorrectKeyTypeError as error:
            return {'status_Code': 400, 'message': str(error)}
