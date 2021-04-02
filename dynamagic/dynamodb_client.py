import dynamagic.modules.exceptions as dynamodb_exceptions
from schema import Schema
from typing import Dict, List, Union, Tuple
from dynamagic.modules.validation import Validation
from dynamagic.modules.dynamodb_api import DynamodbApi

class DynamodbClient(DynamodbApi):
    def __init__(self, aws_region: str, dynamodb_table: str):
        super().__init__(aws_region=aws_region, dynamodb_table=dynamodb_table)
        self.validation = Validation()
        self.client_exceptions: tuple(Exception) = (dynamodb_exceptions.DynamoDbWrongKeyError,
         dynamodb_exceptions.ValidationWrongSchemaTypeError,
          dynamodb_exceptions.ValidationWrongKeyError,
           dynamodb_exceptions.ValidationMissingKeyError,
            dynamodb_exceptions.ValidationIncorrectKeyTypeError,
             dynamodb_exceptions.ValidationIncorrectAttributeError,
              dynamodb_exceptions.ValidationFailedAttributesUpdateError,
              dynamodb_exceptions.DynamoDbInvalidTableError,
              dynamodb_exceptions.DynamoDbWrongKeyFormatError)
    
    def validate_data(self, validation_type: str,
     unvalidated_data: Union[Dict[str, str], str]) -> Union[Dict[str, str], str]:
        validation_schema: Schema = self.validation.validation_schema(validation_type=validation_type)
        return self.validation.validate_item_data_entegrity(dynamodb_schema=validation_schema,
         unvalidated_item=unvalidated_data)

    def create_item(self, dynamodb_item: Dict[str, str]) -> Dict[str, int]:
        try:
            validated_item: Dict[str, str] = self.validate_data(validation_type="new_item",
             unvalidated_data=dynamodb_item)
            formated_db_item: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(validated_item)
            self.add_item(dynamodb_item=formated_db_item)
            return {"status_code": 200, "message": f"Created new item with key: {validated_item['CustomerId']}"}
        except self.client_exceptions as error:
            return {"status_code": 400, "message": str(error)}
 
    def delete_existing_attributes(self, key: Dict[str, str], validated_attributes: Dict[str, str]) -> Dict[str, str]:
        existing_attributes: Dict[str, Dict[str, str]] = self.get_item(key=key)
        converted_existing_attributes: Dict[str, str] = self.validation.validate_item_to_readable_format(dynamodb_item=existing_attributes)
        return self.remove_duplicated_attributes(new_attributes=validated_attributes,
            old_attributes=converted_existing_attributes)

    def generate_expressions(self, confirmed_new_attributes: Dict[str, str]) -> Tuple[str, Dict[str, str]]:
        update_expression: str = self.generate_update_expression(new_attributes=confirmed_new_attributes)
        expression_attribute_names: Dict[str, str] = self.generate_expression_attribute_names(new_attributes=confirmed_new_attributes)
        expression_attribute_values: Dict[str, str] = self.generate_expression_attribute_values(new_attributes=confirmed_new_attributes,
         dynamodb_validation_format_mapper=self.validation.dynamodb_format_mapper)
        return update_expression, expression_attribute_names, expression_attribute_values
    
    def confirm_item_updated(self, update_response: Dict[str, str],
     confirmed_new_attributes: Dict[str, str]) -> Union[bool, Exception]:
        converted_attributes: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(dynamodb_item=confirmed_new_attributes)
        return self.validation.validate_attributes_updated(response=update_response,
         validated_new_attributes=converted_attributes)
   
    def update_item(self, dynamodb_attributes: Dict[str, str]) -> Dict[str, int]:
        try:
            validated_attributes: Dict[str, str] = self.validate_data(validation_type="update_item",
             unvalidated_data=dynamodb_attributes)
            key: Dict[str,
             Dict[str, str]] = self.validation.validate_item_to_db_format(dynamodb_item={"CustomerId": validated_attributes.pop("CustomerId")})
            removed_duplicated_attributes: Dict[str, str] = self.delete_existing_attributes(key=key,
             validated_attributes=validated_attributes)
            validated_new_attributes: Dict[str, str] = self.validation.validate_new_attributes_exist(item_attributes=removed_duplicated_attributes)
            update_expression, expression_attribute_names, expression_attribute_values = self.generate_expressions(confirmed_new_attributes=validated_new_attributes)
            update_response: Dict[str, str] = self.push_update(key=key, update_expression=update_expression,
              expression_attribute_names=expression_attribute_names,
               expression_attribute_values=expression_attribute_values)
            self.confirm_item_updated(update_response=update_response,
             confirmed_new_attributes=validated_new_attributes)
            return {"staus_code": 200, "message": f"Item with the key: {key} has been updated successfully"}
        except self.client_exceptions as error:
            return {"status_code": 400, "message": str(error)}

    def fetch_item(self, key: Dict[str, str]) -> Dict[str, Union[int, Dict[str, str]]]:
        try:
            validated_key: Dict[str, str] = self.validate_data(validation_type="read_item", unvalidated_data=key)
            formated_key: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(dynamodb_item=validated_key)
            fetched_item: Dict[str, str] = self.get_item(key=formated_key)
            readable_item: Dict[str, str] = self.validation.validate_item_to_readable_format(dynamodb_item=fetched_item)
            return {"status_code": 200, "message": readable_item}
        except self.client_exceptions as error:
            return {"status_Code": 400, "message": str(error)}
    
    def get_items(self) -> Dict[str, Union[int, List[Dict[str, str]]]]:
        try:
            unformated_table_items: List[Dict[str, str]] = self.get_items()
            formated_table_items: List[Dict[str, str]] = [self.validation.validate_item_to_readable_format(table_item) \
                for table_item in unformated_table_items]
            return {"status_code": 200, "message": formated_table_items}
        except self.client_exceptions as error:
            return {"status_Code": 400, "message": str(error)}

    def delete_item(self, key: Dict[str, str]) -> Dict[str, int]:
        try:
            validated_key: Dict[str, str] = self.validate_data(validation_type="delete_item", unvalidated_data=key)
            formated_key: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(dynamodb_item=validated_key)
            self.remove_item(key=formated_key)
            return {"status_code": 200,
             "message": f"Item with key: {validated_key['CustomerId']} has been deleted"}
        except self.client_exceptions as error:
            return {"status_Code": 400, "message": str(error)}
