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
     unvalidated_data: Dict[str, str]) -> Dict[str, str]:
        """Designed to validate the data using the validation_schema in the validation module

        Args:
            validation_type (str): Choose between ['new_item', 'update_item', 'delete_item', 'read_item']
            unvalidated_data (Dict[str, str]): The attributes for the item you want to validate are correct

        Returns:
            Dict[str, str]: Validated data with the correct format
        """
        validation_schema: Schema = self.validation.validation_schema(validation_type=validation_type)
        return self.validation.validate_item_data_entegrity(dynamodb_schema=validation_schema,
         unvalidated_item=unvalidated_data)

    def create_item(self, dynamodb_item: Dict[str, str]) -> Dict[str, int]:
        """Create a new item on the table

        Args:
            dynamodb_item (Dict[str, str]): Attributes for the item, must include the key specified on your table.

        Returns:
            Dict[str, int]: Status code of the results of the action
        """
        try:
            validated_item: Dict[str, str] = self.validate_data(validation_type="new_item",
             unvalidated_data=dynamodb_item)
            formated_db_item: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(validated_item)
            self.add_item(dynamodb_item=formated_db_item)
            return {"status_code": 200, "message": f"Created new item with key: {validated_item['CustomerId']}"}
        except self.client_exceptions as error:
            return {"status_code": 400, "message": str(error)}
 
    def delete_existing_attributes(self, key: Dict[str, str], validated_attributes: Dict[str, str]) -> Dict[str, str]:
        """Deletes any attributes that are the same as the old ones, used for update_item

        Args:
            key (Dict[str, str]): The key for the existing item
            validated_attributes (Dict[str, str]): Attributes that you want to update, parse in validated attributes only

        Returns:
            Dict[str, str]: Returns attributes that are only new
        """
        existing_attributes: Dict[str, Dict[str, str]] = self.get_item(key=key)
        converted_existing_attributes: Dict[str, str] = self.validation.validate_item_to_readable_format(dynamodb_item=existing_attributes)
        return self.remove_duplicated_attributes(new_attributes=validated_attributes,
            old_attributes=converted_existing_attributes)

    def generate_expressions(self, confirmed_new_attributes: Dict[str, str]) -> Tuple[str, Dict[str, str]]:
        """Used to generate expressions required to update an item in dynamodb

        Args:
            confirmed_new_attributes (Dict[str, str]): Only parse in the attributes after you've used delete_existing_attributes

        Returns:
            Tuple[str, Dict[str, str]]: Returns the update_expression string, attribute_names and attribute_values
        """
        update_expression: str = self.generate_update_expression(new_attributes=confirmed_new_attributes)
        expression_attribute_names: Dict[str, str] = self.generate_expression_attribute_names(new_attributes=confirmed_new_attributes)
        expression_attribute_values: Dict[str, str] = self.generate_expression_attribute_values(new_attributes=confirmed_new_attributes,
         dynamodb_validation_format_mapper=self.validation.dynamodb_format_mapper)
        return update_expression, expression_attribute_names, expression_attribute_values
    
    def confirm_item_updated(self, update_response: Dict[str, Dict[str, str]],
     confirmed_new_attributes: Dict[str, str]) -> Union[bool, Exception]:
        """Validates that the item has been updated successfully after you've pushed it through the database

        Args:
            update_response (Dict[str, Dict[str, str]]): The response from the database after you've made the change
            confirmed_new_attributes (Dict[str, str]): Parse in the attributes after using delete_existing_attributes

        Returns:
            Union[bool, Exception]: Returns if it's True or raises an exception if it fails to update
        """

        formated_attributes: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(dynamodb_item=confirmed_new_attributes)
        return self.validation.validate_attributes_updated(response=update_response,
         validated_new_attributes=formated_attributes)
   
    def update_item(self, dynamodb_attributes: Dict[str, str]) -> Dict[str, int]:
        """Updates an existing item to the database

        Args:
            dynamodb_attributes (Dict[str, str]): Provide your key along with the attribute names you want to update

        Returns:
            Dict[str, int]: Returns a status code and a message telling you if it passed or failed
        """
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
            return {"staus_code": 200, "message": f"Item with the key provided has been updated successfully"}
        except self.client_exceptions as error:
            return {"status_code": 400, "message": str(error)}


    def fetch_item(self, key: Dict[str, str]) -> Dict[str, Union[int, Dict[str, str]]]:
        """Get an existing item from the database

        Args:
            key (Dict[str, str]): They key for the item in the database

        Returns:
            Dict[str, Union[int, Dict[str, str]]]: Returns the status code and the item or the error why it failed
        """
        try:
            validated_key: Dict[str, str] = self.validate_data(validation_type="read_item", unvalidated_data=key)
            formated_key: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(dynamodb_item=validated_key)
            fetched_item: Dict[str, str] = self.get_item(key=formated_key)
            readable_item: Dict[str, str] = self.validation.validate_item_to_readable_format(dynamodb_item=fetched_item)
            return {"status_code": 200, "message": readable_item}
        except self.client_exceptions as error:
            return {"status_code": 400, "message": str(error)}
    
    def fetch_items(self) -> Dict[str, Union[int, Union[str, List[Dict[str, str]]]]]:
        """Fetch a bulk amount of items from the database

        Returns:
            Dict[str, Union[int, Union[str, List[Dict[str, str]]]]]: Returns either a status code with a list of dictionaries or an error message
        """
        try:
            unformated_table_items: List[Dict[str, Dict[str, str]]] = self.get_items()
            formated_table_items: List[Dict[str, str]] = [self.validation.validate_item_to_readable_format(table_item) \
                for table_item in unformated_table_items]
            return {"status_code": 200, "message": formated_table_items}
        except self.client_exceptions as error:
            return {"status_code": 400, "message": str(error)}

    def delete_item(self, key: Dict[str, str]) -> Dict[str, int]:
        """Delete an existing item from the database

        Args:
            key (Dict[str, str]): The key for the item in the database

        Returns:
            Dict[str, int]: Returns a status code either passing or failing.
        """
        try:
            validated_key: Dict[str, str] = self.validate_data(validation_type="delete_item", unvalidated_data=key)
            formated_key: Dict[str, Dict[str, str]] = self.validation.validate_item_to_db_format(dynamodb_item=validated_key)
            self.remove_item(key=formated_key)
            return {"status_code": 200,
             "message": f"Item with key: {validated_key['CustomerId']} has been deleted"}
        except self.client_exceptions as error:
            return {"status_code": 400, "message": str(error)}
