from schema import Schema, And, SchemaError, SchemaMissingKeyError, SchemaWrongKeyError, Use, Optional
from typing import Dict
from dynamagic.modules.exceptions import ValidationFailedAttributesUpdateError, ValidationIncorrectAttributesError, ValidationNoNewAttributesError, ValidationWrongSchemaTypeError, ValidationWrongKeyError, ValidationMissingKeyError, \
    ValidationIncorrectKeyTypeError


class Validation:
    def __init__(self, aws_region: str or None = None):
        self.new_item_schema = Schema({'CustomerId': And(Use(str), lambda client_id_length: len(client_id_length) == 10, lambda client_id: int(client_id)),
        'name': And(Use(str)), 'address': And(Use(str)), 'age': And(Use(str)), 'car': And(Use(str))})
        self.update_item_schema = Schema({'CustomerId': And(Use(str), lambda client_id_length: len(client_id_length) == 10),
        Optional('name'): And(Use(str)), Optional('address'): And(Use(str)), Optional('age'): And(Use(str)),
        Optional('car'): And(Use(str))})
        self.dynamodb_format_mapper = {'CustomerId': {'dynamodb_type': 'S'}, 'name': {'dynamodb_type': 'S'},
        'address': {'dynamodb_type': 'S'},  'age': {'dynamodb_type': 'S'}, 'car': {'dynamodb_type': 'S'}}
        self.dynamodb_key_schema = Schema({'CustomerId': And(Use(str), lambda client_id_length: len(client_id_length) == 10)})
        self.aws_region = aws_region

  
    def validation_schema(self, validation_type: str) -> Schema or Exception:
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
            raise ValidationWrongSchemaTypeError(data=validation_type)

    def validate_item_data_entegrity(self, dynnamodb_schema: Schema, unvalidated_item: dict) -> Dict[str, str] or Exception:
        try:
            return dynnamodb_schema.validate(unvalidated_item)
        except SchemaWrongKeyError as error:
            raise ValidationWrongKeyError(data=str(error).split()[2])
        except SchemaMissingKeyError as error:
            raise ValidationMissingKeyError(data=str(error).split()[2])
        except SchemaError as error:
            raise ValidationIncorrectKeyTypeError(data=str(error).split()[2])
    
    def validate_item_to_db_format(self, dynamodb_item: dict) -> Dict[str, dict]:
        return {attribute: {self.dynamodb_format_mapper[attribute]['dynamodb_type']: value} for attribute, value in dynamodb_item.items()}
    
    def validate_item_to_readable_format(self, dynamodb_item: dict) -> Dict[str, str]:
        return {attribute: value[self.dynamodb_format_mapper[attribute]['dynamodb_type']] for attribute, value in dynamodb_item.items()}
    
    def validate_new_attributes_exist(self, item_attributes: dict) -> Dict[str, str] or Exception:
        if len(item_attributes) == 0:
            raise ValidationNoNewAttributesError
        elif len(item_attributes) > 0:
            return item_attributes
    
    def validate_attributes_updated(self, response: dict, validated_new_attributes: dict) -> bool or Exception:
        """Validate that the attributes required to be updated have been updated by comparing the response dictionary to the converted attributes dictionary

        Args:
            response (dict): Response output from the push_update method 
            validated_new_attributes (dict): new_attributes that have been converted by using the validate_item_to_db_format

        Returns:
            Dict[str, int]: Generated response code
        """
        try:
            if response['Attributes'] == validated_new_attributes:
                return True
            elif response['Attributes'] != validated_new_attributes:
                raise ValidationIncorrectAttributesError
        except KeyError:
            raise ValidationFailedAttributesUpdateError
