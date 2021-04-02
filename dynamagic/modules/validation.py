from typing import Dict, Union
from schema import (
    Schema,
    And,
    SchemaError,
    SchemaMissingKeyError,
    SchemaWrongKeyError,
    Use,
    Optional,
)
from dynamagic.modules.exceptions import (
    ValidationFailedAttributesUpdateError,
    ValidationIncorrectAttributesError,
    ValidationNoNewAttributesError,
    ValidationWrongSchemaTypeError,
    ValidationWrongKeyError,
    ValidationMissingKeyError,
    ValidationIncorrectKeyTypeError,
)


class Validation:
    def __init__(self) -> None:
        self.new_item_schema = Schema(
            {
                "CustomerId": And(
                    Use(str), lambda client_id: len(client_id) == 10 and int(client_id)
                ),
                "name": And(Use(str)),
                "address": And(Use(str)),
                "age": And(Use(str)),
                "car": And(Use(str)),
            }
        )
        self.update_item_schema = Schema(
            {
                "CustomerId": And(Use(str), lambda client_id: len(client_id) == 10),
                Optional("name"): And(Use(str)),
                Optional("address"): And(Use(str)),
                Optional("age"): And(Use(str)),
                Optional("car"): And(Use(str)),
            }
        )
        self.dynamodb_format_mapper = {
            "CustomerId": {"dynamodb_type": "S"},
            "name": {"dynamodb_type": "S"},
            "address": {"dynamodb_type": "S"},
            "age": {"dynamodb_type": "S"},
            "car": {"dynamodb_type": "S"},
        }
        self.dynamodb_key_schema = Schema(
            {"CustomerId": And(Use(str), lambda client_id: len(client_id) == 10)}
        )

    def validation_schema(self, validation_type: str) -> Union[Schema, Exception]:
        """Grab a schema based on the validation you need to do

        Args:
            validation_type (str): Provide a string with a validation type [new_item,
             update_item, read_item, delete_item]

        Returns:
            Schema or Dict[str, int]: Return the schema you requested or a status code if you didn"t give one of the 3 names.
        """
        if validation_type == "new_item":
            return self.new_item_schema
        if validation_type == "update_item":
            return self.update_item_schema
        if validation_type in ("delete_item", "read_item"):
            return self.dynamodb_key_schema

        raise ValidationWrongSchemaTypeError(data=validation_type)

    @staticmethod
    def validate_item_data_entegrity(
        dynamodb_schema: Schema, unvalidated_item: Dict[str, str]
    ) -> Union[Dict[str, str], Exception]:
        try:
            return dynamodb_schema.validate(unvalidated_item)
        except SchemaWrongKeyError as error:
            raise ValidationWrongKeyError(data=str(error).split()[2]) from error
        except SchemaMissingKeyError as error:
            raise ValidationMissingKeyError(data=str(error).split()[2]) from error
        except SchemaError as error:
            raise ValidationIncorrectKeyTypeError(data=str(error).split()[1]) from error

    def validate_item_to_db_format(
        self, dynamodb_item: Dict[str, str]
    ) -> Dict[str, Dict[str, str]]:
        return {
            attribute: {self.dynamodb_format_mapper[attribute]["dynamodb_type"]: value}
            for attribute, value in dynamodb_item.items()
        }

    def validate_item_to_readable_format(
        self, dynamodb_item: Dict[str, Dict[str, str]]
    ) -> Dict[str, str]:
        return {
            attribute: value[self.dynamodb_format_mapper[attribute]["dynamodb_type"]]
            for attribute, value in dynamodb_item.items()
        }

    def validate_new_attributes_exist(
        self, item_attributes: Dict[str, str]
    ) -> Union[Dict[str, str], Exception]:
        if len(item_attributes) > 0:
            return item_attributes
        if len(item_attributes) == 0:
            raise ValidationNoNewAttributesError

    @staticmethod
    def validate_attributes_updated(
        response: Dict[str, Dict[str,str]], validated_new_attributes: Dict[str, Dict[str, str]]
    ) -> Union[bool, Exception]:
        """Validate that the attributes required to be updated have been updated by comparing the response dictionary to the converted attributes dictionary

        Args:
            response (dict): Response output from the push_update method \
            validated_new_attributes (dict): new_attributes that have been converted by using the validate_item_to_db_format

        Returns:
            Dict[str, int]: Generated response code
        """
        try:
            if response["Attributes"] == validated_new_attributes:
                return True
            if response["Attributes"] != validated_new_attributes:
                raise ValidationIncorrectAttributesError
        except KeyError as error:
            raise ValidationFailedAttributesUpdateError from error
