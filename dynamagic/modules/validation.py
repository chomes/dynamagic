from typing import Dict, List, Union

try:
    from schema import (
        Schema,
        And,
        SchemaError,
        SchemaMissingKeyError,
        SchemaWrongKeyError,
        Use,
        Optional,
    )
except ModuleNotFoundError:
    pass
    # from dynamagic.modules.schema import (
    #     Schema,
    #     And,
    #     SchemaError,
    #     SchemaMissingKeyError,
    #     SchemaWrongKeyError,
    #     Use,
    #     Optional,
    # )
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
    def __init__(self, table_schema: Union[Dict[str, type], Dict[str, str]]) -> None:
        self.schema_template = table_schema
        self.key_template = {}
        self.new_item_schema = None
        self.update_item_schema = None
        self.dynamodb_key_schema = None
        self.dynamodb_format_mapper = None
        self.format_types = {
            str: "S",
            int: "N",
            float: "N",
            bytes: "B",
            list: "SS",
            List[int]: "NS",
            List[float]: "NS",
            List[bytes]: "BS",
            dict: "M",
            list: "L",
        }
        self.format_schema()
        self.generate_item_schema()
        self.generate_key_schema()
        self.generate_update_item_schema()
        self.generate_format_mapper()

    def format_schema(self) -> None:
        try:
            self.schema_template[
                self.schema_template["key_name"]
            ] = self.schema_template["key_type"]
            self.key_template = {
                self.schema_template.pop("key_name"): self.schema_template.pop(
                    "key_type"
                )
            }
        except KeyError as error:
            raise ValidationWrongKeyError(error) from error

    def generate_item_schema(self) -> None:
        self.new_item_schema = Schema(
            {
                attribute: And(Use(data_type))
                for attribute, data_type in self.schema_template.items()
            }
        )

    def generate_key_schema(self) -> None:
        self.dynamodb_key_schema = Schema(
            {
                attribute: And(Use(data_type))
                for attribute, data_type in self.key_template.items()
            }
        )

    def generate_update_item_schema(self) -> None:
        self.update_item_schema = Schema(
            {
                Optional(attribute)
                if attribute not in self.key_template
                else attribute: And(Use(data_type))
                for attribute, data_type in self.schema_template.items()
            }
        )

    def generate_format_mapper(self) -> None:
        self.dynamodb_format_mapper = {
            attribute: {"dynamodb_type": self.format_types[data_type]}
            for attribute, data_type in self.schema_template.items()
        }

    def generate_expression_mapper(self) -> None:
        # seperate function to recursively check keys in template
        # loop through schema_template
        # if keys is false increase split of expression until the full word is used
        # else use just the first letter of the word
        # generate expression mapper dict

        pass

    def validation_schema(self, validation_type: str) -> Union[Schema, Exception]:
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
        response: Dict[str, Dict[str, str]],
        validated_new_attributes: Dict[str, Dict[str, str]],
    ) -> Union[bool, Exception]:
        """Validate that the attributes required to be updated have been updated by comparing the response dictionary to the converted attributes dictionary

        Args:
            response (Dict[str, Dict[str, str]]): Response output from the push_update method \
            validated_new_attributes (Dict[str, Dict[str, str]]): new_attributes that have been converted by using the validate_item_to_db_format

        Returns:
            Union[bool, Exception]: Returns True if the response data is correct or raises Exception if failed
        """
        try:
            if response["Attributes"] == validated_new_attributes:
                return True
            if response["Attributes"] != validated_new_attributes:
                raise ValidationIncorrectAttributesError
        except KeyError as error:
            raise ValidationFailedAttributesUpdateError from error
