from dynamagic.modules.validation import Validation
import schema
from dynamagic.modules.exceptions import (
    ValidationFailedAttributesUpdateError,
    ValidationIncorrectAttributesError,
    ValidationNoNewAttributesError,
    ValidationWrongSchemaTypeError,
    ValidationWrongKeyError,
    ValidationMissingKeyError,
)
import unittest


class TestValidation(unittest.TestCase):
    @staticmethod
    def generate_schema_template():
        return {
            "key_name": "CustomerId",
            "key_type": str,
            "name": str,
            "address": str,
            "age": str,
            "car": str,
        }

    def test_format_schema(self):
        validation = Validation(table_schema=self.generate_schema_template())
        self.assertEqual(
            (validation.schema_template, validation.key_template),
            (
                {
                    "CustomerId": str,
                    "name": str,
                    "address": str,
                    "age": str,
                    "car": str,
                },
                {"CustomerId": str},
            ),
        )
    
    def test_bad_format_schema(self):
        with self.assertRaises(ValidationWrongKeyError):
            Validation(table_schema={
                    "CustomerId": str,
                    "name": str,
                    "address": str,
                    "age": str,
                    "car": str,
                })
    
    def test_generate_new_item_schema(self):
        validation = Validation(table_schema=self.generate_schema_template())
        self.assertEqual(validation.new_item_schema, Schema())


    def test_creating_validation_class(self):
        validation = Validation(table_schema=self.generate_schema_template())
        self.assertIsInstance(validation, Validation)

    def test_validation_schema_new_item(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validation_schema(validation_type="new_item"),
            validation.new_item_schema,
        )

    def test_validation_schema_updated_item(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validation_schema(validation_type="update_item"),
            validation.update_item_schema,
        )

    def test_validation_schema_delete_item(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validation_schema(validation_type="delete_item"),
            validation.dynamodb_key_schema,
        )

    def test_failed_to_grab_validation_Schema(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertRaises(
            ValidationWrongSchemaTypeError,
            validation.validation_schema,
            validation_type="not a schema",
        )

    def test_validate_item_data_entegrity_new_item(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validate_item_data_entegrity(
                dynamodb_schema=validation.new_item_schema,
                unvalidated_item={
                    "CustomerId": "1482328791",
                    "name": "James Joseph",
                    "address": "Jeff Bezos Candy land road",
                    "age": "32",
                    "car": "Black Skoda",
                },
            ),
            {
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            },
        )

    def test_validate_item_data_entegrity_update_item(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validate_item_data_entegrity(
                dynamodb_schema=validation.update_item_schema,
                unvalidated_item={
                    "CustomerId": "1482328791",
                    "name": "James Joseph",
                    "address": "Jeff Bezos Candy land road",
                },
            ),
            {
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
            },
        )

    def test_validate_item_data_entegrity_delete_item(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validate_item_data_entegrity(
                dynamodb_schema=validation.dynamodb_key_schema,
                unvalidated_item={"CustomerId": "1482328791"},
            ),
            {"CustomerId": "1482328791"},
        )

    def test_validate_item_data_entegrity_int_to_string(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validate_item_data_entegrity(
                dynamodb_schema=validation.new_item_schema,
                unvalidated_item={
                    "CustomerId": 1482328791,
                    "name": "James Joseph",
                    "address": "Jeff Bezos Candy land road",
                    "age": 32,
                    "car": "Black Skoda",
                },
            ),
            {
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            },
        )

    def test_validate_item_data_entegrity_missing_key(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertRaises(
            ValidationMissingKeyError,
            validation.validate_item_data_entegrity,
            dynamodb_schema=validation.new_item_schema,
            unvalidated_item={
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
            },
        )

    def test_validate_item_data_entegrity_wrong_key(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertRaises(
            ValidationWrongKeyError,
            validation.validate_item_data_entegrity,
            dynamodb_schema=validation.update_item_schema,
            unvalidated_item={
                "CustomerId": "1482328790",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "comics": "Batman",
            },
        )

    def test_validate_item_to_db_format(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validate_item_to_db_format(
                dynamodb_item={
                    "CustomerId": "1482328791",
                    "name": "James Joseph",
                    "address": "Jeff Bezos Candy land road",
                    "age": "32",
                    "car": "Black Skoda",
                }
            ),
            {
                "CustomerId": {"S": "1482328791"},
                "name": {"S": "James Joseph"},
                "address": {"S": "Jeff Bezos Candy land road"},
                "age": {"S": "32"},
                "car": {"S": "Black Skoda"},
            },
        )

    def test_validate_item_to_readable_format(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validate_item_to_readable_format(
                dynamodb_item={
                    "CustomerId": {"S": "1482328791"},
                    "name": {"S": "James Joseph"},
                    "address": {"S": "Jeff Bezos Candy land road"},
                    "age": {"S": "32"},
                    "car": {"S": "Black Skoda"},
                }
            ),
            {
                "CustomerId": "1482328791",
                "name": "James Joseph",
                "address": "Jeff Bezos Candy land road",
                "age": "32",
                "car": "Black Skoda",
            },
        )

    def test_validate_new_attributes_exist(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertEqual(
            validation.validate_new_attributes_exist(
                item_attributes={
                    "name": "James Joseph",
                    "address": "Jeff Bezos Candy land road",
                }
            ),
            {"name": "James Joseph", "address": "Jeff Bezos Candy land road"},
        )

    def test_validate_new_attributes_do_not_exist(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        with self.assertRaises(ValidationNoNewAttributesError):
            validation.validate_new_attributes_exist(item_attributes={})

    def test_validate_attributes_updated(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertTrue(
            validation.validate_attributes_updated(
                response={
                    "Attributes": {"age": {"S": "32"}, "car": {"S": "Black Skoda"}}
                },
                validated_new_attributes={
                    "age": {"S": "32"},
                    "car": {"S": "Black Skoda"},
                },
            )
        )

    def test_validate_attributes_not_updated(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertRaises(
            ValidationIncorrectAttributesError,
            validation.validate_attributes_updated,
            response={"Attributes": {"age": {"S": "32"}, "car": {"S": "Black Skoda"}}},
            validated_new_attributes={"age": {"S": "35"}, "car": {"S": "Blue BMW"}},
        )

    def test_validate_attributes_failed_to_update(self):
        validation: Validation = Validation(
            table_schema=self.generate_schema_template()
        )
        self.assertRaises(
            ValidationFailedAttributesUpdateError,
            validation.validate_attributes_updated,
            response={},
            validated_new_attributes={"age": {"S": "32"}, "car": {"S": "Black Skoda"}},
        )


if __name__ == "__main__":
    unittest.main()
