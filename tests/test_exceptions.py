from unittest import TestCase
import unittest
from dynamagic.modules.exceptions import ValidationIncorrectAttributesError, ValidationNoNewAttributesError, ValidationWrongKeyError, ValidationMissingKeyError, ValidationIncorrectKeyTypeError, \
ValidationWrongSchemaTypeError, ValidationFailedAttributesUpdateError, DynamoDbInvalidTableError, DynamoDbWrongKeyError

class TestExceptions(unittest.TestCase):
    def test_ValidationIncorrectAttributesError(self):
        self.assertEqual(ValidationIncorrectAttributesError().__str__(), 'The items did not update as expected, please try again')
    
    def test_ValidationNoNewAttributeserror(self):
        self.assertEqual(ValidationNoNewAttributesError().__str__(), 'There are no new attributes being added, please check the data and try again')
    
    def test_ValidationWrongKeyError(self):
        self.assertEqual(ValidationWrongKeyError(data='Comic').__str__(), f'Key Comic is not part of this the schema for this data, please try again')
    
    def test_ValidationMissingKeyError(self):
        self.assertEqual(ValidationMissingKeyError(data='age').__str__(), 'Key age was missing from the schema from this data, please try again')
    
    def test_ValidationIncorrectKeyTypeError(self):
        self.assertEqual(ValidationIncorrectKeyTypeError(data='falaffel'), 
        'DynamoDb Key falaffel is either not 10 characters long or cannot convert to an int, please try again')
    
    

    

    
   
