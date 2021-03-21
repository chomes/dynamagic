class ValidationWrongKeyError(Exception):
    def __init__(self, data: str) -> None:
        self.data = data
    
    def __str__(self) -> str:
        return f'Key {self.data} is not part of this the schema for this data, please try again'

class ValidationMissingKeyError(Exception):
    def __init__(self, data: str) -> None:
        self.data = data

    def __str__(self) -> str:
        return f'Key {self.data} was missing from the schema from this data, please try again'

class ValidationIncorrectKeyTypeError(Exception):
    def __init__(self, data: str) -> None:
        self.data = data
    
    def __str__(self) -> str:
        return f'DynamoDb Key {self.data} is either not 10 characters long or cannot convert to an int, please try again'

class ValidationWrongSchemaTypeError(Exception):
    def __init__(self, data: str) -> None:
        self.data = data
    
    def __str__(self) -> str:
        return f'The schema type {self.data} is not a correct schema type, please choose either new_item, '\
        'delete_item or update_item and try again'

class ValidationNoNewAttributesError(Exception):     
    def __str__(self) -> str:
        return 'There are no new attributes being added, please check the data and try again'

class ValidationIncorrectAttributesError(Exception):
    def __str__(self) -> str:
        return 'The items did not update as expected, please try again'

class ValidationFailedAttributesUpdateError(Exception):
    def __str__(self) -> str:
        return 'Attributes do not exist for this item, please check the key is correct and try again'

class DynamoDbWrongKeyError(Exception):
    def __str__(self) -> str:
        return 'The item you tried to fetch does not exist, please check the key is correct and try again'

class DynamoDbInvalidTableError(Exception):
    def __str__(self) -> str:
        return 'Either the table does not exist or there are no items populated yet, '\
            'please check and try again'
        