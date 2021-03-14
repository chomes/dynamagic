import boto3
from typing import Dict

class CreateItem:
    def __init__(self, aws_region: str, dynamodb_table: str) -> None:
        self.client = boto3.client('dynamodb', region_name=aws_region)
        self.dynamodb_table = dynamodb_table
    
    def add_item(self,dynamodb_item: dict) -> Dict[str, int]:
        """Add item to the database, to be used in conjunction with the validation module after checking the data first.

        Args:
            dynamodb_item (dict): Data that has been formated and ready to be added to the database

        Returns:
            bool: returns True after adding the item.
        """
        try:
            self.client.put_item(
                TableName=self.dynamodb_table,
                Item=dynamodb_item
            )
            return {'status_code': 200, 'message': 'Item has been added successfully'}
        except self.client.exceptions.ClientError:
            return {'status_code': 400, 'message': 'Failed to add the item, a key was not provided'}
        
