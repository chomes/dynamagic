import boto3
from typing import Dict

class DeleteItem:
    def __init__(self, aws_region: str, dynnamodb_table: str) -> None:
        self.client = boto3.client('dynamodb', region_name=aws_region)
        self.dynnamodb_table = dynnamodb_table
    
    def remove_item(self, key: str) -> Dict[str, int]:
        """Delete Item from the database

        Args:
            key (str): Dynamodb Key
        
        Returns:
            Dict[str, int]: status code of the deleted item status
        """
        try:
            self.client.delete_item(TableName=self.table,
            Key=key)
            return {'status_code': 200, 'message': 'Item was attempted to be deleted, validate to confirm'}
        except self.client.exceptions.ParamValidationError:
            return {'status_code': 400, 'message': 'You have provided the wrong type for the key, it must be a dict, please try again'}
            