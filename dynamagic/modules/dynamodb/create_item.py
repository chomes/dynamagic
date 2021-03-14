import boto3

class CreateItem:
    def __init__(self, aws_region: str, dynamodb_table: str) -> None:
        self.client = boto3.client('dynamodb', region_name=aws_region)
        self.dynamodb_table = dynamodb_table
    
    def add_item(self,dynamodb_item: dict) -> bool:
        """Add item to the database, to be used in conjunction with the validation module after checking the data first.

        Args:
            dynamodb_item (dict): Data that has been formated and ready to be added to the database

        Returns:
            bool: returns True after adding the item.
        """
        self.client.put_item(
            TableName=self.dynamodb_table,
            Item=dynamodb_item
        )
        return True
        
