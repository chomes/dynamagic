import boto3

class add_item:
    def __init__(self, aws_region: str, dynamodb_table: str) -> None:
        self.client = boto3.client('dynamodb', region_name=aws_region)
        self.dynamodb_table = dynamodb_table
    
    def create_new_item(self,dynamodb_item: dict) -> bool:
        self.client.put_item(
            TableName=self.dynamodb_table,
            Item=dynamodb_item
        )
        return True
        
