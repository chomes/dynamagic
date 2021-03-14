from dynamagic.modules.dynamodb.update_item import UpdateItem
import boto3
from moto import mock_dynamodb2
import unittest

class TestUpdateItem(unittest.TestCase):
    @mock_dynamodb2
    def create_table(self):
        client = boto3.client('dynamodb', region_name='eu-west-2')
        client.create_table(
                TableName='test_table',
            ProvisionedThroughput={
                'ReadCapacityUnits': 140,
                'WriteCapacityUnits': 140
            },
            AttributeDefinitions=[
                {
                    'AttributeName': 'CustomerId',
                    'AttributeType': 'S'
                }
            ],
            KeySchema=[
            {
                    'AttributeName': 'CustomerId',
                    'KeyType': 'HASH'
                } 
            ],
            BillingMode='PROVISIONED')
        client.get_waiter('table_exists').wait(TableName='test_table',
        WaiterConfig={'Delay': 2, 'MaxAttempts': 5})
    
    def test_remove_duplicate_attributes(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.remove_duplicated_attributes(new_attributes={'address': 'Jeff Bezos Candy land road',
    'age': '32'}, old_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {'address': 'Jeff Bezos Candy land road',
    'age': '32'})

    def test_remove_duplicate_attributes_wrong_key(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.remove_duplicated_attributes(new_attributes={'address': 'Jeff Bezos Candy land road',
    'age': '32', 'comics': 'Batman'}, old_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})

    def test_remove_all_duplicate_attributes(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.remove_duplicated_attributes(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}, old_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {})

    def test_generate_update_expression(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.generate_update_expression(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}), 'SET #AD = :ad, #AG = :ag')

    def test_generate_update_expression_with_wrong_key(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.generate_update_expression(new_attributes={'address': 'Rochet trust drive',
    'age': '35', 'comic': 'Batman'}), {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})

    def test_generate_expression_attribute_names(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.generate_expression_attribute_names(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}), {'#AD': 'address', '#AG': 'age'})

    def test_generate_expression_attribute_names_with_wrong_key(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.generate_expression_attribute_names(new_attributes={'address': 'Rochet trust drive',
    'age': '35', 'comic': 'Batman'}), {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})

    def test_generate_expression_attribute_values(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.generate_expression_attribute_values(new_attributes={'address': 'Rochet trust drive',
    'age': '35'}, dynamodb_format_mapper={'CustomerId': {'dynamodb_type': 'S'}, 'name': {'dynamodb_type': 'S'},
        'address': {'dynamodb_type': 'S'},  'age': {'dynamodb_type': 'S'}, 'car': {'dynamodb_type': 'S'}}),
        {':ad': {'S': 'Rochet trust drive'}, ':ag': {'S': '35'}})
    
    def test_generate_expression_attribute_values_with_wrong_key(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.assertEqual(update_item.generate_expression_attribute_values(new_attributes={'address': 'Rochet trust drive',
    'age': '35', 'comic': 'Batman'}, dynamodb_format_mapper={'CustomerId': {'dynamodb_type': 'S'}, 'name': {'dynamodb_type': 'S'},
        'address': {'dynamodb_type': 'S'},  'age': {'dynamodb_type': 'S'}, 'car': {'dynamodb_type': 'S'}}), 
        {'status_code': 400, 'message': 'You have provided an incorrect attribute please try again'})
    
    @mock_dynamodb2
    def test_push_update(self):
        update_item: UpdateItem = UpdateItem(aws_region='eu-west-2', dynamodb_table='test_table')
        self.create_table()
        client = boto3.client('dynamodb', region_name='eu-west-2')
        client.put_item(TableName='test_table',
        Item={'CustomerId': {'S': '1482328791'}, 'name': {'S': 'James Joseph'}, 
        'address': {'S': 'Jeff Bezos Candy land road'}, 'age': {'S': '32'}, 'car': {'S': 'Black Skoda'}})
        self.assertEqual(update_item.push_update(key={'CustomerId': {'S': '1482328791'}},
        update_expression='SET #AD = :ad, #AG = :ag',
        expression_attribute_names={'#AD': 'address', '#AG': 'age'},
        expression_attribute_values={':ad': {'S': 'Rochet trust drive'}, ':ag': {'S': '35'}})['Attributes'], {'address': {'S': 'Rochet trust drive'},
        'age': {'S': '35'}})


if __name__ == '__main__':
    unittest.main()
