from pprint import pprint
import boto3
from botocore.exceptions import ClientError
import time

DYNAMO_DB = 'dynamodb'
ENDPOINT_URL = 'http://localhost:8000'
TABLE_NAME = 'todoTablelocal'

class todoTable:
    def create_todo_table(self, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource(DYNAMO_DB, endpoint_url=ENDPOINT_URL)
    
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
    
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
        # assert table.table_status == 'ACTIVE'
    
        return table

    def delete_todo_table(self, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource(DYNAMO_DB, endpoint_url=ENDPOINT_URL)

        table = dynamodb.Table(TABLE_NAME)
        table.delete()
        
    def put_todo(self, text, uuid, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource(DYNAMO_DB, endpoint_url=ENDPOINT_URL)
    
        table = dynamodb.Table(TABLE_NAME)
        timestamp = str(time.time())
    
        try:
            response = table.put_item(
            Item = {
                'id': uuid,
                'text': text,
                'checked': False,
                'createdAt': timestamp,
                'updatedAt': timestamp,
            })
        
        except ClientError as e:
            #print(e.response['Error']['Message'])
            pass
        else:
            return response

    def get_todo(self, uuid, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource(DYNAMO_DB, endpoint_url=ENDPOINT_URL)
    
        table = dynamodb.Table(TABLE_NAME)
    
        try:
            response = table.get_item(
                Key={
                    'id': uuid
                 }
             )
        except ClientError as e:
            #print(e.response['Error']['Message'])
            pass
        else:
            return response

    def update_todo(self, text, uuid, checked, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource(DYNAMO_DB, endpoint_url=ENDPOINT_URL)
    
        table = dynamodb.Table(TABLE_NAME)
        timestamp = str(time.time())
    
        try:
            response = table.update_item(
                Key={
                    'id': uuid
                },
                ExpressionAttributeNames={
                    '#todo_text': 'text',
                },
                ExpressionAttributeValues={
                    ':text': text,
                    ':checked': checked,
                    ':updatedAt': timestamp,
                },
                UpdateExpression='SET #todo_text = :text, '
                                 'checked = :checked, '
                                 'updatedAt = :updatedAt',
                ReturnValues='ALL_NEW',
            )
        except ClientError as e:
            #print(e.response['Error']['Message'])
            pass
        else:
            return response

    def delete_todo(self, uuid, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource(DYNAMO_DB, endpoint_url=ENDPOINT_URL)
    
        table = dynamodb.Table(TABLE_NAME)
    
        try:
            # delete the todo from the database
            response = table.delete_item(
                Key={
                    'id': uuid
                }
            )
        except ClientError as e:
            #print(e.response['Error']['Message'])
            pass
        else:
            return response

if __name__ == '__main__':
    handler = todoTable()
    todo_table = handler.create_todo_table()
    print("Table status:", todo_table.table_status)