import boto3
import pytest

import json

from moto import mock_sqs, mock_dynamodb2
from moto.dynamodb2 import dynamodb_backend2


# Becasue the boto clients are instantiated at module level, we need to call this manually at the start
m_sqs = mock_sqs()
m_dynamodb = mock_dynamodb2()
m_sqs.start()
m_dynamodb.start()

import index

SOURCES = [
    'https://aws.amazon.com/blogs/aws/category/serverless/feed/', # The serverless blog
    'https://aws.amazon.com/blogs/developer/feed/', # The AWS developer blog
    'https://aws.amazon.com/blogs/infrastructure-and-automation/feed/', # The Infrastructure and Automation blog
]

EXTRA_SOURCE = 'https://aws.amazon.com/blogs/opensource/feed/' # The Open Source Blog

SOURCES_2 = SOURCES.copy()
SOURCES_2.append(EXTRA_SOURCE)

def test_handler(monkeypatch):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.create_table(
        TableName='SourcesTable',
        KeySchema = [
            {
                'AttributeName': 'source',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'source',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName='SourcesTable')


    for source in SOURCES:
        table.put_item(Item={'source': source}) 

    monkeypatch.setenv('DYNAMO_TABLE', table.name)

    sqs = boto3.resource('sqs')
    queue = sqs.create_queue(QueueName='test')

    monkeypatch.setenv('QUEUE_URL', queue.url)

   
    index.handler({}, {})

    responses = []
    responses.append(queue.receive_messages(MessageAttributeNames = ['All'])[0])
    responses.append(queue.receive_messages(MessageAttributeNames = ['All'])[0])
    responses.append(queue.receive_messages(MessageAttributeNames = ['All'])[0])
    assert [json.loads(response.body)['source'] for response in responses] == SOURCES

    table.put_item(Item={'source': EXTRA_SOURCE})

    index.handler({}, {})

    responses = []
    responses.append(queue.receive_messages(MessageAttributeNames = ['All'])[0])
    responses.append(queue.receive_messages(MessageAttributeNames = ['All'])[0])
    responses.append(queue.receive_messages(MessageAttributeNames = ['All'])[0])
    responses.append(queue.receive_messages(MessageAttributeNames = ['All'])[0])
    assert [json.loads(response.body)['source'] for response in responses] == SOURCES_2    

#m_sqs.stop()
#m_dynamodb.stop()