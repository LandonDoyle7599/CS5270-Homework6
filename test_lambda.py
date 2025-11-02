import pytest
import boto3
from moto import mock_aws
import json
import os

from producer_lambda.producer import lambda_handler

QUEUE = "test-queue"

@pytest.fixture
def sqs_queue():
    with mock_aws():
        sqs = boto3.client('sqs', region_name='us-east-1')
        queue = sqs.create_queue(QueueName=QUEUE)
        queue_url = queue.get('QueueUrl')
        os.environ['SQS_QUEUE_URL'] = queue_url
        yield sqs
        
def test_create_widget_request(sqs_queue):
    event = {
        'body': json.dumps({
            'type': 'create',
            'requestId': 'request id',
            'widgetId': 'widget id',
            'owner': 'joe',
            'label': 'here is my label',
            'description': 'description description'
        })
    }
    
    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['message'] == 'Message sent to SQS'
    assert 'MessageId' in body
    messages = sqs_queue.receive_message(QueueUrl=os.environ['SQS_QUEUE_URL'], MaxNumberOfMessages=1)
    assert 'Messages' in messages
    
def test_update_widget_request(sqs_queue):
    event = {
        'body': json.dumps({
            'type': 'update',
            'requestId': 'request id',
            'widgetId': 'widget id',
            'owner': 'joe',
            'label': 'here is my label',
            'description': 'description description'
        })
    }
    
    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['message'] == 'Message sent to SQS'
    assert 'MessageId' in body
    messages = sqs_queue.receive_message(QueueUrl=os.environ['SQS_QUEUE_URL'], MaxNumberOfMessages=1)
    assert 'Messages' in messages
    
def test_delete_widget_request(sqs_queue):
    event = {
        'body': json.dumps({
            'type': 'delete',
            'requestId': 'request id',
            'widgetId': 'widget id',
            'owner': 'joe'
        })
    }
    
    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['message'] == 'Message sent to SQS'
    assert 'MessageId' in body
    messages = sqs_queue.receive_message(QueueUrl=os.environ['SQS_QUEUE_URL'], MaxNumberOfMessages=1)
    assert 'Messages' in messages
    
def test_invalid_type_request(sqs_queue):
    event = {
        'body': json.dumps({
            'type': 'invalid', # bad type
            'requestId': 'request id',
            'widgetId': 'widget id',
            'owner': 'joe',
            'label': 'here is my label',
            'description': 'description description'
        })
    }
    
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400
    body = json.loads(response['body'])
    assert body['message'] == 'Invalid type. Must be one of create, update, delete.'
    
def test_missing_field_request(sqs_queue):
    event = {
        'body': json.dumps({
            'type': 'create',
            # no request id
            'widgetId': 'id',
            'owner': 'joe',
            'label': 'here is my label',
            'description': 'description description'
        })
    }
    
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400
    body = json.loads(response['body'])
    assert body['message'] == 'Missing required field: requestId'
