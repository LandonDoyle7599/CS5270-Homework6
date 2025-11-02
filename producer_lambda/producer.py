import json
import boto3
import os

sqs = boto3.client('sqs')

def lambda_handler(event, context):
    QUEUE_URL = os.environ['SQS_QUEUE_URL']

    try:
        body = json.loads(event['body'])
        type = body.get('type')
        valid_types = ['create', 'update', 'delete']
        
        if type not in valid_types:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid type. Must be one of create, update, delete.'})
            }
            
        if 'requestId' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required field: requestId'})
            }
            
        if 'widgetId' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required field: widgetId'})
            }
        
        if 'owner' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required field: owner'})
            }
            
        if type == 'create' and 'label' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required field: label'})
            }
            
        if type == 'create' and 'description' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required field: description'})
            }
    
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(body)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Message sent to SQS', 'MessageId': response['MessageId']})
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error', 'error': str(e)})
        }
