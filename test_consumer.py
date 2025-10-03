import pytest
import boto3
from moto import mock_aws
import json
import os
import sys
from consumer import Consumer, parse_arguments

BUCKET = "test-bucket"

#s3 client fixture
@pytest.fixture 
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3   # provide to tests
        
#dynamodb client fixture
@pytest.fixture
def dynamo_client():
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName='Widgets',
            KeySchema=[
                {
                    'AttributeName': 'widget_id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'widget_id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        yield dynamodb  # provide to tests

#test initialization of Consumer class with dynamo dest table
def test_consumer_init_dynamo(s3_client, dynamo_client):
    class Args:
        region = "us-east-1"
        request_bucket = BUCKET
        dynamodb_widget_table = "widget-table"
        widget_bucket = None
        queue_wait_timeout = 1

    args = Args()
    consumer = Consumer(args)
    assert consumer.s3_client is not None
    assert consumer.request_bucket_name == BUCKET
    assert consumer.dynamo_client is not None
    assert consumer.dynamo_widget_table_name == "widget-table"
    assert consumer.store_in_dynamo is True
    
#test initialization of Consumer class with s3 dest bucket
def test_consumer_init_s3(s3_client):
    class Args:
        region = "us-east-1"
        request_bucket = BUCKET
        dynamodb_widget_table = None
        widget_bucket = "widget-bucket"
        queue_wait_timeout = 1

    args = Args()
    consumer = Consumer(args)
    assert consumer.s3_client is not None
    assert consumer.request_bucket_name == BUCKET
    assert consumer.s3_widget_bucket_name == "widget-bucket"
    assert consumer.store_in_dynamo is False

#test widget processing logic
def test_process_widgets(s3_client):
    class Args:
        region = "us-east-1"
        request_bucket = BUCKET
        dynamodb_widget_table = None
        widget_bucket = "widget-bucket"
        queue_wait_timeout = 1
        widget_key_prefix = "widgets/"

    args = Args()
    consumer = Consumer(args)
    #create the widget bucket
    s3_client.create_bucket(Bucket=args.widget_bucket)
    
    #provided sample request
    request = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    s3_client.put_object(Bucket=BUCKET, Key="request.json", Body=json.dumps(request))
    
    consumer.process_widgets()
    
    #check if request is deleted from queue
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert 'Contents' not in response
    
    #verify widget was created in widget bucket
    response = s3_client.list_objects_v2(Bucket=args.widget_bucket)
    assert 'Contents' in response
    
    #TODO: verify this works with update and delete requests in the future



#test check s3 (request bucket) empty function



#test retrieve s3 request function



#test widget create function


#test initialize logger function
def test_initialize_logger():
    class Args:
        request_bucket = BUCKET
        widget_bucket = "widget-bucket"
        region = "us-east-1"
        dynamodb_widget_table = None

    args = Args()
    consumer = Consumer(args)
    assert consumer.logger is not None
    consumer.logger.info("Test log message")
    #verify log file created
    assert os.path.exists("logs/consumer.log")
    
    


#test parse arguments function including defaults
def test_parse_arguments():
    test_args = ["program", "-rb", BUCKET, "-dwt", "widget-table", "-wb", "widget-bucket"]
    sys.argv = test_args
    
    parsed_args = parse_arguments()
    assert parsed_args.request_bucket == BUCKET
    assert parsed_args.dynamodb_widget_table == "widget-table"
    assert parsed_args.widget_bucket == "widget-bucket"
    assert parsed_args.region == 'us-east-1'  # default value
    assert parsed_args.queue_wait_timeout == 10  # default value
    assert parsed_args.widget_key_prefix == 'widgets/'  # default value

#TODO: Test widget delete function
def test_widget_delete():
    pass

#TODO: Test widget update function
def test_widget_update():
    pass


