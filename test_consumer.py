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
        
@pytest.fixture
def consumer_s3_dest(s3_client, dynamo_client):
    class Args:
        region = "us-east-1"
        request_bucket = BUCKET
        dynamodb_widget_table = None
        widget_bucket = "widget-bucket"
        widget_key_prefix = "widgets/"
        queue_wait_timeout = 10

    args = Args()
    consumer = Consumer(args)
    return consumer

@pytest.fixture
def consumer_dynamo_dest(s3_client, dynamo_client):
    class Args:
        region = "us-east-1"
        request_bucket = BUCKET
        dynamodb_widget_table = "widget-table"
        widget_bucket = None
        widget_key_prefix = "widgets/"
        queue_wait_timeout = 10

    args = Args()
    consumer = Consumer(args)
    return consumer

#test initialization of Consumer class with dynamo dest table
def test_consumer_init_dynamo(s3_client, dynamo_client, consumer_dynamo_dest):
    consumer = consumer_dynamo_dest
    assert consumer.s3_client is not None
    assert consumer.request_bucket_name == BUCKET
    assert consumer.dynamo_client is not None
    assert consumer.dynamo_widget_table_name == "widget-table"
    assert consumer.store_in_dynamo is True
    
#test initialization of Consumer class with s3 dest bucket
def test_consumer_init_s3(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    assert consumer.s3_client is not None
    assert consumer.request_bucket_name == BUCKET
    assert consumer.s3_widget_bucket_name == "widget-bucket"
    assert consumer.store_in_dynamo is False

#test widget processing logic
def test_process_widgets(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    #create the widget bucket
    s3_client.create_bucket(Bucket=consumer.s3_widget_bucket_name)
    
    #provided sample request
    request = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    s3_client.put_object(Bucket=BUCKET, Key="request.json", Body=json.dumps(request))
    
    consumer.process_widgets()
    
    #check if request is deleted from queue
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert 'Contents' not in response
    
    #verify widget was created in widget bucket
    response = s3_client.list_objects_v2(Bucket=consumer.s3_widget_bucket_name)
    assert 'Contents' in response
    
    #TODO: verify this works with update and delete requests in the future



#test check s3 (request bucket) empty function
def test_check_s3_empty(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert consumer.check_s3_empty(response) is True
    
    s3_client.put_object(Bucket=BUCKET, Key="object.json", Body=json.dumps({"test": "data"}))
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert consumer.check_s3_empty(response) is False



#test retrieve s3 request function
def test_retrieve_s3_request(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    request = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    s3_client.put_object(Bucket=BUCKET, Key="request.json", Body=json.dumps(request))
    
    item = s3_client.list_objects_v2(Bucket=BUCKET, MaxKeys=1)
    request_data = consumer.retrieve_s3_request(item)
    assert json.loads(request_data) == request
    
    #ensure request deleted from queue
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert 'Contents' not in response
    




#test widget create function
def test_widget_create(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    s3_client.create_bucket(Bucket=consumer.s3_widget_bucket_name)
    
    request = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    consumer.widget_create(request)
    
     # check if widget was created
    response = s3_client.list_objects_v2(Bucket=consumer.s3_widget_bucket_name)    
    assert 'Contents' in response
    
    # create expected key
    owner = request['owner'].replace(' ', '-').lower()
    expected_key = f"{consumer.args.widget_key_prefix}{owner}{request['widgetId']}.json"
    
    keys = [obj['Key'] for obj in response['Contents']]
    assert expected_key in keys

#test initialize logger function
def test_initialize_logger(consumer_s3_dest):
    consumer = consumer_s3_dest
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


