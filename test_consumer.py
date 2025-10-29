import pytest
import boto3
from moto import mock_aws
import json
import os
import sys
from consumer import Consumer, parse_arguments

BUCKET = "test-bucket"
QUEUE = "test-queue"

#s3 client fixture
@pytest.fixture 
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3   # provide to tests
        
@pytest.fixture
def sqs_client():
    with mock_aws():
        sqs = boto3.client("sqs", region_name="us-east-1")
        queue = sqs.create_queue(QueueName=QUEUE)
        yield sqs  # provide to tests
        
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
def sqs_src_s3_dest():
    class Args:
        region = "us-east-1"
        request_queue = QUEUE
        request_bucket = None
        dynamodb_widget_table = None
        widget_bucket = "widget-bucket"
        widget_key_prefix = "widgets/"
        queue_wait_timeout = 10

    args = Args()
    consumer = Consumer(args)
    return consumer
        
@pytest.fixture
def consumer_s3_dest():
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
def consumer_dynamo_dest():
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
def test_consumer_init_dynamo(consumer_dynamo_dest):
    consumer = consumer_dynamo_dest
    assert consumer.s3_client is not None
    assert consumer.request_bucket_name == BUCKET
    assert consumer.dynamo_client is not None
    assert consumer.dynamo_widget_table_name == "widget-table"
    assert consumer.store_in_dynamo is True
    
#test initialization of Consumer class with s3 dest bucket
def test_consumer_init_s3(consumer_s3_dest):
    consumer = consumer_s3_dest
    assert consumer.s3_client is not None
    assert consumer.request_bucket_name == BUCKET
    assert consumer.s3_widget_bucket_name == "widget-bucket"
    assert consumer.store_in_dynamo is False

#test widget processing logic
def test_process_create_widgets(s3_client, consumer_s3_dest):
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
    
def test_process_create_widgets_sqs(sqs_client, sqs_src_s3_dest):
    consumer = sqs_src_s3_dest
    #create the widget bucket
    sqs = sqs_client
    queue_url = sqs.get_queue_url(QueueName=QUEUE)['QueueUrl']
    consumer.request_queue_url = queue_url
    s3_client = consumer.s3_client
    s3_client.create_bucket(Bucket=consumer.s3_widget_bucket_name)
    
    #provided sample request
    request = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(request))
    
    consumer.retrieval_method = "sqs"
    consumer.process_widgets()
    
    #check if request is deleted from queue
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert 'Messages' not in response
    
    #verify widget was created in widget bucket
    response = s3_client.list_objects_v2(Bucket=consumer.s3_widget_bucket_name)
    assert 'Contents' in response
    
def test_process_delete_widgets(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    #create the widget bucket
    s3_client.create_bucket(Bucket=consumer.s3_widget_bucket_name)
    
    #provided sample delete request
    request = {"type":"delete","requestId":"21cf74b2-dbf2-46bb-a274-b8e3eac679bf","widgetId":"e50b4381-4332-438f-bcec-bd2b8c0fa5ed","owner":"Sue Smith"}
    s3_client.put_object(Bucket=BUCKET, Key="request.json", Body=json.dumps(request))
    
    consumer.process_widgets()
    
    #check if request is deleted from queue
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert 'Contents' not in response
    
    #verify widget was deleted in widget bucket (should be empty)
    response = s3_client.list_objects_v2(Bucket=consumer.s3_widget_bucket_name)
    assert 'Contents' not in response
    
def test_process_update_widgets(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    #create the widget bucket
    s3_client.create_bucket(Bucket=consumer.s3_widget_bucket_name)
    
    #first create a widget to update
    request_create = {"type":"create","requestId":"abc12345-6789-0abc-def1-234567890abc","widgetId":"6984abeb-5b24-42eb-93cc-3a5bef6b4b8a","owner":"Mary Matthews","description":"PUMMCL","otherAttributes":[{"name":"size","value":"745"},{"name":"size-unit","value":"cm"},{"name":"height","value":"879"},{"name":"height-unit","value":"cm"},{"name":"width-unit","value":"cm"},{"name":"length","value":"793"},{"name":"price","value":"50.96"},{"name":"quantity","value":"311"},{"name":"note","value":"MYEVVLRLAWVRZTQIMWRTJFDZTSJNJTWXQBFXOBABMNGJDCWRJMAGVYSWWAPYWDCHSDKFAURWSBHGABSMVKRLQZKXEXJLNXZU"}]}
    s3_client.put_object(Bucket=BUCKET, Key="request.json", Body=json.dumps(request_create))
    consumer.process_widgets()
    
    #now update the widget
    request_update = {"type":"update","requestId":"d61c3e72-1a66-4cfa-9162-56712e4580d8","widgetId":"6984abeb-5b24-42eb-93cc-3a5bef6b4b8a","owner":"Mary Matthews","description":"PUMMCL","otherAttributes":[{"name":"size","value":"745"},{"name":"size-unit","value":"cm"},{"name":"height","value":"879"},{"name":"height-unit","value":"cm"},{"name":"width-unit","value":"cm"},{"name":"length","value":"793"},{"name":"price","value":"50.96"},{"name":"quantity","value":"311"},{"name":"note","value":"MYEVVLRLAWVRZTQIMWRTJFDZTSJNJTWXQBFXOBABMNGJDCWRJMAGVYSWWAPYWDCHSDKFAURWSBHGABSMVKRLQZKXEXJLNXZU"}]}
    s3_client.put_object(Bucket=BUCKET, Key="request.json", Body=json.dumps(request_update))
    consumer.process_widgets()
    
    # check if widget was updated
    owner = request_update['owner'].replace(' ', '-').lower()
    expected_key = f"{consumer.args.widget_key_prefix}{owner}{request_update['widgetId']}.json"
    response = s3_client.get_object(Bucket=consumer.s3_widget_bucket_name, Key=expected_key)
    widget_data = json.loads(response['Body'].read().decode('utf-8'))
    

#test check s3 (request bucket) empty function
def test_check_s3_empty(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert consumer.check_s3_empty(response) is True
    
    s3_client.put_object(Bucket=BUCKET, Key="object.json", Body=json.dumps({"test": "data"}))
    response = s3_client.list_objects_v2(Bucket=BUCKET)
    assert consumer.check_s3_empty(response) is False



#test retrieve s3 request function
def test_parse_and_delete_s3_request(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    request = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    s3_client.put_object(Bucket=BUCKET, Key="request.json", Body=json.dumps(request))
    
    item = s3_client.list_objects_v2(Bucket=BUCKET, MaxKeys=1)
    request_data = consumer.parse_and_delete_s3_request(item)
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

def test_widget_delete(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    s3_client.create_bucket(Bucket=consumer.s3_widget_bucket_name)
    #first create a widget to delete
    request_create = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    consumer.widget_create(request_create)
    
    #now delete the widget
    request_delete = {"type":"delete","requestId":"12345678-1234-1234-1234-1234567890ab","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    consumer.widget_delete(request_delete)
    
    # check if widget was deleted
    response = s3_client.list_objects_v2(Bucket=consumer.s3_widget_bucket_name)    
    if 'Contents' in response:
        keys = [obj['Key'] for obj in response['Contents']]
        owner = request_delete['owner'].replace(' ', '-').lower()
        expected_key = f"{consumer.args.widget_key_prefix}{owner}{request_delete['widgetId']}.json"
        assert expected_key not in keys
        
def test_update_widget(s3_client, consumer_s3_dest):
    consumer = consumer_s3_dest
    s3_client.create_bucket(Bucket=consumer.s3_widget_bucket_name)
    #first create a widget to update
    request_create = {"type":"create","requestId":"9ca0d18a-57ab-4ac6-89dc-146f092ea9fe","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"QQGRLNZY","description":"JLVIEOHPQXKDXKPHOHFOXNKSYDEWRNEQWMPVPVHZVJCHCUIIWSRXITPWOKTMHULMVUNWGRREQYPQYO","otherAttributes":[{"name":"size","value":"926"},{"name":"height","value":"828"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"1.6420901"}]}
    consumer.widget_create(request_create)
    
    #now update the widget
    request_update = {"type":"update","requestId":"87654321-4321-4321-4321-ba0987654321","widgetId":"ad0bb9e1-28e9-46e0-ad08-8192f4d3b6c6","owner":"John Jones","label":"UPDATED_LABEL","description":"UPDATED_DESCRIPTION","otherAttributes":[{"name":"size","value":"999"},{"name":"height","value":"888"},{"name":"height-unit","value":"cm"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"5.0000000"}]}
    consumer.widget_update(request_update)
    
    # check if widget was updated
    owner = request_update['owner'].replace(' ', '-').lower()
    expected_key = f"{consumer.args.widget_key_prefix}{owner}{request_update['widgetId']}.json"
    response = s3_client.get_object(Bucket=consumer.s3_widget_bucket_name, Key=expected_key)
    widget_data = json.loads(response['Body'].read().decode('utf-8'))
    assert widget_data['label'] == "UPDATED_LABEL"
    assert widget_data['description'] == "UPDATED_DESCRIPTION"
    assert widget_data['otherAttributes'][0]['value'] == "999"
    
    
    
    
#test initialize logger function
def test_initialize_logger(consumer_s3_dest):
    consumer = consumer_s3_dest
    assert consumer.logger is not None
    consumer.logger.info("Test log message")
    #verify log file created
    assert os.path.exists("logs/consumer.log")
    
    
#test parse arguments function including defaults
def test_parse_arguments():
    test_args = [
        "consumer.py",
        "--region", "us-west-2",
        "--request-bucket", "my-request-bucket",
        "--dynamodb-widget-table", "my-widget-table",
        "--widget-bucket", "my-widget-bucket",
        "--widget-key-prefix", "my-widgets/",
        "--queue-wait-timeout", "15"
    ]
    sys.argv = test_args
    args = parse_arguments()
    
    assert args.region == "us-west-2"
    assert args.request_bucket == "my-request-bucket"
    assert args.dynamodb_widget_table == "my-widget-table"
    assert args.widget_bucket == "my-widget-bucket"
    assert args.widget_key_prefix == "my-widgets/"
    assert args.queue_wait_timeout == 15
    
    #test defaults
    test_args_default = [
        "consumer.py",
        "--region", "us-west-2",
        "--request-bucket", "my-request-bucket"
    ]
    sys.argv = test_args_default
    args = parse_arguments()
    
    assert args.dynamodb_widget_table is None
    assert args.widget_bucket is None
    assert args.widget_key_prefix == "widgets/"
    assert args.queue_wait_timeout == 10
