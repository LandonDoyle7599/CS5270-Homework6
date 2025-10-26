import boto3
import argparse
import time
import json
import logging
import sys
import os

class Consumer:
    def __init__(self, args):
        self.args = args
        self.initialize_logger()
        self.s3_client = boto3.client('s3', region_name=self.args.region)
        
        #source logic
        if self.args.request_bucket:
            self.request_bucket_name = self.args.request_bucket
            self.retrieval_method = "s3"
        elif self.args.request_queue:
            self.sqs_client = boto3.client('sqs', region_name=self.args.region)
            self.request_queue_url = self.sqs_client.get_queue_url(QueueName=self.args.request_queue)['QueueUrl']
            self.retrieval_method = "sqs"
            self.sqs_cache = []
        else:
            self.logger.info("No source for widget requests specified, exiting")
            exit(1) 
        
        #destination logic
        if self.args.dynamodb_widget_table:
            self.dynamo_client = boto3.client('dynamodb', region_name=self.args.region)
            self.dynamo_widget_table_name = self.args.dynamodb_widget_table
            self.store_in_dynamo = True
        elif self.args.widget_bucket:
            self.s3_widget_bucket_name = self.args.widget_bucket
            self.store_in_dynamo = False
        else:
            self.logger.info("No destination for widgets specified, exiting")
            exit(1)


    def process_widgets(self):
        empty_queue = False
        while True:
            #s3 method
            if self.retrieval_method == "s3":
                item = self.s3_client.list_objects_v2(Bucket=self.request_bucket_name, MaxKeys=1)
                if self.check_s3_empty(item):
                    if empty_queue:
                        self.logger.info("Still no requests in bucket, exiting...")
                        break
                    self.logger.info("No requests in bucket, waiting...")
                    time.sleep(self.args.queue_wait_timeout)
                    empty_queue = True
                    continue
                else:
                    empty_queue = False
                request_data = json.loads(self.retrieve_s3_request(item))
                    
            #sqs method
            elif self.retrieval_method == "sqs":
                if len(self.sqs_cache) > 0:
                    item = self.sqs_cache.pop(0)
                    receipt_handle = item['ReceiptHandle']
                    self.sqs_client.delete_message(
                        QueueUrl=self.request_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    request_data = json.loads(item['Body'])
                    continue
                else:
                    response = self.sqs_client.receive_message(
                        QueueUrl=self.request_queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=self.args.queue_wait_timeout
                    )
                    if 'Messages' not in response:
                        if empty_queue:
                            self.logger.info("Still no requests in queue, exiting...")
                            break
                        self.logger.info("No requests in queue, waiting...")
                        empty_queue = True
                        continue
                    else:
                        empty_queue = False
                    self.sqs_cache.extend(response['Messages'])
                    item = self.sqs_cache.pop(0)
                    receipt_handle = item['ReceiptHandle']
                    self.sqs_client.delete_message(
                        QueueUrl=self.request_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    request_data = json.loads(item['Body'])
            
            #processing block
            if request_data['type'] == "create":
                self.widget_create(request_data)
            elif request_data['type'] == "delete":
                self.widget_delete(request_data)
            elif request_data['type'] == "update":
                self.widget_update(request_data)
            
    def check_s3_empty(self, item):
        if 'Contents' not in item:
            return True
        return False
    
    def retrieve_s3_request(self, item):
        request_key = item['Contents'][0]['Key']
        request_object = self.s3_client.get_object(Bucket=self.request_bucket_name, Key=request_key)
        request_data = request_object['Body'].read().decode('utf-8')
        self.s3_client.delete_object(Bucket=self.request_bucket_name, Key=request_key)
        return request_data
                    
    
    def widget_create(self, request_data):
        if self.store_in_dynamo:
            widget = request_data
            item = {
                'id': {'S': widget['widgetId']},
                'owner': {'S': widget['owner']},
                'label': {'S': widget.get('label', '')},
                'description': {'S': widget.get('description', '')},
                'otherAttributes': {'S': json.dumps(widget.get('otherAttributes', []))}
            }
            self.logger.info("Put or update in DynamoDB table widgets a widget with key %s", widget['widgetId'])
            self.dynamo_client.put_item(TableName=self.dynamo_widget_table_name, Item=item)
        else:
            widget = request_data
            formatted_owner = widget['owner'].replace(' ', '-').lower()
            widget_key = f"{self.args.widget_key_prefix}{formatted_owner}{widget['widgetId']}.json"
            self.logger.info("Add to S3 bucket %s a widget with key %s", self.s3_widget_bucket_name, widget_key)
            self.s3_client.put_object(
                Bucket=self.s3_widget_bucket_name,
                Key=widget_key,
                Body=json.dumps(widget),
                ContentType='application/json'
            )
    
    def widget_delete(self, request_data):
        if self.store_in_dynamo:
            widget_id = request_data['widgetId']
            # TODO: log
            self.dynamo_client.delete_item(
                TableName=self.dynamo_widget_table_name,
                Key={'id': {'S': widget_id}}
            )
        else:
            widget = request_data
            formatted_owner = widget['owner'].replace(' ', '-').lower()
            widget_key = f"{self.args.widget_key_prefix}{formatted_owner}{widget['widgetId']}.json"
            # TODO: log
            self.s3_client.delete_object(
                Bucket=self.s3_widget_bucket_name,
                Key=widget_key
            )
    
    def widget_update(self, request_data):
        if self.store_in_dynamo:
            widget = request_data
            item = {
                'id': {'S': widget['widgetId']},
                'owner': {'S': widget['owner']},
                'label': {'S': widget.get('label', '')},
                'description': {'S': widget.get('description', '')},
                'otherAttributes': {'S': json.dumps(widget.get('otherAttributes', []))}
            }
            # TODO: log
            self.dynamo_client.put_item(TableName=self.dynamo_widget_table_name, Item=item)
        else:
            widget = request_data
            formatted_owner = widget['owner'].replace(' ', '-').lower()
            widget_key = f"{self.args.widget_key_prefix}{formatted_owner}{widget['widgetId']}.json"
            # TODO: log
            self.s3_client.put_object(
                Bucket=self.s3_widget_bucket_name,
                Key=widget_key,
                Body=json.dumps(widget),
                ContentType='application/json'
            )
    
    def initialize_logger(self):
        os.makedirs('logs', exist_ok=True)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        file_handler = logging.FileHandler('logs/consumer.log')
        logger.addHandler(handler)
        logger.addHandler(file_handler)
        handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        self.logger = logger
        
    
def parse_arguments():
    arg_parser = argparse.ArgumentParser(description='Consumer for processing widget requests.')
    arg_parser.add_argument('-r', '--region', type=str, default='us-east-1', help='AWS region to use (default=us-east-1)')
    arg_parser.add_argument('-rb', '--request-bucket', type=str, required=True, help='Name of bucket that will contain requests')
    arg_parser.add_argument('-wb', '--widget-bucket', type=str, help='Name of the S3 bucket that holds the widgets')
    arg_parser.add_argument('-wkp', '--widget-key-prefix', type=str, default='widgets/', help='Prefix for widget objects (default=widgets/)')
    arg_parser.add_argument('-dwt', '--dynamodb-widget-table', type=str,  help='Name of the DynamoDB table that holds widgets')
    arg_parser.add_argument('-qwt', '--queue-wait-timeout', type=int, default=10, help='The duration (in seconds) to wait for a message to arrive in the request when trying to receive a message (default=10)')
    arg_parser.add_argument('-rq', '--request-queue', type=str,  help='Name of the SQS queue that will contain requests')
    # arg_parser.add_argument('-mrt', '--max-runtime', type=int, default=0, help='Maximum runtime in milliseconds, with 0 meaning no maximum (default=0)')
    # arg_parser.add_argument('-p', '--profile', type=str, default='default', help='Name of AWS profile to use for credentials (default=default)')
    # arg_parser.add_argument('-uop', '--use-owner-in-prefix', type=bool, default=False, help="Use the owner's name in the object's prefix when storing in S3")
    # arg_parser.add_argument('-pdbc', '--pdb-conn', type=str, help='Postgres Database connection string')
    # arg_parser.add_argument('-pdbu', '--pdb-username', type=str, help='Postgres Database username')
    # arg_parser.add_argument('-pdbp', '--pdb-password', type=str, help='Password for the Postgres database user')
    # arg_parser.add_argument('-qvt', '--queue-visibility-timeout', type=int, default=2, help='The duration (in seconds) to the messages received from the queue are hidden from others (default=2)')
    return arg_parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    consumer = Consumer(args)
    consumer.process_widgets()