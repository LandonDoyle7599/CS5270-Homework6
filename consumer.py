import boto3
import argparse
import time
import json

class Consumer:
    def __init__(self):
        self.args = self.parse_arguments()
        self.s3_client = boto3.client('s3', region_name=self.args.region)
        self.request_bucket_name = self.args.request_bucket
        
        if self.args.dynamodb_widget_table:
            self.dynamo_client = boto3.client('dynamodb', region_name=self.args.region)
            self.dynamo_widget_table_name = self.args.dynamodb_widget_table
            self.store_in_dynamo = True
            
        elif self.args.widget_bucket:
            self.s3_widget_bucket_name = self.args.widget_bucket
            self.store_in_dynamo = False
            
        else:
            print("No destination for widgets specified, exiting")
            exit(1)


    def process_widgets(self):
        empty_queue = False
        while True:
            #FUTURE: leave room for different retrieval methods
            if self.check_s3_empty():
                if empty_queue:
                    print("Still no requests in bucket, exiting...")
                    break
                print("No requests in bucket, waiting...")
                time.sleep(self.args.queue_wait_timeout)
                empty_queue = True
                continue
            else:
                empty_queue = False
            
            request_data = json.loads(self.retrieve_s3_request())
            #process request based on type
            if request_data['type'] == "create":
                self.widget_create(request_data)
            elif request_data['type'] == "delete":
                self.widget_delete(request_data)
            elif request_data['type'] == "update":
                self.widget_update(request_data)
            
    def check_s3_empty(self):
        item = self.s3_client.list_objects_v2(Bucket=self.request_bucket_name, MaxKeys=1)
        if 'Contents' not in item:
            return True
        return False
    
    def retrieve_s3_request(self):
        item = self.s3_client.list_objects_v2(Bucket=self.request_bucket_name, MaxKeys=1)
        request_key = item['Contents'][0]['Key']
        print(f"Processing request {request_key}")
        request_object = self.s3_client.get_object(Bucket=self.request_bucket_name, Key=request_key)
        request_data = request_object['Body'].read().decode('utf-8')
        print(f"Request data: {request_data}")
        self.s3_client.delete_object(Bucket=self.request_bucket_name, Key=request_key)
        print(f"Deleted request {request_key} from bucket {self.request_bucket_name}")
        return request_data
                    
    
    def widget_create(self, request_data):
        if self.store_in_dynamo:
            print("Storing widget in DynamoDB")
            widget = request_data
            item = {
                'id': {'S': widget['widgetId']},
                'owner': {'S': widget['owner']},
                'label': {'S': widget.get('label', '')},
                'description': {'S': widget.get('description', '')},
                'otherAttributes': {'S': json.dumps(widget.get('otherAttributes', []))}
            }
            
            self.dynamo_client.put_item(TableName=self.dynamo_widget_table_name, Item=item)
            print(f"Widget {widget['widgetId']} stored in DynamoDB table {self.dynamo_widget_table_name}")
        else:
            print("Storing widget in S3")
            widget = request_data
            widget_key = f"{self.args.widget_key_prefix}{widget['widgetId']}.json"
            self.s3_client.put_object(
                Bucket=self.s3_widget_bucket_name,
                Key=widget_key,
                Body=json.dumps(widget),
                ContentType='application/json'
            )
            print(f"Widget {widget['widgetId']} stored in S3 bucket {self.s3_widget_bucket_name} with key {widget_key}")
    
    def widget_delete(self, request_data):
        #TODO: implement in future assignment
        print("Processing delete request")
        return
    
    def widget_update(self, request_data):
        #TODO: implement in future assignment
        print("Processing update request")
        return
        
    
    def parse_arguments(self):
        arg_parser = argparse.ArgumentParser(description='Consumer for processing widget requests.')
        arg_parser.add_argument('-r', '--region', type=str, default='us-east-1', help='AWS region to use (default=us-east-1)')
        arg_parser.add_argument('-rb', '--request-bucket', type=str, required=True, help='Name of bucket that will contain requests')
        arg_parser.add_argument('-wb', '--widget-bucket', type=str, help='Name of the S3 bucket that holds the widgets')
        arg_parser.add_argument('-wkp', '--widget-key-prefix', type=str, default='widgets/', help='Prefix for widget objects (default=widgets/)')
        arg_parser.add_argument('-dwt', '--dynamodb-widget-table', type=str,  help='Name of the DynamoDB table that holds widgets')
        arg_parser.add_argument('-qwt', '--queue-wait-timeout', type=int, default=10, help='The duration (in seconds) to wait for a message to arrive in the request when trying to receive a message (default=10)')
        # arg_parser.add_argument('-mrt', '--max-runtime', type=int, default=0, help='Maximum runtime in milliseconds, with 0 meaning no maximum (default=0)')
        # arg_parser.add_argument('-p', '--profile', type=str, default='default', help='Name of AWS profile to use for credentials (default=default)')
        # arg_parser.add_argument('-uop', '--use-owner-in-prefix', type=bool, default=False, help="Use the owner's name in the object's prefix when storing in S3")
        # arg_parser.add_argument('-rq', '--request-queue', type=str,  help='URL of queue that will contain requests')
        # arg_parser.add_argument('-pdbc', '--pdb-conn', type=str, help='Postgres Database connection string')
        # arg_parser.add_argument('-pdbu', '--pdb-username', type=str, help='Postgres Database username')
        # arg_parser.add_argument('-pdbp', '--pdb-password', type=str, help='Password for the Postgres database user')
        # arg_parser.add_argument('-qvt', '--queue-visibility-timeout', type=int, default=2, help='The duration (in seconds) to the messages received from the queue are hidden from others (default=2)')
        return arg_parser.parse_args()

if __name__ == '__main__':
    consumer = Consumer()
    consumer.process_widgets()