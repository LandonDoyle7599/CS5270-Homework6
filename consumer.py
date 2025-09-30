import boto3
import argparse

class Consumer:
    def __init__(self):
        self.args = self.parse_arguments()
        self.s3_client = boto3.client('s3', region_name=self.args.region)
        self.request_bucket_name = self.args.request_bucket
        
        if self.args.dynamodb_widget_table:
            dynamo_client = boto3.client('dynamodb', region_name=self.args.region)
            self.dynamo_widget_table_name = self.args.dynamodb_widget_table
            self.store_in_dynamo = True
            
        elif self.args.widget_bucket:
            self.s3_widget_bucket_name = self.args.widget_bucket
            self.store_in_dynamo = False
            
        else:
            print("No destination for widgets specified, exiting")
            exit(1)


    def process_widgets(self):
        #read requests one at a time (smallest key first) from request bucket, then delete and create widgets
        while True:
            item = self.s3_client.list_objects_v2(Bucket=self.request_bucket_name, MaxKeys=1)
            if 'Contents' not in item:
                print("No more requests to process, waiting")
                #TODO: Implement wait based on argument length
                continue
            request_key = item['Contents'][0]['Key']
            print(f"Processing request {request_key}")
            request_object = self.s3_client.get_object(Bucket=self.request_bucket_name, Key=request_key)
            request_data = request_object['Body'].read().decode('utf-8')
            print(f"Request data: {request_data}")
            self.s3_client.delete_object(Bucket=self.request_bucket_name, Key=request_key)
            print(f"Deleted request {request_key} from bucket {self.request_bucket_name}")
            
            #process request based on type
            if '"type": "WidgetCreateRequest"' in request_data:
                self.widget_create()
            elif '"type": "WidgetDeleteRequest"' in request_data:
                self.widget_delete()
            elif '"type": "WidgetUpdateRequest"' in request_data:
                self.widget_update()
            
            
    def widget_create(self):
        return
    
    def widget_delete(self):
        #TODO: implement in future assignment
        print("Processing delete request")
        return
    
    def widget_update(self):
        #TODO: implement in future assignment
        print("Processing update request")
        return
        
    
    def parse_arguments(self):
        arg_parser = argparse.ArgumentParser(description='Consumer for processing widget requests.')
        arg_parser.add_argument('-p', '--profile', type=str, default='default', help='Name of AWS profile to use for credentials (default=default)')
        arg_parser.add_argument('-r', '--region', type=str, default='us-east-1', help='AWS region to use (default=us-east-1)')
        arg_parser.add_argument('-mrt', '--max-runtime', type=int, default=0, help='Maximum runtime in milliseconds, with 0 meaning no maximum (default=0)')
        arg_parser.add_argument('-rb', '--request-bucket', type=str, required=True, help='Name of bucket that will contain requests')
        arg_parser.add_argument('-uop', '--use-owner-in-prefix', type=bool, default=False, help="Use the owner's name in the object's prefix when storing in S3")
        arg_parser.add_argument('-rq', '--request-queue', type=str,  help='URL of queue that will contain requests')
        arg_parser.add_argument('-wb', '--widget-bucket', type=str, help='Name of the S3 bucket that holds the widgets')
        arg_parser.add_argument('-wkp', '--widget-key-prefix', type=str, default='widgets/', help='Prefix for widget objects (default=widgets/)')
        arg_parser.add_argument('-dwt', '--dynamodb-widget-table', type=str,  help='Name of the DynamoDB table that holds widgets')
        # arg_parser.add_argument('-pdbc', '--pdb-conn', type=str, help='Postgres Database connection string')
        # arg_parser.add_argument('-pdbu', '--pdb-username', type=str, help='Postgres Database username')
        # arg_parser.add_argument('-pdbp', '--pdb-password', type=str, help='Password for the Postgres database user')
        arg_parser.add_argument('-qwt', '--queue-wait-timeout', type=int, default=10, help='The duration (in seconds) to wait for a message to arrive in the request when trying to receive a message (default=10)')
        arg_parser.add_argument('-qvt', '--queue-visibility-timeout', type=int, default=2, help='The duration (in seconds) to the messages received from the queue are hidden from others (default=2)')
        return arg_parser.parse_args()

if __name__ == '__main__':
    consumer = Consumer()



#request format in JSON
# {
#   "$schema": "http://json-schema.org/draft-04/schema#",
#   "type": "object",
#   "properties": {
#     "type": {
#       "type": "string",
#       "pattern": "WidgetCreateRequest|WidgetDeleteRequest|WidgetUpdateRequest"
#     },
#     "requestId": {
#       "type": "string"
#     },
#     "widgetId": {
#       "type": "string"
#     },
#     "owner": {
#       "type": "string",
#       "pattern": "[A-Za-z ]+"
#     },
#     "label": {
#       "type": "string"
#     },
#     "description": {
#       "type": "string"
#     },
#     "otherAttributes": {
#       "type": "array",
#       "items": [
#         {
#           "type": "object",
#           "properties": {
#             "name": {
#               "type": "string"
#             },
#             "value": {
#               "type": "string"
#             }
#           },
#           "required": [
#             "name",
#             "value"
#           ]
#         }
#       ]
#     }
#   },
#   "required": [
#     "type",
#     "requestId",
#     "widgetId",
#     "owner"
#   ]
# }


#arguments from provided consumer
#         -p, --profile                  Name of AWS profile to use for credentials (default=default)
#         -r, --region                   Name of AWS profile to use for credentials (default=us-east-1)
#         -mrt, --max-runtime            Maximum runtime in milliseconds, with 0 meaning no maximum (default=30000 for producers, 0 for consumers)
#         -rb, --request-bucket          Name of bucket that will contain requests (default=null
#         -uop, --use-owner-in-prefix    User the owner's name in the object's prefix when storing in S3
#         -rq, --request-queue           URL of queue that will contain requests (default=null
#         -wb, --widget-bucket           Name of the S3 bucket that holds the widgets (default=null)
#         -wkp, --widget-key-prefix      Prefix for widget objects (default=widgets/)
#         -dwt, --dynamodb-widget-table  Name of the DynamoDB table that holds widgets (default=null)
#         -pdbc, --pdb-conn              Postgres Database connection string
#         -pdbu, --pdb-username          Postgres Database username
#         -pdbp, --pdb-password          Password for the Postgres database user
#         -qwt, --queue-wait-timeout     The duration (in seconds) to wait for a message to arrive in the request when trying to receive a message (default=10)
#         -qvt, --queue-visibility-timeout The duration (in seconds) to the messages received from the queue are hidden from others (default=2)
