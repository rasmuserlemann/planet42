import boto3

s3_client = boto3.client(service_name='s3', region_name='eu-north-1',
                         aws_access_key_id='<INSERT HERE>',
                         aws_secret_access_key='<INSERT HERE>')

bucket_name = '<INSERT HERE>'
file_name = '<INSERT HERE>'
s3_client.upload_file('/Users/rasmuserlemann/Downloads/AAPL.csv', Bucket=bucket_name,Key=file_name)