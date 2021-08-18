import datetime

import boto3

S3_SERVICE = 's3'
S3_BUCKET_SOURCE = 'bucketentrada'
S3_BUCKET_DESTINY = 'bucketsaida'

class S3:
    def __init__(self, service):
        self.service = service

    def get_all_buckets(self):
        return self.service.buckets.all()

    def get_data_object_bucket_byname(self, bucket_name, key):
        data = self.service.Object(bucket_name, key)
        return data.get()['Body'].read().decode('utf-8')

    def post_data_bucket(self, bucket_name, key, json_data):
        self.service.Bucket(bucket_name).put_object(Key=key, Body=json_data)

    def get_last_data_file_bucket(self, bucket_name):
        my_bucket = self.service.Bucket(bucket_name)
        date_atual = datetime.datetime.today().replace(tzinfo=None)
        last_file = ''
        for file in my_bucket.objects.all():
            current_date = file.last_modified.replace(tzinfo=None)

            if current_date > date_atual:
                date_atual = current_date
                last_file = file.key

        return last_file


class Aws:
    def __init__(self, service_name):
        self.service_name = service_name
        self.service = boto3.resource(service_name)
        self.S3 = S3(self.service)

    def get_service(self):
        return self.service


try:

    aws_consumer = Aws(S3_SERVICE)
    last_file = aws_consumer.S3.get_last_data_file_bucket(S3_BUCKET_SOURCE)
    json_input = aws_consumer.S3.get_data_object_bucket_byname(S3_BUCKET_SOURCE, last_file)
    aws_consumer.S3.post_data_bucket(S3_BUCKET_DESTINY, last_file, json_input)

except Exception as e:
    print(e)
