import pandas as pd
import boto3
import json
import io
import os


class S3Client:

    def __init__(self, bucket_name):
        self.client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ID'),
            aws_secret_access_key=os.getenv('AWS_KEY'),
            region_name='eu-west-3'
        )
        self.bucket = bucket_name

    def read_from_s3(self, Key, reader=pd.read_csv):  # path to s3, reader function
        obj = self.client.get_object(
            Bucket=self.bucket,  # 'nilmdev01',
            Key=Key  # 'REDD/test.csv'
        )
        data = reader(obj['Body'])
        return data

    def write_to_s3(self, data, Key):  # pandas df, path to s3

        with io.StringIO() as csv_buffer:
            data.to_csv(csv_buffer, index=False)

        response = self.client.put_object(Bucket=self.bucket,
                                          Body=csv_buffer.getvalue(),
                                          Key=Key)
        print(response)
        return None