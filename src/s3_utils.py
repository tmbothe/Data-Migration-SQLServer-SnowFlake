import boto3
import json
import os
from botocore import config
import threading
import sys
from botocore.retries import bucket
from boto3.s3.transfer import TransferConfig


def s3_client():
    s3 = boto3.client('s3')
    return s3


def s3_resource():  # allow direct access to the bucket
    s3 = boto3.resource('s3')
    return s3


def create_bucket(bucket_name):
    s3_client().create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': 'us-east-2'
        })


def create_bucket_policy(bucket_name):
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AssPerm",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:*"],
                "Resource":["arn:aws:s3:::"+bucket_name+"/*"]
            }

        ]
    }
    policy_string = json.dumps(bucket_policy)
    return s3_client().put_bucket_policy(
        Bucket=bucket_name,
        Policy=policy_string

    )


def list_buckets():
    return s3_client().list_buckets()


def get_bucket_policy(bucket_name):
    return s3_client().get_bucket_policy(Bucket=bucket_name)


def get_bucket_encrytion(bucket_name):
    return s3_client().get_bucket_encryption(Bucket=bucket_name)


def update_bucket_policy(bucket_name):
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AddPerm',
                'Effect': 'Allow',
                'Principal': '*',
                'Action': [
                    's3:DeleteObject',
                    's3:GetObject',
                    's3:PutObject'
                ],
                "Resource": "arn:aws:s3:::" + bucket_name + "/*"

            }
        ]
    }
    policy_string = json.dumps(bucket_policy)
    return s3_client().put_bucket_policy(
        Bucket=bucket_name,
        Policy=policy_string
    )


def server_side_encrypt_bucket(bucket_name):
    return s3_client().put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }
            ]
        }
    )


def delete_bucket(bucket_name):
    return s3_client().delete_bucket(Bucket=bucket_name)


def upload_small_file(bucket_name, file_path, file_key):

    return s3_client().upload_file(file_path, bucket_name, file_key)


def upload_large_file(bucket_name, file_path, file_name):
    config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                            multipart_chunksize=1024*25, use_threads=True)
    s3_resource().meta.client.upload_file(file_path, bucket_name, file_name,
                                          ExtraArgs={'ACL': 'public-read',
                                                     'ContentType': 'text/pdf'},
                                          Config=config,
                                          Callback=ProgressPercentage(file_path))


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_do_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_do_far += bytes_amount
            percentage = (self._seen_do_far/self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s (%.2f%%)" % (
                    self._filename, self._seen_do_far, self._size, percentage
                )
            )
            sys.stdout.flush()


def read_object_from_bucket(bucket_name, object_key):

    return s3_client().get_object(Bucket=bucket_name, Key=object_key)


if __name__ == '__main__':
    BUCKET_NAME = 'thim-snow1'
    print(create_bucket(bucket_name=BUCKET_NAME))
    # print(create_bucket_policy(BUCKET_NAME))
    # print(list_buckets())
    # print(get_bucket_policy(BUCKET_NAME))
    # print(get_bucket_encrytion(BUCKET_NAME))
    # print(update_bucket_policy(BUCKET_NAME))
    # print(server_side_encrypt_bucket(BUCKET_NAME))
    # print(delete_bucket(bucket_name=BUCKET_NAME))
    #file_name = 'readme.txt'
    #file_path = '/Users/thimothekonchou/Documents/DataSience/Data-Migration-SQLServer-SnowFlake/README.md'
    #file_path = '/Users/thimothekonchou/Documents/DataSience/Data-Migration-SQLServer-SnowFlake/9781789346640-NEURAL_NETWORKS_WITH_KERAS_COOKBOOK.pdf'
    #file_name = 'keras-cookbook'
    #print(upload_small_file(BUCKET_NAME, file_path, file_name))
    #upload_large_file(BUCKET_NAME, file_path, file_name)

    #print(read_object_from_bucket(BUCKET_NAME, file_name))
