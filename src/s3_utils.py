import boto3
import json
import os
from botocore import config
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


def upload_large_file(filepath, file_name):
    config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                            multipart_chunksize=1024*25, use_threads=True)


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
    file_name = 'readme.txt'
    file_path = '/Users/thimothekonchou/Documents/DataSience/Data-Migration-SQLServer-SnowFlake/README.md'
    print(upload_small_file(BUCKET_NAME, file_path, file_name))
