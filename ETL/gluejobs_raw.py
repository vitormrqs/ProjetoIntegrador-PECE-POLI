%matplotlib inline

from datetime import datetime

import boto3
import geopandas as gp
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Create an S3 client object
print('Iniciando objeto boto.')
s3 = boto3.client(
    's3',
    aws_access_key_id='****',
    aws_secret_access_key='****',
    aws_session_token = '****'
)

# Set the source and destination bucket names and prefixes
src_bucket_name = 'ookla-open-data'
src_prefix = ''
dst_bucket_name = '600588094430speedtestglobalperformancepublic'
dst_prefix = 'raw/'

# List the objects in the source bucket
objects = s3.list_objects(Bucket=src_bucket_name, Prefix=src_prefix)

# Loop through the objects and copy them to the destination bucket
for obj in objects['Contents']:
    src_key = obj['Key']
    dst_key = dst_prefix + src_key.split('/')[-1]
    s3.copy_object(Bucket=dst_bucket_name, CopySource={'Bucket': src_bucket_name, 'Key': src_key}, Key=dst_key)

print('Finalizado')