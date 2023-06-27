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
    aws_access_key_id='ASIAYXVOXBPPI7IM4W2Z',
    aws_secret_access_key='z/PK/ikgdp+cE28+dHepAOW3LxQT0jTHiys5slFh',
    aws_session_token = 'FwoGZXIvYXdzEEAaDMvR4ts/g/VNs0Xe5CK+AV/04RtgfY8OKlNx8WKQ/EZx4nFwlDMKAWI2QDZr0CQy5sFgi+Hifej9WYEWl0TNJOGSVZT4QqaoKmcId3KAF4iJHN3NalzS/7l08LVnutPEvTKpjm5pkGXxyJepKZLW7bLYgGlTnx0Md7nXv2FOLZGOQk9CZpLaW/n5seCWdAS/FKsxZOtTQzBFg8NQPciBdzzH9zPGo5ukXxt2hA88cd4IgiUy1xmdr10EV3+rEvzZU98ElLfS0vRA/aNI4HMo/smmogYyLeuSIpRfdsrPjwpJG5dlVf96mCIywwrbFPu9aWypO9YPLWEyyCk/YHZcpIESKQ=='
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