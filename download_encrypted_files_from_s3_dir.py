#!/usr/bin/env python2.7
# John Vivian
"""
Download files from S3 dir to where this script is run
"""
import base64
import hashlib
import os
import subprocess
import boto.s3.connection
import sys


def generate_unique_key(master_key_path, url):
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
                                  'Key: {}, Length: {}'.format(master_key, len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32, 'New key is invalid and is not 32 characters: {}'.format(new_key)
    return new_key

s3_dir = sys.argv[1]
bucket_name = s3_dir.split('/')[0]
bucket_dir = '/'.join(s3_dir.split('/')[1:])

conn = boto.s3.connect_to_region('us-west-2', calling_format=boto.s3.connection.OrdinaryCallingFormat())
bucket = conn.get_bucket(bucket_name)
urls =[str(key.generate_url(expires_in=0, query_auth=False)) for key in bucket.list(bucket_dir) if not key.name.endswith('/')]

for url in urls:
    print url
    file_path = os.path.basename(url)
    key = generate_unique_key('master.key', url)
    encoded_key = base64.b64encode(key)
    encoded_key_md5 = base64.b64encode( hashlib.md5(key).digest() )
    h1 = 'x-amz-server-side-encryption-customer-algorithm:AES256'
    h2 = 'x-amz-server-side-encryption-customer-key:{}'.format(encoded_key)
    h3 = 'x-amz-server-side-encryption-customer-key-md5:{}'.format(encoded_key_md5)
    subprocess.check_call(['curl', '-fs', '--retry', '5', '-H', h1, '-H', h2, '-H', h3, url, '-o', file_path])