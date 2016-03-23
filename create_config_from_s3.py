#!/usr/bin/env python2.7
# John Vivian
"""
Creates a config file that will run with Toil scripts
from an S3 bucket/dir

1st Argument: bucket_dir (e.g. cgl-driver-projects-encrypted/wcdt/exome_fastqs)
"""
import os
import sys
import boto.s3.connection


s3_dir = sys.argv[1]

# Get bucket name and bucket_dir
bucket_name = s3_dir.split('/')[0]
bucket_dir = '/'.join(s3_dir.split('/')[1:])

# Fetch S3 keys and do the collation
conn = boto.s3.connect_to_region('us-west-2', calling_format=boto.s3.connection.OrdinaryCallingFormat())
bucket =  conn.get_bucket(bucket_name)
temp_id = None
line = []
with open('config.txt', 'w') as f_out:
    for key in bucket.list(bucket_dir):
        # FIXME This will change depending on context
        id = os.path.basename(key.name).split('.tar')[0]
        url = str(key.generate_url(expires_in=0, query_auth=False))
        if id:
            if temp_id == id:
                line.append(url)
            else:
                f_out.write(','.join(line) + '\n')
                line = []
                line.append(id)
                line.append(url)
        temp_id = id



