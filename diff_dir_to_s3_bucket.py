#!/usr/bin/env python2.7
# John Vivian
"""
Diffs contents of dir to S3 bucket, uploads difference to server
"""

import os
import boto
import subprocess

conn = boto.connect_s3()
bucket = conn.get_bucket('cgl-driver-projects-encrypted')

files_in_s3 = [str(os.path.basename(key.name)) for key in bucket.list('wcdt/exome_fastqs')]
files_in_dir = os.listdir('.')

for fname in files_in_dir:
	if not fname in files_in_s3:
		print fname
		subprocess.check_call(['scp', fname, 'ubuntu@52.88.168.210:/home/ubuntu/wcdt_exome_fastqs/'])
