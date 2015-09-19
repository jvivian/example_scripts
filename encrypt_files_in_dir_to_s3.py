#!/usr/bin/env python2.7
# John Vivian
# 9-18-15
"""
Move files in a directory to S3 with encryption

1st arg:    directory that files will be uploaded from
2nd arg:    path to master_key
3rd arg:    bucket name
"""
import hashlib
import os
import subprocess
import sys


def generate_unique_key(master_key_path, url):
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
                                  'Key: {}, Length: {}'.format(master_key, len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32, 'New key is invalid and is not 32 characters: {}'.format(new_key)
    return new_key


directory = sys.argv[1]
master_key_path = sys.argv[2]
bucket = sys.argv[3]
key_dir = 'wcdt/exome_fastqs/'

exit_codes = []
url_base = 'https://s3-us-west-2.amazonaws.com/'
files = [ os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) ]

for file_path in files:
    url = os.path.join(url_base, bucket, key_dir,  os.path.basename(file_path))
    new_key = generate_unique_key(master_key_path, url)
    print 'New Key: {} formed from url: {}'.format(new_key, url)

    with open(os.path.basename(url)+'.key', 'wb') as f_out:
        f_out.write(new_key)

    command = ['s3am',
               'upload',
               '--sse-key-file', os.path.basename(url)+'.key',
               'file://' + os.path.abspath(file_path),
               bucket,
               os.path.join(key_dir, os.path.basename(file_path))]

    print ' '.join(command)
    p = subprocess.Popen(command)
    exit_codes.append(p)

exit_codes = [x.wait() for x in exit_codes]