#!/usr/bin/env python2.7
# John Vivian
# 9-11-15
"""
S3 data is encrypted with a single master key.
For security, every sample will be encrypted with a unique key
that is derived from the master key and the public S3 URL.

1st argument: master key
"""
import hashlib
import os
import subprocess
import sys
import boto.s3
import boto.s3.connection


def generate_unique_key(master_key_path, url):
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
                                  'Key: {}, Length: {}'.format(master_key, len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32, 'New key is invalid and is not 32 characters: {}'.format(new_key)
    return new_key


master_key_path = sys.argv[1]
exit_codes =[]
# Fetch URLs via boto
conn = boto.s3.connect_to_region('us-west-2', calling_format=boto.s3.connection.OrdinaryCallingFormat())
bucket = conn.get_bucket('cgl-driver-projects-encrypted')
urls =[str(key.generate_url(expires_in=0, query_auth=False)) for key in bucket.list('wcdt/rna-seq-samples/') if not key.name.endswith('/')]
# For each URL, download the file then re-encrypt with unique key
for url in urls:
    new_key = generate_unique_key(master_key_path, url.rstrip('.TMP'))
    print '\nKey generated with URL: {}'.format(url.rstrip('.TMP'))
    print 'Key: {}'.format(new_key)
    # I'm writing out the key, b/c this loop happens too fast for S3am to access a temp file
    # and I don't want to inject the key directly into the subprocess call to avoid
    # key exposure in the .bash_history()
    with open(os.path.basename(url)+'.key', 'wb') as f_out:
         f_out.write(new_key)

    # Invoke S3AM to stream file directly from S3 back into S3 (Thanks Hannes!)
    file_name = os.path.basename(url.rstrip('.TMP'))
    command = ['s3am',
               'upload',
               '--src-sse-key-file', master_key_path,
               '--sse-key-file', os.path.basename(url)+'.key',
               's3://' + url.split('.com/')[1],
               'cgl-driver-projects-encrypted',
               os.path.join('wcdt/rna-seq-samples/', file_name)]
    print ' '.join(command)
    p = subprocess.Popen(command)
    exit_codes.append(p)

# Cool method to invoke several subprocesses without using MP module.
exit_codes = [x.wait() for x in exit_codes]


