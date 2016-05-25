#!/usr/bin/env python2.7
"""
Download and re-upload with encryption

Author: John Vivian
"""
import argparse
import base64
import hashlib
import os
import subprocess
import boto
from urlparse import urlparse
from math import ceil

from toil.job import Job


def parse_bucket(bucket):
    """
    Parses config file. Returns list of samples: [ [uuid1, url1], [uuid2, url2], ... ]
    """
    url = urlparse(bucket)
    if url.scheme == 's3':
        bucket = url.netloc
    else:
        bucket = url.path.lstrip('/')

    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket)

    url_prefix = 'https://s3-us-west-2.amazonaws.com/'
    samples = []
    for key in bucket.list():
        samples.append((os.path.join(url_prefix, key.name), key.size))

    return samples


def generate_unique_key(master_key_path, url):
    """
    master_key_path: str    Path to the BD2K Master Key (for S3 Encryption)
    url: str                S3 URL (e.g. https://s3-us-west-2.amazonaws.com/bucket/file.txt)

    Returns: str            32-byte unique key generated for that URL
    """
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
                                  'Key: {}, Length: {}'.format(master_key, len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32, 'New key is invalid and is not 32 characters: {}'.format(new_key)
    return new_key


def generate_unique_key2(master_key_path, url):
    """
    Generates unique 32-byte encryption key given a master key
    to use as a template and an S3 URL (s3://bucket/dir format)

    :param str master_key_path: Path to master key which per-file encryption key is based off of
    :param str url: S3 URL to be used as the key
    :return:
    """
    assert url.startswith('s3'), 'URL must be an s3 url: s3://bucket/example/file'
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters: {}'.format(len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32
    return new_key


def batcher(job, samples, args):
    """
    Spawns a tree of jobs to avoid overloading the number of jobs spawned by a single parent.
    """
    if len(samples) > 1:
        a = samples[len(samples)/2:]
        b = samples[:len(samples)/2]
        job.addChildJobFn(batcher, a, args)
        job.addChildJobFn(batcher, b, args)
    else:
        url, size = samples[0]
        size = '{}G'.format(ceil(size * 1.0 / 1024**3))
        job.addChildJobFn(download_encrypted_file_upload_to_s3, url, args, disk=size)


def download_encrypted_file_upload_to_s3(job, url, args):
    """
    Downloads encrypted files from S3 via header injection

    input_args: dict    Input dictionary defined in main()
    name: str           Symbolic name associated with file
    """
    work_dir = job.fileStore.getLocalTempDir()
    file_path = os.path.join(work_dir, os.path.basename(url))

    # Download File
    key = generate_unique_key(args.ssec, url)

    encoded_key = base64.b64encode(key)
    encoded_key_md5 = base64.b64encode(hashlib.md5(key).digest())
    h1 = 'x-amz-server-side-encryption-customer-algorithm:AES256'
    h2 = 'x-amz-server-side-encryption-customer-key:{}'.format(encoded_key)
    h3 = 'x-amz-server-side-encryption-customer-key-md5:{}'.format(encoded_key_md5)
    subprocess.check_call(['curl', '-fs', '--retry', '5', '-H', h1, '-H', h2, '-H', h3, url, '-o', file_path])
    assert os.path.exists(file_path)

    # Upload
    s3_url = os.path.join('s3://', urlparse(url).path.lstrip('/'))
    with open(os.path.join(work_dir, 'temp.key'), 'wb') as f_out:
        f_out.write(generate_unique_key2(args.ssec, s3_url))
    # Upload to S3 via S3AM
    s3am_command = ['s3am',
                    'upload',
                    '--resume'
                    '--sse-key-file', os.path.join(work_dir, 'temp.key'),
                    'file://{}'.format(file_path),
                    s3_url]
    subprocess.check_call(s3am_command)


def main():
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bucket', required=True, help='bucket to reencrypt')
    parser.add_argument('--ssec', required=True, help='master key')

    Job.Runner.addToilOptions(parser)
    args = parser.parse_args()

    samples = parse_bucket(args.bucket)
    Job.Runner.startToil(Job.wrapJobFn(batcher, samples, args), args)


if __name__ == '__main__':
    main()