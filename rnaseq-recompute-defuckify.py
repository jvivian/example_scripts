#!/usr/bin/env python2.7
"""
A couple files in the RNA-seq recompute are fucked.  This script will, for every sample:

- Download sample tar
- Untar sample
- Remove offending files (norm_tpm / norm_fpkm)
- Retar
- Upload back to S3.
"""
import os
import shutil
from urlparse import urlparse
from uuid import uuid4
import boto
import subprocess


def _download_s3_url(fpath, url):
    """
    Downloads from S3 URL via Boto

    :param str fpath: Path to file
    :param str url: S3 URL
    """
    from boto.s3.connection import S3Connection
    s3 = S3Connection()
    try:
        parsed_url = urlparse(url)
        if not parsed_url.netloc or not parsed_url.path.startswith('/'):
            raise ValueError("An S3 URL must be of the form s3:/BUCKET/ or "
                             "s3://BUCKET/KEY. '%s' is not." % url)
        bucket = s3.get_bucket(parsed_url.netloc)
        key = bucket.get_key(parsed_url.path[1:])
        key.get_contents_to_filename(fpath)
    finally:
        s3.close()


def s3am_upload(fpath, s3_dir, num_cores=1, s3_key_path=None):
    """
    Uploads a file to s3 via S3AM
    For SSE-C encryption: provide a path to a 32-byte file

    :param str fpath: Path to file to upload
    :param str s3_dir: Ouptut S3 path. Format: s3://bucket/[directory]
    :param int num_cores: Number of cores to use for up/download with S3AM
    :param str s3_key_path: (OPTIONAL) Path to 32-byte key to be used for SSE-C encryption
    """
    if not s3_dir.startswith('s3://'):
        raise ValueError('Format of s3_dir (s3://) is incorrect: {}'.format(s3_dir))
    s3_dir = os.path.join(s3_dir, os.path.basename(fpath))
    if s3_key_path:
        _s3am_with_retry(num_cores, '--sse-key-is-master', '--sse-key-file', s3_key_path,
                         'file://{}'.format(fpath), s3_dir)
    else:
        _s3am_with_retry(num_cores, 'file://{}'.format(fpath), s3_dir)


def _s3am_with_retry(num_cores, *args):
    """
    Calls S3AM upload with retries

    :param int num_cores: Number of cores to pass to upload/download slots
    :param list[str] args: Additional arguments to append to s3am
    """
    retry_count = 3
    for i in xrange(retry_count):
        s3am_command = ['s3am', 'upload', '--force', '--part-size=50M', '--exists=skip',
                        '--upload-slots={}'.format(num_cores),
                        '--download-slots={}'.format(num_cores)] + list(args)
        ret_code = subprocess.call(s3am_command)
        if ret_code == 0:
            return
        else:
            print 'S3AM failed with status code: {}'.format(ret_code)
    raise RuntimeError('S3AM failed to upload after {} retries.'.format(retry_count))


dst_bucket = 'cgl-rnaseq-recompute-fixed'
src_bucket = 'cgl-rnaseq-recompute-non-wiggle'
dst_dir = 'gtex/'

# Collect samples
samples = []
s3 = boto.connect_s3()
bucket = s3.get_bucket(src_bucket)
for key in bucket.list(dst_dir):
    samples.append(os.path.join('s3://', src_bucket, key.name))

work_dir = str(uuid4())
os.mkdir(work_dir)
work_dir = os.path.abspath(work_dir)
os.chdir(work_dir)
for sample in samples:
    if sample.endswith('.tar.gz'):
        print 'Downloading: {}'.format(sample)
        file_path = os.path.join(work_dir, os.path.basename(sample))
        # Download sample
        _download_s3_url(file_path, sample)
        # Process
        print '\tUntarring'
        subprocess.check_call(['tar', '-xf', file_path, '-C', work_dir])
        os.remove(file_path)
        sample_path = os.listdir(work_dir)[0]
        file_path = os.path.abspath(sample_path) + '.tar.gz'
        # Remove bad files
        print '\tRemoving bad files'
        bad_files = []
        for root, d, files in os.walk(sample_path):
            bad_files.extend([os.path.join(root, x) for x in files if 'tpm' in x or 'fpkm' in x])
        for bad_file in bad_files:
            os.remove(bad_file)
        # Retar
        print '\tRetar'
        subprocess.check_call(['tar', '-czf', sample_path + '.tar.gz', os.path.basename(sample_path)])
        # Upload to s3
        print '\tUploading'
        s3am_upload(file_path, os.path.join('s3://', dst_bucket, dst_dir))
        print '\tCleaning up'
        shutil.rmtree(sample_path)
        os.remove(sample_path + '.tar.gz')
print '\n{} succesfully defucked'.format(dst_dir)
shutil.rmtree(work_dir)
