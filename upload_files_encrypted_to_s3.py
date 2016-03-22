#!/usr/bin/env python2.7
# John Vivian
# 9-18-15
"""
Move files in a directory to S3 with or without encryption
"""
import hashlib
import os
import subprocess
import argparse
from urlparse import urlparse


def generate_unique_key(master_key_path, url):
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
                                  'Key: {}, Length: {}'.format(master_key, len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32, 'New key is invalid and is not 32 characters: {}'.format(new_key)
    return new_key


def main():
    """
    Upload files with/without encryption to s3
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d', '--directory', required=True, help='Directory for files to upload')
    parser.add_argument('-k', '--master-key', default=None, help='Path to master key')
    parser.add_argument('-s', '--s3-path', required=True, help='S3 path to upload: i.e. s3://bucket/dir')
    args = parser.parse_args()
    # Parse and check s3 path
    s3_url = urlparse(args.s3_path)
    assert s3_url.scheme == 's3', 's3 path is in an incorrect format. s3://bucket/dir. \n{}'.format(args.s3_path)
    # S3AM base call
    exit_codes = []
    url_base = 'https://s3-us-west-2.amazonaws.com/'
    files = [os.path.abspath(os.path.join(args.directory, f)) for f in os.listdir(args.directory)
             if os.path.isfile(os.path.join(args.directory, f))]
    for fpath in files:
        command = ['s3am', 'upload', '--resume']
        if args.master_key:
            url = os.path.join(url_base, s3_url.netloc, s3_url.path, os.path.basename(fpath))
            new_key = generate_unique_key(args.master_key, url)
            print 'New encryption key formed from url: {}'.format(url)

            key_fname = os.path.basename(fpath) + '.key'

            with open(key_fname, 'wb') as f_out:
                f_out.write(new_key)

            command.extend(['--sse-key-file', key_fname])

        command.extend(['file://{}'.format(fpath),
                        os.path.join(args.s3_path, os.path.basename(fpath))])
        print 'Command: {}\n'.format(' '.join(command))
        p = subprocess.Popen(command)
        exit_codes.append(p)

    [x.wait() for x in exit_codes]


if __name__ == '__main__':
    main()
