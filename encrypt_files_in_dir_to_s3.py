#!/usr/bin/env python2.7
"""
Author : Arjun Arkal Rao
Affiliation : UCSC BME, UCSC Genomics Institute
File : one_off_scripts/encrypt_files_in_dir_to_s3.py
SOURCE: https://github.com/jvivian/one_off_scripts/blob/master/
        encrypt_files_in_dir_to_s3.py
ORIGINAL AUTHOR: John Vivian

Move files in a directory to S3 with encryption
"""
from __future__ import print_function
import argparse
import base64
import hashlib
import os
import subprocess
import sys
import re


class InputParameterError(Exception):
    '''
    This Error Class will be raised  in the case of a bad parameter provided.
    '''
    pass


def generate_unique_key(master_key, url):
    '''
    This module will take a master key and a url, and then make a new key
    specific to the url, based off the master.
    '''
    with open(master_key, 'r') as keyfile:
        master_key = keyfile.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
        'Key: {}, Length: {}'.format(master_key, len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32, 'New key is invalid and is not ' + \
        '32 characters: {}'.format(new_key)
    return new_key


def write_to_s3(datum, master_key, bucket, remote_dir):
    '''
    This module will take in some datum (a file, or a folder) and write it to
    S3.  It requires a master key to encrypt the datum with, and a bucket to
    drop the results into.  If remote dir is set, the datum is dropped into the
    provided directory.
    datum - :str: Path to file or folder to write to s3am
    master_key - :str: Path to master key
    bucket - :str: Bucket of s3am
    remote_dir - :str: Path describing pseudo dir to store files on s3
    '''
    exit_codes = []
    s3_url_base = 'https://s3-us-west-2.amazonaws.com/'
    #  Retain the base dir separately from the file name / folder structure of
    #  DATUM.  This way it can be easily joined into an AWS filename
    folder_base_dir = os.path.split(datum)[0]
    #  Ensure files are either "regular files" or folders
    if os.path.isfile(datum):
        files = [os.path.basename(datum)]
    elif os.path.isdir(datum):
        files = ['/'.join([re.sub(folder_base_dir, '', folder),
                           filename]).lstrip('/') for folder, _,
                 files in os.walk(datum) for filename in files]
    else:
        raise RuntimeError(datum + 'was neither regular file nor folder.')
    #  Write each file to S3
    for file_path in files:
        url = os.path.join(s3_url_base, bucket)
        if remote_dir:
            url = os.path.join(url, remote_dir)
        url = os.path.join(url, file_path)
        new_key = generate_unique_key(master_key, url)
        #  base command call
        command = ['s3am', 'upload']
        #  Add base64 encoded key
        command.extend(['--sse-key-base64', base64.b64encode(new_key)])
        #  Add URL and bucket info to the call
        command.extend(['file://' + os.path.join(folder_base_dir, file_path),
                        bucket])
        #  If a remote directory was provided, add it here
        if remote_dir:
            command.append(os.path.join(remote_dir, file_path))
        #  Else, add the name of the file itself
        else:
            command.append(file_path)
        proc = subprocess.Popen(command)
        exit_codes.append(proc)
    exit_codes = [x.wait() for x in exit_codes]
    return None


def main():
    '''
    This is the main module for the script.  The script will accept a file, or
    a directory, and then encrypt it with a provided key before pushing it to S3
    into a specified bucket.
    '''
    parser = argparse.ArgumentParser(description=main.__doc__, add_help=True)
    parser.add_argument('-M', '--master_key', dest='master_key', help='Path' +
                        ' to the master key used for the encryption.', type=str,
                        required=True)
    parser.add_argument('-B', '--bucket', dest='bucket', help='S3 bucket.',
                        type=str, required=True)
    parser.add_argument('-R', '--remote_dir', dest='remote_dir',
                        help='Pseudo directory within the bucket to store the' +
                        ' file(s).  NOTE: Folder structure below REMOTE_DIR ' +
                        'will be retained.', type=str,
                        required=False, default=None)
    parser.add_argument('data', help='File(s) or folder(s) to transfer to S3.',
                        type=str, nargs='+')
    params = parser.parse_args()
    #  Input handling
    if not os.path.exists(params.master_key):
        raise InputParameterError('The master key was not found at ' +
                                  params.master_key)
    #  If the user doesn't have ~/.boto , it doesn't even make sense to go ahead
    if not os.path.exists(os.path.expanduser('~/.boto')):
        raise RuntimeError('~/.boto not found')

    #  Process each of the input arguments.
    for datum in params.data:
        datum = os.path.abspath(datum)
        if not os.path.exists(datum):
            print('ERROR:', datum, 'could not be found.', sep=' ',
                  file=sys.stderr)
            continue
        write_to_s3(datum, params.master_key, params.bucket, params.remote_dir)
    return None


if __name__ == '__main__':
    main()
