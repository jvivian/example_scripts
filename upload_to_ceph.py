#!/usr/bin/env python2.7
import boto
import boto.s3.connection
from boto.s3 import connect_to_region
import math
import os
import sys


def upload_file(s3, bucketname, keyname, file_path):

        b = s3.get_bucket(bucketname)

        k = b.new_key(keyname)
        if not k.exists():

            mp = b.initiate_multipart_upload(keyname)

            source_size = os.stat(file_path).st_size
            bytes_per_chunk = 5000*1024*1024
            chunks_count = int(math.ceil(source_size / float(bytes_per_chunk)))

            for i in range(chunks_count):
                offset = i * bytes_per_chunk
                remaining_bytes = source_size - offset
                bytes = min([bytes_per_chunk, remaining_bytes])
                part_num = i + 1

                print "uploading part " + str(part_num) + " of " + str(chunks_count)

                with open(file_path, 'r') as fp:
                        fp.seek(offset)
                        mp.upload_part_from_file(fp=fp, part_num=part_num, size=bytes)

            if len(mp.get_all_parts()) == chunks_count:
                    mp.complete_upload()
                    print "upload_file done"
            else:
                    mp.cancel_upload()
                    print "upload_file failed"



host = 'ceph-gw-01.pod'
s3 = boto.connect_s3(host=host,
                     is_secure=False,
                     calling_format=boto.s3.connection.OrdinaryCallingFormat(),)


directory = sys.argv[1]
# bucket = conn.get_bucket('cgl-driver-projects')
files = [os.path.join(directory, f) for f in os.listdir(directory)]
failures = []
for f in files:
    retry = 3
    # k = bucket.new_key('ispy/' + os.path.basename(f))
    # if not k.exists():
    for i in xrange(retry):
        try:
            print 'uploading: {}\tTry: {}'.format(f, i+1)
            upload_file(s3, 'cgl-pipeline-inputs', 'rnaseq-cgl/' + os.path.basename(f), f)
            break
        except Exception as e:
            if i == 2:
                print '{}: Permamently failed to upload'.format(f)
                print e.message, e.args
                failures.append(f)
            pass

if failures:
    print '\n=====Failures====='
    print '\n'.join(failures)