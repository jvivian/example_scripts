#!/usr/bin/env python2.7
# John Vivian
# 9-18-15
"""
Convert files in a directory to a random UUID4

Write a TSV matching original name to UUID4
"""

import sys
import uuid
import os

dir = sys.argv[1]
project = 'WCDT'
extension = '.fastq.gz'
onlyfiles = [ os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) ]

with open('name_pair.tsv', 'w') as f_out:
    for fname in onlyfiles:
        sample_uuid = str(uuid.uuid4())
        f_out.write('{}\t{}\t{}\n'.format(project, os.path.basename(fname).split('.')[0], sample_uuid))
        os.rename(fname, os.path.join(dir, sample_uuid + extension))