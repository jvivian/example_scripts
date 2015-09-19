#!/usr/bin/env python2.7
# John Vivian
"""
Rename files from a name_pair.tsv
"""
import os
import sys

name_pair = sys.argv[1]

directory = 'tumor_data'

with open(name_pair, 'r') as f_in:
     for line in f_in:
         line = line.strip().split('\t')
         current = line[2] + '.fastq.gz'
         new = line[1] + '.fastq.gz'
         print '{} -> {}'.format(os.path.join(directory, current), os.path.join(directory, new))
         os.rename(os.path.join(directory, current), os.path.join(directory, new))

