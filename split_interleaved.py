#!/usr/bin/env python2.7
"""
Credits to Ian Fiddes

Produces interleaved (R1 and R2) fastq files from samtools pipe.

For use with samtools (> version 1.0)
Example of how to use this script:

samtools bamshuf -uO foo.bam tmp | samtools bam2fq -s /dev/null - | ./split_interleaved.py R1.fq.gz R2.fq.gz
"""
import argparse
import itertools
import gzip
import sys

def main():
    p = argparse.ArgumentParser()
    p.add_argument('files', nargs=2)
    a = p.parse_args()

    left_outf, right_outf = a.files
    with gzip.open(left_outf, "w") as left_outf_handle, gzip.open(right_outf, "w") as right_outf_handle:
        for read_pair in itertools.izip(*[sys.stdin] * 8):
            left_outf_handle.write("".join(read_pair[:4]))
            right_outf_handle.write("".join(read_pair[4:]))

if __name__ == "__main__":
    main()