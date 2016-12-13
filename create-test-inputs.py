#!/usr/bin/env python2.7
import argparse
import os
import shutil
import subprocess
import sys


def make_reference(ref_path, chrom):
    with open(chrom + '.' + ref_path, 'w') as f:
        subprocess.check_call(['samtools', 'faidx', ref_path])
        subprocess.check_call(['samtools', 'faidx', ref_path, chrom], stdout=f)


def make_gtf(gtf_path, chrom):
    with open(chrom + '.' + gtf_path, 'w') as f:
        subprocess.check_call(['grep', '"^{}\b"'.format(chrom), gtf_path], stdout=f)


def make_bam(bam_path, chrom):
    with open(chrom + '.' + bam_path, 'w') as f:
        subprocess.check_call(['samtools', 'index', bam_path])
        subprocess.check_call(['samtools', 'view', '-b', '-h', bam_path, chrom], stdout=f)


def truncate_fastq(fastq_path):
    with open('trunc' + '.' + fastq_path, 'w') as f:
        subprocess.check_call(['sed', '-n', '-e', '1,20000p', fastq_path], stdout=f)


def make_vcf(vcf_path, chrom):
    subprocess.check_call(['vcftools', '--vcf', vcf_path, '--chr', chrom, '--recode', '--out', chrom + '.' + vcf_path])
    shutil.move(chrom + '.' + vcf_path + '.recode.vcf', chrom + '.' + vcf_path)
    os.remove(chrom + '.' + vcf_path + '.log')
    os.remove(vcf_path + '.vcfidx')


def main():
    """
    Author: John Vivian

    Make test inputs used for continuous integration (or other things)

    Dependencies
    ------------
    Samtools: apt-get install samtools
    VCFtools: apt-get install vcftools
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')
    parser.add_argument('--chr', type=str, default='chr6', help='Determine chromosome to use')
    ref = subparsers.add_parser('reference', help='Generate a test reference (chr6)')
    gtf = subparsers.add_parser('gtf', help='Generate a test GTF file (chr6)')
    bam = subparsers.add_parser('bam', help='Generate a test Bam (chr6)')
    trunc = subparsers.add_parser('truncate-fastq', help='Truncates a fastq to 5,000 reads')
    vcf = subparsers.add_parser('vcf', help='Generate a test VCF (chr6)')
    # Add commands
    ref.add_argument('reference', type=str, help='Path to reference')
    gtf.add_argument('gtf', type=str, help='Path to GTF')
    bam.add_argument('bam', type=str, help='Path to bam')
    trunc.add_argument('fastqs', type=str, nargs='+', help='Path to fastq(s)')
    vcf.add_argument('vcf', type=str, help='Path to VCF')
    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    if args.command == 'reference':
        make_reference(args.reference, args.chr)
    elif args.command == 'gtf':
        make_gtf(args.gtf, args.chr)
    elif args.command == 'bam':
        make_bam(args.bam, args.chr)
    elif args.command == 'truncate-fastq':
        for fastq in args.fastqs:
            truncate_fastq(fastq)
    elif args.command == 'vcf':
        make_vcf(args.vcf, args.chr)


if __name__ == '__main__':
    main()
