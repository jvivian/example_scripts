import argparse
import os
import tarfile
from glob import glob

import subprocess
from toil.job import Job
from toil_lib import partitions
from toil_lib.programs import docker_call
from toil_lib.urls import s3am_upload


def start_jobs(job, ids):
    num_partitions = 100
    partition_size = len(ids) / num_partitions
    if partition_size > 1:
        for partition in partitions(ids, partition_size):
            job.addChildJobFn(start_jobs, partition)
    else:
        for sample in ids:
            job.addChildJobFn(download_bam, sample, disk='40G')


def download_bam(job, gdc_id, disk='40G'):
    work_dir = job.fileStore.getLocalTempDir()
    output_dir = os.path.join(work_dir, gdc_id)

    job.fileStore.logToMaster('Downloading: ' + gdc_id)
    parameters = ['download', '-d', '/data', gdc_id]
    docker_call(tool='jvivian/gdc-client', work_dir=work_dir, parameters=parameters)

    sample = glob(os.path.join(output_dir, '*.bam'))[0]
    bam_id = job.fileStore.writeGlobalFile(sample)

    job.addChildJobFn(process_bam_and_upload, bam_id, gdc_id, disk='80G')


def process_bam_and_upload(job, bam_id, gdc_id, disk='80G'):
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(bam_id, os.path.join(work_dir, 'input.bam'))

    parameters = ['fastq', '-1', '/data/R1.fastq', '-2', '/data/R2.fastq', '/data/input.bam']
    docker_call(tool='quay.io/ucsc_cgl/samtools', work_dir=work_dir, parameters=parameters)

    subprocess.check_call(['gzip', os.path.join(work_dir, 'R1.fastq')])
    subprocess.check_call(['gzip', os.path.join(work_dir, 'R2.fastq')])

    out_tar = os.path.join(work_dir, gdc_id + '.tar.gz')
    with tarfile.open(out_tar, 'w:gz') as tar:
        for name in [os.path.join(work_dir, x) for x in ['R1.fastq.gz', 'R2.fastq.gz']]:
            tar.add(name, arcname=os.path.basename(name))

    s3am_upload(out_tar, s3_dir='s3://cgl-ccle-data/')


def parse_gdc_manifest(manifest_path):
    ids = []
    with open(manifest_path, 'r') as f:
        f.readline()
        for line in f:
            if not line.isspace():
                gdc_id = line.strip().split('\t')[0]
                ids.append(gdc_id)
    return ids


def main():
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--manifest', help='Legacy manifest file from the GDC')
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args()

    ids = parse_gdc_manifest(params.manifest)

    Job.Runner.startToil(Job.wrapJobFn(start_jobs, ids), params)

if __name__ == '__main__':
    main()
