import argparse
import os
import tarfile
from glob import glob

from toil.job import Job
from toil_lib.jobs import map_job
from toil_lib.programs import docker_call
from toil_lib.urls import s3am_upload


def download_bam(job, gdc_id):
    work_dir = job.fileStore.getLocalTempDir()
    output_dir = os.path.join(work_dir, gdc_id)

    parameters = ['download', '-d', '/data', gdc_id]
    docker_call(tool='jvivian/gdc-client', work_dir=work_dir, parameters=parameters)

    sample = glob(os.path.join(output_dir, '*.bam'))[0]
    bam_id = job.fileStore.writeGlobalFile(sample)

    job.addChildJobFn(process_bam_and_upload, bam_id, gdc_id, disk='100G')


def process_bam_and_upload(job, bam_id, gdc_id):
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(bam_id, os.path.join(work_dir, 'input.bam'))

    parameters = ['fastq', '-1', '/data/r1.fastq', '-2', '/data/r2.fastq', '/data/input.bam']
    docker_call(tool='quay.io/ucsc_cgl/samtools', work_dir=work_dir, parameters=parameters)

    out_tar = os.path.join(work_dir, gdc_id + '.tar.gz')
    with tarfile.open(out_tar, 'w:gz') as tar:
        for name in [os.path.join(work_dir, x) for x in ['r1.fastq', 'r2.fastq']]:
            tar.add(name, arcname=os.path.basename(name))

    s3am_upload(out_tar, s3_dir='s3://cgl-ccle-data/')


def parse_gdc_manifest(manifest_path):
    ids = []
    with open(manifest_path, 'r') as f:
        f.readline()
        for line in f:
            if line:
                gdc_id = line.split('\t')[0]
                ids.append(gdc_id)
    return ids


def main():
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--manifest', help='Legacy manifest file from the GDC')
    Job.Runner.addToilOptions(parser)
    params = parser.parse_args()

    ids = parse_gdc_manifest(params.manifest)

    Job.Runner.startToil(Job.wrapFn(map_job, download_bam, ids), params)

if __name__ == '__main__':
    main()
