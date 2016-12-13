import argparse
import os
import shutil

from toil_lib.programs import docker_call
from toil.job import Job
from bd2k.util.files import mkdir_p
from toil_lib.urls import s3am_upload
from toil_lib.jobs import map_job
from glob import glob


def download_bam(job, gdc_id):
    work_dir = job.fileStore.getLocalTempDir()
    output_dir = os.path.join(work_dir, gdc_id)

    parameters = ['download', '-d', '/data', gdc_id]
    docker_call(tool='jvivian/gdc-client', work_dir=work_dir, parameters=parameters)

    sample = glob(os.path.join(output_dir, '*.bam'))[0]
    bam_id = job.fileStore.writeGlobalFile(sample)

    job.addChildJobFn(process_bam, bam_id, gdc_id)


def process_bam(job, bam_id, gdc_id):
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(bam_id, os.path.join(work_dir, 'input.bam'))

    parameters = ['fastq', '-1', '/data/r1.fastq', '-2', '/data/r2.fastq', '/data/input.bam']
    docker_call(tool='quay.io/ucsc_cgl/samtools', work_dir=work_dir, parameters=parameters)





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