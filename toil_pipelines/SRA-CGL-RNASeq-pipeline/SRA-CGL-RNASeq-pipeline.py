import argparse
import multiprocessing
import os
import sys
import textwrap
from glob import glob
from subprocess import Popen, PIPE
from urlparse import urlparse

import yaml
from bd2k.util.files import mkdir_p
from bd2k.util.processes import which
from toil.job import Job
from toil_lib import UserError, partitions
from toil_lib import require
from toil_lib.files import move_files
# from toil_lib.jobs import map_job
from toil_lib.urls import s3am_upload, download_url
from toil_rnaseq.rnaseq_cgl_pipeline import generate_file
from toil_rnaseq.rnaseq_cgl_pipeline import preprocessing_declaration
from toil_rnaseq.rnaseq_cgl_pipeline import schemes


def download_and_process_sra(job, sra_info, config):
    """
    Download file from SRA and run toil-rnaseq pipeline

    :param job:
    :param sra_info:
    :param config:
    :return:
    """
    work_dir = job.fileStore.getLocalTempDir()
    sra_id, project_id, read_type = sra_info

    # Setup config parameters expected in downstream pipeline
    config.file_type = 'fq'
    config.paired = True if read_type == 'PAIRED' else False
    config.uuid = sra_id
    config.cores = int(multiprocessing.cpu_count())

    # Get key
    download_url(url=config.sra_key, work_dir=work_dir, name='srakey.ngc')

    # Define parameters to fastq-dump
    parameters = ['--split-files', config.uuid] if config.paired else [config.uuid]

    # Define Docker call
    call = ['docker', 'run',
            '-v', work_dir + ':/data',
            '--log-driver=none',
            '--rm',
            'jvivian/fastq-dump'] + parameters

    # Run Docker
    p = Popen(call, stderr=PIPE, stdout=PIPE)
    out, err = p.communicate()

    # If sample could not be downloaded, record failed SRA
    if 'cannot be opened as database or table' in err:
        job.fileStore.logToMaster('Sample "{}" failed to be download from SRA.'.format(config.uuid))
        fail_path = os.path.join(work_dir, config.uuid + '.fail')
        output_dir = os.path.join(config.output_dir, 'failed-samples')
        with open(fail_path, 'w') as f:
            f.write(err + '\n' + str(sra_info) + '\n')
        if urlparse(output_dir).scheme == 's3':
            s3am_upload(job, fpath=fail_path, s3_dir=output_dir)
        elif urlparse(output_dir).scheme == '':
            mkdir_p(output_dir)
            move_files([fail_path], output_dir)
        return None

    # If sample is succesfully downloaded
    else:
        # Check that run was succesful
        require(p.returncode == 0, 'Run failed\n\nout: {}\n\nerr: {}'.format(out, err))

        # Create subdir by project_id
        config.output_dir = os.path.join(config.output_dir, 'experiments', project_id)
        if urlparse(config.output_dir).scheme == '':
            mkdir_p(config.output_dir)

        # If sample is paired-end
        r1_id, r2_id = None, None
        if config.paired:
            try:
                r1 = [x for x in glob(os.path.join(work_dir, '*_1.*'))][0]
                r2 = [x for x in glob(os.path.join(work_dir, '*_2.*'))][0]
                r2_id = job.fileStore.writeGlobalFile(r2)
            except IndexError as e:
                raise UserError("Couldn't locate paired fastqs (_1. and _2.) in sample.\n\n" + e.message)

        # If sample is single-end
        else:
            try:
                r1 = [x for x in glob(os.path.join(work_dir, '*.f*'))][0]
            except IndexError as e:
                raise UserError("Couldn't locate fastq in sample.\n\n" + e.message)

        r1_id = job.fileStore.writeGlobalFile(r1)
        job.addChildJobFn(preprocessing_declaration, config, r1_id=r1_id, r2_id=r2_id)


def generate_config():
    return textwrap.dedent("""
        # RNA-seq CGL Pipeline configuration file
        # This configuration file is formatted in YAML. Simply write the value (at least one space) after the colon.
        # Edit the values in this configuration file and then rerun the pipeline: "toil-rnaseq run"
        # Just Kallisto or STAR/RSEM can be run by supplying only the inputs to those tools
        #
        # URLs can take the form: http://, ftp://, file://, s3://, gnos://
        # Local inputs follow the URL convention: file:///full/path/to/input
        # S3 URLs follow the convention: s3://bucket/directory/file.txt
        #
        # Comments (beginning with #) do not need to be removed. Optional parameters left blank are treated as false.
        ##############################################################################################################
        # Required: URL {scheme} to index tarball used by STAR
        star-index: s3://cgl-pipeline-inputs/rnaseq_cgl/starIndex_hg38_no_alt.tar.gz

        # Required: URL {scheme} to kallisto index file.
        kallisto-index: s3://cgl-pipeline-inputs/rnaseq_cgl/kallisto_hg38.idx

        # Required: URL {scheme} to reference tarball used by RSEM
        rsem-ref: s3://cgl-pipeline-inputs/rnaseq_cgl/rsem_ref_hg38_no_alt.tar.gz

        # Required: Output location of sample. Can be full path to a directory or an s3:// URL
        # Warning: S3 buckets must exist prior to upload or it will fail.
        output-dir:

        # Required: URL to SRA key
        sra-key:

        # Optional: If true, will preprocess samples with cutadapt using adapter sequences.
        cutadapt: true

        # Optional: If true, will run FastQC and include QC in sample output
        fastqc: true

        # Optional: If true, will run BAM QC (as specified by California Kid's Cancer Comparison)
        bamqc:

        # Adapter sequence to trim. Defaults set for Illumina
        fwd-3pr-adapter: AGATCGGAAGAG

        # Adapter sequence to trim (for reverse strand). Defaults set for Illumina
        rev-3pr-adapter: AGATCGGAAGAG

        # Optional: Provide a full path to a 32-byte key used for SSE-C Encryption in Amazon
        ssec:

        # Optional: Provide a full path to a CGHub Key used to access GNOS hosted data
        gtkey:

        # Optional: If true, saves the wiggle file (.bg extension) output by STAR
        wiggle:

        # Optional: If true, saves the aligned bam (by coordinate) produced by STAR
        # You must also specify an ssec key if you want to upload to the s3-output-dir
        save-bam:

        # Optional: If true, uses resource requirements appropriate for continuous integration
        ci-test:
    """.format(scheme=[x + '://' for x in schemes])[1:])


def generate_manifest():
    return textwrap.dedent("""
        #   Edit this manifest to include information pertaining to each sample to be run.
        #   There are 2 tab-separated columns: Experiment Accession and Sample Accession
        #
        #   Run Accession
        #   Experiment Accession
        #   Library Layout (SINGLE / PAIRED)
        #
        #   Examples:
        #   SRR5119551  PRJNA357964 SINGLE
        #   SRR5122592  PRJNA358250 PAIRED
        #
        #   Place your samples below, one per line.
        """)


def parse_samples(path_to_manifest=None):
    """
    Parses samples, specified in either a manifest or listed with --samples

    :param str path_to_manifest: Path to configuration file
    :return: Samples and their attributes as defined in the manifest
    :rtype: list[list]
    """
    return [x.strip().split('\t') for x in open(path_to_manifest, 'r').readlines()
            if not x.isspace() and not x.startswith('#')]


def map_job(job, func, inputs, *args):
    """
    Spawns a tree of jobs to avoid overloading the number of jobs spawned by a single parent.
    This function is appropriate to use when batching samples greater than 1,000.

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param function func: Function to spawn dynamically, passes one sample as first argument
    :param list inputs: Array of samples to be batched
    :param list args: any arguments to be passed to the function
    """
    # num_partitions isn't exposed as an argument in order to be transparent to the user.
    # The value for num_partitions is a tested value
    num_partitions = 100
    partition_size = len(inputs) / num_partitions
    if partition_size > 1:
        for partition in partitions(inputs, partition_size):
            job.addChildJobFn(map_job, func, partition, *args)
    else:
        for sample in inputs:
            job.addChildJobFn(func, sample, *args, cores=8, disk='500G')


def main():
    """
    Pipeline shim for running toil-rnaseq on SRA data data
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')
    # Generate subparsers
    subparsers.add_parser('generate-config', help='Generates an editable config in the current working directory.')
    subparsers.add_parser('generate-manifest', help='Generates an editable manifest in the current working directory.')
    subparsers.add_parser('generate', help='Generates a config and manifest in the current working directory.')
    # Run subparser
    parser_run = subparsers.add_parser('run', help='Runs the RNA-seq pipeline')
    group = parser_run.add_mutually_exclusive_group()
    parser_run.add_argument('--config', default='config-toil-rnaseq.yaml', type=str,
                            help='Path to the (filled in) config file, generated with "generate-config". '
                                 '\nDefault value: "%(default)s"')
    group.add_argument('--manifest', default='manifest-toil-rnaseq.tsv', type=str,
                       help='Path to the (filled in) manifest file, generated with "generate-manifest". '
                            '\nDefault value: "%(default)s"')
    # If no arguments provided, print full help menu
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    # Add Toil options
    Job.Runner.addToilOptions(parser_run)
    args = parser.parse_args()
    # Parse subparsers related to generation of config and manifest
    cwd = os.getcwd()
    if args.command == 'generate-config' or args.command == 'generate':
        generate_file(os.path.join(cwd, 'config-toil-rnaseq.yaml'), generate_config)
    if args.command == 'generate-manifest' or args.command == 'generate':
        generate_file(os.path.join(cwd, 'manifest-toil-rnaseq.tsv'), generate_manifest)
    # Pipeline execution
    elif args.command == 'run':
        require(os.path.exists(args.config), '{} not found. Please run '
                                             '"toil-rnaseq generate-config"'.format(args.config))
        require(os.path.exists(args.manifest), '{} not found and no samples provided. Please '
                                               'run "toil-rnaseq generate-manifest"'.format(args.manifest))
        samples = parse_samples(path_to_manifest=args.manifest)
        # Parse config
        parsed_config = {x.replace('-', '_'): y for x, y in yaml.load(open(args.config).read()).iteritems()}
        config = argparse.Namespace(**parsed_config)
        config.maxCores = int(args.maxCores) if args.maxCores else sys.maxint
        # Config sanity checks
        require(config.kallisto_index or config.star_index,
                'URLs not provided for Kallisto or STAR, so there is nothing to do!')
        if config.star_index or config.rsem_ref:
            require(config.star_index and config.rsem_ref, 'Input provided for STAR or RSEM but not both. STAR: '
                                                           '{}, RSEM: {}'.format(config.star_index, config.rsem_ref))
        require(config.output_dir, 'No output location specified: {}'.format(config.output_dir))
        for input in [x for x in [config.kallisto_index, config.star_index, config.rsem_ref] if x]:
            require(urlparse(input).scheme in schemes,
                    'Input in config must have the appropriate URL prefix: {}'.format(schemes))
        if not config.output_dir.endswith('/'):
            config.output_dir += '/'
        # Program checks
        for program in ['curl', 'docker']:
            require(next(which(program), None), program + ' must be installed on every node.'.format(program))

        # Start the workflow by using map_job() to run the pipeline for each sample
        Job.Runner.startToil(Job.wrapJobFn(map_job, download_and_process_sra, samples, config), args)


if __name__ == '__main__':
    main()
