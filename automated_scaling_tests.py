#!/usr/bin/env python2.7
"""
Author: John Vivian
Date: 1-9-16

Designed for doing scaling tests on the rna-seq cgl pipeline.
"""
import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
                    datefmt='%m-%d %H:%M:%S')

import argparse
from collections import namedtuple
import csv
import os
import random
import subprocess
import boto
from boto.exception import BotoServerError, EC2ResponseError
import boto.ec2.cloudwatch
import time
from uuid import uuid4
from tqdm import tqdm
import errno
from boto_lib import get_instance_ids
from datetime import datetime, timedelta

metric_endtime_margin = timedelta(hours=1)
metric_initial_wait_period_in_seconds = 0
metric_collection_interval_in_seconds = 3600
metric_start_time_margin = 1800


def create_config(params):
    """
    Creates a configuration file with a random selection of samples that equal the sample size desired.

    params: argparse.Namespace      Input arguments
    """
    log.info('Creating Configuration File')
    # Acquire keys from bucket
    conn = boto.connect_s3()
    bucket = conn.get_bucket(params.bucket)
    keys = [x for x in bucket.list()]
    random.shuffle(keys)
    log.info('Choosing subset from {} number of samples'.format(len(keys)))
    # Collect random samples until specified limit is reached
    total = 0
    samples = []
    while total <= params.sample_size:
        key = keys.pop()
        samples.append(key)
        total += key.size * 1.0 / (1024 ** 4)
    log.info('{} samples selected, totaling {} TB (requested {} TB).'.format(len(samples),
                                                                                 total, params.sample_size))
    # Write out config
    with open(os.path.join(params.shared_dir, 'config.txt'), 'w') as f:
        prefix = 'https://s3-us-west-2.amazonaws.com'
        for key in samples:
            name = key.name.split('/')[-1]
            f.write(name.split('.')[0] + ',' + os.path.join(prefix, params.bucket, name) + '\n')
    log.info('Number of samples selected is: {}'.format(len(samples)))


def launch_cluster(params):
    """
    Launches a toil cluster of size N, with shared dir S, of instance type I, at a spot bid of B

    params: argparse.Namespace      Input arguments
    """
    log.info('Launching cluster of size: {} and type: {}'.format(params.num_workers, params.instance_type))
    subprocess.check_call(['cgcloud',
                           'create-cluster',
                           '--leader-instance-type', params.leader_type,
                           '--instance-type', params.instance_type,
                           '--share', params.share,
                           '--num-workers', str(params.num_workers),
                           '--cluster-name', params.cluster_name,
                           '--spot-bid', str(params.spot_bid),
                           '--leader-on-demand',
                           '--num-threads', str(params.num_workers),
                           '--ssh-opts',
                           '-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no',
                           'toil'])


def place_boto_on_leader(params):
    log.info('Adding a .boto to leader to avoid credential timeouts.')
    subprocess.check_call(['cgcloud', 'rsync', '--cluster-name', params.cluster_name,
                           '--ssh-opts=-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no',
                           'toil-leader', params.boto_path, ':'])


def launch_pipeline(params):
    """
    Launches pipeline on toil-leader in a screen named the cluster run name

    params: argparse.Namespace      Input arguments
    """
    if not params.jobstore:
        jobstore = '{}-{}'.format(uuid4(), str(datetime.utcnow().date()))
    else:
        jobstore = params.jobstore
    restart = '--restart' if params.restart else ''
    log.info('Launching Pipeline and blocking. Check log.txt on leader for stderr and stdout')
    try:
        # Create screen session
        subprocess.check_call(['cgcloud', 'ssh', '--cluster-name', params.cluster_name, 'toil-leader',
                               '-o', 'UserKnownHostsFile=/dev/null', '-o', 'StrictHostKeyChecking=no',
                               'screen', '-dmS', params.cluster_name])
        # Run command on screen session
        subprocess.check_call(['cgcloud', 'ssh', '--cluster-name', params.cluster_name, 'toil-leader',
                               '-o', 'UserKnownHostsFile=/dev/null', '-o', 'StrictHostKeyChecking=no',
                               'screen', '-S', params.cluster_name, '-X', 'stuff',
                               '"python /home/mesosbox/shared/rnaseq_cgl_pipeline.py \
                                aws:us-west-2:{0} \
                                --config /home/mesosbox/shared/config.txt \
                                --retryCount 2 \
                                --ssec /home/mesosbox/shared/master.key \
                                --s3_dir {1} \
                                --sseKey=/home/mesosbox/shared/master.key \
                                --batchSystem="mesos" \
                                --mesosMaster mesos-master:5050 \
                                --workDir=/var/lib/toil \
                                --save_bam \
                                --wiggle {2} >& log.txt\n"'.format(jobstore, params.bucket, restart)])
    except subprocess.CalledProcessError as e:
        log.info('Pipeline exited with non-zero status code: {}'.format(e))


def get_metric(cw, metric, instance_id, start, stop):
    """
    returns metric object associated with a paricular instance ID

    metric_name: str            Name of Metric to be Collected
    instance_id: str            Instance ID
    start: float                ISO format of UTC time start point
    stop: float                 ISO format of UTC time stop point
    :return: metric object
    """
    namespace, metric_name = metric.rsplit('/', 1)
    metric_object = cw.get_metric_statistics(namespace=namespace,
                                             metric_name=metric_name,
                                             dimensions={'InstanceId': instance_id},
                                             start_time=start,
                                             end_time=stop,
                                             period=300,
                                             statistics=['Average'])
    return metric_object


def collect_realtime_metrics(params, threshold=0.5, region='us-west-2'):
    """
    Collect metrics from AWS instances in 1 hour intervals.
    Instances that have gone idle (below threshold CPU value) are terminated.

    params: argparse.Namespace      Input arguments
    region: str                     AWS region metrics are being collected from
    uuid: str                       UUID of metric collection
    """
    list_of_metrics = ['AWS/EC2/CPUUtilization',
                       'CGCloud/MemUsage',
                       'CGCloud/DiskUsage_mnt_ephemeral',
                       'CGCloud/DiskUsage_root',
                       'AWS/EC2/NetworkIn',
                       'AWS/EC2/NetworkOut',
                       'AWS/EC2/DiskWriteOps',
                       'AWS/EC2/DiskReadOps']

    # Create output directory
    uuid = str(uuid4())
    date = str(datetime.utcnow().date())
    dir_path = '{}_{}_{}'.format(params.cluster_name, uuid, date)
    mkdir_p(dir_path)

    start = time.time() - metric_start_time_margin

    # Create connections to ec2 and cloudwatch
    conn = boto.ec2.connect_to_region(region)
    cw = boto.ec2.cloudwatch.connect_to_region(region)
    # Create initial variables
    start = datetime.utcfromtimestamp(start)
    DataPoint = namedtuple('datapoint', ['instance_id', 'value', 'timestamp'])
    timestamps = {}
    # Begin loop
    log.info('Metric collection has started. '
                 'Waiting {} seconds before initial collection.'.format(metric_initial_wait_period_in_seconds))
    time.sleep(metric_initial_wait_period_in_seconds)
    while True:
        ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')
        if not ids:
            break
        metric_collection_time = time.time()
        try:
            for instance_id in tqdm(ids):
                kill_instance = False
                for metric in list_of_metrics:
                    datapoints = []
                    aws_start = timestamps.get(instance_id, start)
                    aws_stop = datetime.utcnow() + metric_endtime_margin
                    metric_object = get_metric(cw, metric, instance_id, aws_start, aws_stop)
                    for datum in metric_object:
                        d = DataPoint(instance_id=instance_id, value=datum['Average'], timestamp=datum['Timestamp'])
                        datapoints.append(d)
                    # Save data in local directory
                    if datapoints:
                        datapoints = sorted(datapoints, key=lambda x: x.timestamp)
                        with open(os.path.join(dir_path, '{}.csv'.format(os.path.basename(metric))), 'a') as f:
                            writer = csv.writer(f, delimiter='\t')
                            writer.writerows(datapoints)
                    # Check if instance's CPU has been idle the last 30 minutes.
                    if metric == 'AWS/EC2/CPUUtilization':
                        averages = [x.value for x in sorted(datapoints, key=lambda x: x.timestamp)][-6:]
                        # If there is at least 30 minutes of data points and max is below threshold, flag to be killed.
                        if len(averages) == 6:
                            if max(averages) < threshold:
                                kill_instance = True
                                log.info('Flagging {} to be killed. '
                                             'Max CPU {} for last 30 minutes.'.format(instance_id, max(averages)))
                # Kill instance if idle
                if kill_instance:
                    try:
                        log.info('Terminating Instance: {}'.format(instance_id))
                        conn.terminate_instances(instance_ids=[instance_id])
                    except (EC2ResponseError, BotoServerError) as e:
                        log.info('Error terminating instance: {}\n{}'.format(instance_id, e))
                # Set start point to be last collected timestamp
                timestamps[instance_id] = max(x.timestamp for x in datapoints) if datapoints else start
        except BotoServerError:
            log.error('Giving up trying to fetch metric for this interval')
        # Sleep
        collection_time = time.time() - metric_collection_time
        log.info('Metric collection took: {} seconds. Waiting one hour.'.format(collection_time))
        wait_time = metric_collection_interval_in_seconds - collection_time
        if wait_time < 0:
            log.warning('Collection time exceeded metric collection interval by: %i', -wait_time)
        else:
            time.sleep(wait_time)
    log.info('Metric collection has finished.')


def mkdir_p(path):
    """
    It is Easier to Ask for Forgiveness than Permission
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def main():
    """
    Modular script for running toil pipelines
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')

    # Create Config
    parser_config = subparsers.add_parser('create-config', help='Creates config.txt based on sample size')
    parser_config.add_argument('-s', '--sample-size', required=True, type=float,
                               help='Size of the sample deisred in TB.')
    parser_config.add_argument('-b', '--bucket', default='tcga-data-cgl-recompute',
                               help='Source bucket to pull data from.')

    # Launch Cluster
    parser_cluster = subparsers.add_parser('launch-cluster', help='Launches AWS cluster via CGCloud')
    parser_cluster.add_argument('-s', '--num-workers', required=True, help='Number of workers desired in the cluster.')
    parser_cluster.add_argument('-c', '--cluster-name', required=True, help='Name of cluster.')
    parser_cluster.add_argument('-S', '--share', required=True,
                                help='Full path to directory: pipeline script, launch script, config, and master key.')
    parser_cluster.add_argument('--spot-bid', default=1.00, help='Change spot price of instances')
    parser_cluster.add_argument('-t', '--instance-type', default='c3.8xlarge',
                                help='slave instance type. e.g.  m4.large or c3.8xlarge.')
    parser_cluster.add_argument('-T', '--leader-type', default='m3.medium', help='Sets leader instance type.')
    parser_cluster.add_argument('-b', '--boto-path', default='/home/mesosbox/.boto', type=str,
                                help='Path to local .boto file to be placed on leader.')

    # Launch Pipeline
    parser_pipeline = subparsers.add_parser('launch-pipeline', help='Launches pipeline')
    parser_pipeline.add_argument('-c', '--cluster-name', required=True, help='Name of cluster.')
    parser_pipeline.add_argument('-j', '--jobstore', default=None,
                                 help='Name of jobstore. Defaults to UUID-Date if not set')
    parser_pipeline.add_argument('--restart', default=None, action='store_true',
                                 help='Attempts to restart pipeline, requires existing jobstore.')
    parser_pipeline.add_argument('-b', '--bucket', default='tcga-output', help='Set destination bucket.')

    # Launch Metric Collection
    parser_metric = subparsers.add_parser('launch-metrics', help='Launches metric collection thread')
    parser_metric.add_argument('-c', '--cluster-name', required=True, help='Name of cluster.')
    parser_metric.add_argument('--namespace', default='jtvivian', help='CGCloud NameSpace')

    # Parse args
    params = parser.parse_args()

    # Modular Run Sequence
    if params.command == 'create-config':
        create_config(params)
    elif params.command == 'launch-cluster':
        launch_cluster(params)
    elif params.command == 'launch-pipeline':
        launch_pipeline(params)
        place_boto_on_leader(params)
    elif params.command == 'launch-metrics':
        collect_realtime_metrics(params)


if __name__ == '__main__':
    main()