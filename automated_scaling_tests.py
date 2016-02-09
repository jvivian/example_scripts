#!/usr/bin/env python2.7
"""
Author: John Vivian
Date: 1-9-16

Designed for doing scaling tests on the rna-seq cgl pipeline.
"""
import argparse
from collections import namedtuple
import csv
import os
import random
import subprocess
import threading
import boto
from boto.exception import BotoServerError, EC2ResponseError
import logging
import boto.ec2.cloudwatch
import time
from uuid import uuid4
from tqdm import tqdm
import errno
from boto_lib import get_instance_ids, get_instance_ips
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

metric_endtime_margin = timedelta(hours=1)
metric_initial_wait_period_in_seconds = 900
metric_collection_interval_in_seconds = 3600


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


def create_config(params):
    """
    Creates a configuration file with a random selection of samples that equal the sample size desired.

    params: argparse.Namespace      Input arguments
    """
    logging.info('Creating Configuration File')
    # Acquire keys from bucket
    conn = boto.connect_s3()
    bucket = conn.get_bucket(params.bucket)
    keys = [x for x in bucket.list()]
    random.shuffle(keys)
    logging.info('Choosing subset from {} number of samples'.format(len(keys)))
    # Collect random samples until specified limit is reached
    total = 0
    samples = []
    while total <= params.sample_size:
        key = keys.pop()
        samples.append(key)
        total += key.size * 1.0 / (1024 ** 4)
    logging.info('{} samples selected, totaling {} TB (requested {} TB).'.format(len(samples),
                                                                                 total, params.sample_size))
    # Write out config
    with open(os.path.join(params.shared_dir, 'config.txt'), 'w') as f:
        prefix = 'https://s3-us-west-2.amazonaws.com'
        for key in samples:
            name = key.name.split('/')[-1]
            f.write(name.split('.')[0] + ',' + os.path.join(prefix, params.bucket, name) + '\n')
    return len(samples)


def launch_cluster(params):
    """
    Launches a toil cluster of size N, with shared dir S, of instance type I, at a spot bid of B

    params: argparse.Namespace      Input arguments
    """
    logging.info('Launching cluster of size: {} and type: {}'.format(params.cluster_size, params.instance_type))
    subprocess.check_call(['cgcloud',
                           'create-cluster',
                           '--leader-instance-type', 'm3.medium',
                           '--instance-type', params.instance_type,
                           '--share', params.shared_dir,
                           '--num-workers', str(params.cluster_size),
                           '--cluster-name', params.cluster_name,
                           '--spot-bid', str(params.spot_price),
                           '--leader-on-demand',
                           '--num-threads', str(params.cluster_size),
                           '--ssh-opts',
                           '-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no',
                           'toil'])


def launch_pipeline(params, date, uuid, *args):
    """
    Launches pipeline on toil-leader in a screen named the cluster run name

    params: argparse.Namespace      Input arguments
    uuid: str                       UUID of run
    *args: list                     Used to pass `--restart` on retry.
    """
    logging.info('Launching Pipeline and blocking. Check log.txt on leader for stderr and stdout')
    try:
        subprocess.check_call('cgcloud ssh --cluster-name {0} toil-leader '
                              '-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '
                              'python /home/mesosbox/shared/rnaseq_cgl_pipeline.py \
                               aws:us-west-2:{1}-{2} \
                               --config /home/mesosbox/shared/config.txt \
                               --retryCount 2 \
                               --ssec /home/mesosbox/shared/master.key \
                               --s3_dir tcga-output/{1}_{2} \
                               --sseKey=/home/mesosbox/shared/master.key \
                               --batchSystem="mesos" \
                               --mesosMaster mesos-master:5050 \
                               --workDir=/var/lib/toil \
                               --save_bam \
                               --wiggle {3} ">&" log.txt'.format(params.cluster_name, uuid, date, ' '.join(args)),
                              shell=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.info('Pipeline exited prematurely: {}'.format(e))
        return False


def add_boto_to_nodes(params):
    """
    Sometimes S3AM is needed to upload files to S3. This requires a .boto config file.

    params: argparse.Namespace      Input arguments
    """
    ips = get_instance_ips(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')
    logging.info('transferring boto to every node')
    boto_path = os.path.join(params.shared_dir, 'boto')
    for ip in ips:
        try:
            subprocess.check_call(['scp', '-o', 'stricthostkeychecking=no',
                                   boto_path,
                                   'mesosbox@{}:/home/mesosbox/.boto'.format(ip)])
        except subprocess.CalledProcessError:
            logging.info("Couldn't add Boto to: {}. Skipping".format(ip))


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


def collect_realtime_metrics(params, date, start, uuid=str(uuid4()), threshold=0.5, region='us-west-2'):
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
    mkdir_p('{}_{}'.format(uuid, date))
    # Create connections to ec2 and cloudwatch
    conn = boto.ec2.connect_to_region(region)
    cw = boto.ec2.cloudwatch.connect_to_region(region)
    # Create initial variables
    start = datetime.utcfromtimestamp(start)
    DataPoint = namedtuple('datapoint', ['instance_id', 'value', 'timestamp'])
    timestamps = {}
    # Begin loop
    logging.info('Metric collection has started. '
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
                        with open('{}_{}/{}.csv'.format(uuid, date, os.path.basename(metric)), 'a') as f:
                            writer = csv.writer(f, delimiter='\t')
                            writer.writerows(datapoints)
                    # Check if instance's CPU has been idle the last 30 minutes.
                    if metric == 'AWS/EC2/CPUUtilization':
                        averages = [x.value for x in sorted(datapoints, key=lambda x: x.timestamp)][-6:]
                        # If there is at least 30 minutes of data points and max is below threshold, flag to be killed.
                        if len(averages) == 6:
                            if max(averages) < threshold:
                                kill_instance = True
                                logging.info('Flagging {} to be killed. '
                                             'Max CPU {} for last 30 minutes.'.format(instance_id, max(averages)))
                # Kill instance if idle
                if kill_instance:
                    try:
                        logging.info('Terminating Instance: {}'.format(instance_id))
                        conn.terminate_instances(instance_ids=[instance_id])
                    except (EC2ResponseError, BotoServerError) as e:
                        logging.info('Error terminating instance: {}\n{}'.format(instance_id, e))
                # Set start point to be last collected timestamp
                timestamps[instance_id] = max(x.timestamp for x in datapoints)
        except BotoServerError:
            logging.error('Giving up trying to fetch metric for this interval')
        # Sleep
        collection_time = time.time() - metric_collection_time
        logging.info('Metric collection took: {} seconds. Waiting one hour.'.format(collection_time))
        wait_time = metric_collection_interval_in_seconds - collection_time
        if wait_time < 0:
            logging.warning('Collection time exceeded metric collection interval by: %i', -wait_time)
        else:
            time.sleep(wait_time)
    logging.info('Metric collection thread has finished.')


def main():
    """
    Automation script for running scaling tests for Toil Recompute.
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--cluster_size', required=True, help='Number of workers desired in the cluster.')
    parser.add_argument('-s', '--sample_size', required=True, type=float, help='Size of the sample deisred in TB.')
    parser.add_argument('-t', '--instance_type', default='c3.8xlarge', help='e.g. m4.large or c3.8xlarge.')
    parser.add_argument('-n', '--cluster_name', required=True, help='Name of cluster.')
    parser.add_argument('--namespace', default='jtvivian', help='CGCloud NameSpace')
    parser.add_argument('--spot_price', default=1.00, help='Change spot price of instances')
    parser.add_argument('-r', '--region', default='us-west-2', help='AWS Region')
    parser.add_argument('-b', '--bucket', default='tcga-data-cgl-recompute', help='Bucket where data is.')
    parser.add_argument('-d', '--shared_dir', required=True,
                        help='Full path to directory with: pipeline script, launch script, config, and master key.')
    params = parser.parse_args()

    # Run sequence
    start = time.time()
    date = str(datetime.utcnow().date())
    uuid = uuid4()
    num_samples = create_config(params)
    # uuid = fix_launch(params)
    launch_cluster(params)
    # Add Boto to Leader
    logging.info('Adding a .boto to leader to avoid credential timeouts.')
    subprocess.check_call(['cgcloud', 'rsync', '--cluster-name', params.cluster_name,
                           '--ssh-opts=-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no',
                           'toil-leader', '/home/mesosbox/.boto', ':'])
    # Launch metric collection thread
    t = threading.Thread(target=collect_realtime_metrics, args=(params, date, start, uuid))
    t.start()
    # Get avail zone
    logging.info('Collecting availability zone')
    avail_zone = subprocess.check_output(['cgcloud', 'ssh', '--cluster-name', params.cluster_name, 'toil-worker',
                                          '-o', 'UserKnownHostsFile=/dev/null', '-o', 'StrictHostKeyChecking=no',
                                          'curl', 'http://169.254.169.254/latest/meta-data/placement/availability-zone'])
    # Launch pipeline and block
    script_succeeded = launch_pipeline(params, date, uuid)
    if not script_succeeded:
        script_succeeded = launch_pipeline(params, date, uuid, '--restart')
    stop = time.time()
    # Join metric thread
    logging.info('Pipeline exited exit status 0: {}. Blocking until all instances terminated.'.format(script_succeeded))
    t.join()
    logging.info('Metric thread joined.')
    # Kill leader if pipeline was successful
    if script_succeeded:
        logging.info('Terminating remainder of cluster')
        subprocess.check_call(['cgcloud', 'terminate-cluster', '--cluster-name', params.cluster_name])
    else:
        logging.error('Pipeline exited non-zero exit status. Check log.txt file on leader.')
    # Generate Run Report
    logging.info('Writing out run report')
    output = ['UUID: {}'.format(uuid),
              'Number of Samples: {}'.format(num_samples),
              'Number of Nodes: {}'.format(params.cluster_size),
              'Cluster Name: {}'.format(params.cluster_name),
              'Source Bucket: {}'.format(params.bucket),
              'Availability Zone: {}'.format(avail_zone),
              'Start Time: {}'.format(datetime.isoformat(datetime.utcfromtimestamp(start))),
              'Stop Time: {}'.format(datetime.isoformat(datetime.utcfromtimestamp(stop)))]
    with open('{}_{}/{}'.format(uuid, date, 'run_report.txt'), 'w') as f:
        f.write('\n'.join(output))
    # You're done!
    logging.info('\n\nScaling Test Complete.')


if __name__ == '__main__':
    main()