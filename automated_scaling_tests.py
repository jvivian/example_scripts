#!/usr/bin/env python2.7
"""
Author: John Vivian
Date: 1-9-16

Given some basic inputs:  Number of Nodes, Number of Samples
    - Create a cluster of N size, with keys and a config of size S
        - Launch Script
        - Master Key
        - Config of Appropriate Size
    - Launch the pipeline
    - Get instance IDs of slaves
        - Collect metrics
        -
    - Apply an alarm
"""
import argparse
import os
import random
import subprocess
from uuid import uuid4
import boto
import logging
import boto.ec2.cloudwatch
import time
from boto_lib import get_instance_ids
from metrics_from_instance import plot_metrics
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


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
        total += key.size*1.0 / (1024**4)
    logging.info('{} samples selected, totaling {}TB (requested {}TB).'.format(len(samples), total, params.sample_size))
    # Write out config
    with open(os.path.join(params.shared_dir, 'config.txt'), 'w') as f:
        prefix = 'https://s3-us-west-2.amazonaws.com'
        for key in samples:
            name = key.name.split('/')[-1]
            f.write(name.split('.')[0] + ',' + os.path.join(prefix, params.bucket, name) + '\n')
    return len(samples)


def fix_launch(params):
    """
    Fixes the bash script that launches the pipeline to have a unique s3_dir and aws jobstore bucket name

    params: argparse.Namespace      Input arguments
    """
    logging.info('Fixing launch script aws bucket name')
    uuid = uuid4()
    with open(os.path.join(params.shared_dir, 'launch.sh'), 'r') as f_in:
        with open(os.path.join(params.shared_dir, 'fixed.sh'), 'w') as f_out:
            for line in f_in:
                if line.startswith('aws'):
                    f_out.write('aws:us-west-2:auto-{} \\\n'.format(uuid))
                elif line.startswith('--s3_dir'):
                    f_out.write('--s3_dir tcga-output/{} \\\n'.format(uuid))
                else:
                    f_out.write(line)
    os.remove(os.path.join(params.shared_dir, 'launch.sh'))
    os.rename(os.path.join(params.shared_dir, 'fixed.sh'), os.path.join(params.shared_dir, 'launch.sh'))
    logging.info('Fixing execution privileges of bash script.')
    st = os.stat(os.path.join(params.shared_dir, 'launch.sh'))
    os.chmod(os.path.join(params.shared_dir, 'launch.sh'), st.st_mode | 0111)


def launch_cluster(params):
    """
    Launches a toil cluster of size N, with shared dir S, of instance type I, at a spot brid of B

    params: argparse.Namespace      Input arguments
    """
    logging.info('Launching cluster of size: {} and type: {}'.format(params.cluster_size, params.instance_type))
    subprocess.check_call(['cgcloud',
                           'create-cluster',
                           '--leader-instance-type', 'm3.medium',
                           '--instance-type', params.instance_type,
                           '--share', params.shared_dir,
                           '--num-workers', str(params.cluster_size),
                           '-c', params.cluster_name,
                           '--spot-bid', str(params.spot_price),
                           '--leader-on-demand',
                           '--ssh-opts',
                           '-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no',
                           'toil'])


def launch_pipeline(params):
    """
    Launches pipeline on toil-leader in a screen named the cluster run name

    params: argparse.Namespace      Input arguments
    """
    logging.info('Launching Pipeline')
    subprocess.check_call(['cgcloud',
                           'ssh',
                           '-c', params.cluster_name,
                           'toil-leader',
                           'screen',
                           '-dmS', params.cluster_name,
                          '/home/mesosbox/shared/launch.sh'])
    logging.info('Waiting 5 minutes to set alarm to avoid early termination')
    time.sleep(300)


def apply_alarm_to_instance(instance_id, region='us-west-2'):
    """
    Applys an alarm to a given instance that terminates after a consecutive period of 1 hour at 0.5 CPU usage.

    instance_id: str        ID of the EC2 Instance
    region: str             AWS region EC2 instances are in
    """
    logging.info('Applying Cloudwatch alarm to: {}'.format(instance_id))
    cw = boto.ec2.cloudwatch.connect_to_region(region)
    alarm = boto.ec2.cloudwatch.MetricAlarm(name='CPUAlarm_{}'.format(instance_id),
                                            description='Terminate instance after low CPU.', namespace='AWS/EC2',
                                            metric='CPUUtilization', statistic='Average', comparison='<',
                                            threshold=0.5, period=3600, evaluation_periods=1,
                                            dimensions={'InstanceId': [instance_id]},
                                            alarm_actions=['arn:aws:automate:{}:ec2:terminate'.format(region)])
    cw.put_metric_alarm(alarm)


def block_on_workers(params):
    """
    Waits until all worker nodes have been killed, then returns. Checks every 5 minutes.
    """
    t = 1
    while True:
        logging.info('Blocking while pipeline runs... Time: {} Minutes'.format(t*5))
        ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')
        if len(ids) == 0:
            break
        t += 1
        time.sleep(300)


def main():
    """
    Automation script for running scaling tests for Toil Recompute
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--cluster_size', required=True, help='Number of workers desired in the cluster.')
    parser.add_argument('-s', '--sample_size', required=True, type=float, help='Size of the sample deisred in TB.')
    parser.add_argument('-t', '--instance_type', default='c3.8xlarge', help='e.g. m4.large or c3.8xlarge.')
    parser.add_argument('--spot_price', default=0.60, help='Change spot price of instances')
    parser.add_argument('-n', '--cluster_name', required=True, help='Name of cluster.')
    parser.add_argument('--namespace', default='jtvivian')
    parser.add_argument('-b', '--bucket', default='tcga-data-cgl-recompute', help='Bucket where data is.')
    parser.add_argument('-d', '--shared_dir', required=True,
                        help='Full path to directory with: pipeline script, launch script, config, and master key.')
    params = parser.parse_args()

    # Run sequence
    num_samples = create_config(params)
    fix_launch(params)
    launch_cluster(params)
    launch_pipeline(params)
    # Apply alarms to all instances
    ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')
    map(apply_alarm_to_instance, ids)
    # Function will block until all workers are terminated
    block_on_workers(params)
    # Collect metrics from cluster
    list_of_metrics = [('AWS/EC2/CPUUtilization', 'Percent'),
                       ('CGCloud/MemUsage', 'Percent'),
                       ('CGCloud/DiskUsage_mnt_ephemeral', 'Percent'),
                       ('CGCloud/DiskUsage_root', 'Percent'),
                       ('AWS/EC2/NetworkIn', 'Bytes'),
                       ('AWS/EC2/NetworkOut', 'Bytes'),
                       ('AWS/EC2/DiskWriteOps', 'Bytes'),
                       ('AWS/EC2/DiskReadOps', 'Bytes')]
    plot_metrics(ids, list_of_metrics, num_samples, params.sample_size)


if __name__ == '__main__':
    main()