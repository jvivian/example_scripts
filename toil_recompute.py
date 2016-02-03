#!/usr/bin/env python2.7
"""
Author: John Vivian
Date: 1-9-16

Designed for doing scaling tests on the rna-seq cgl pipeline.

- Creates configuration file with a number of samples that meet the size quota
- Create launch script with UUID for this run
-

"""
import argparse
import os
import random
import subprocess
import boto
import boto.exception
import logging
import boto.ec2.cloudwatch
import time
from uuid import uuid4
from boto_lib import get_instance_ids, get_instance_ips, get_avail_zone
from calculate_ec2_spot_instance import calculate_cost
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


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
                    f_out.write('aws:us-west-2:{}-{} \\\n'.format(uuid, str(datetime.utcnow()).split()[0]))
                elif line.startswith('--s3_dir'):
                    f_out.write('--s3_dir toil-recompute \\\n')
                else:
                    f_out.write(line)
    os.remove(os.path.join(params.shared_dir, 'launch.sh'))
    os.rename(os.path.join(params.shared_dir, 'fixed.sh'), os.path.join(params.shared_dir, 'launch.sh'))
    logging.info('Fixing execution privileges of bash script.')
    st = os.stat(os.path.join(params.shared_dir, 'launch.sh'))
    os.chmod(os.path.join(params.shared_dir, 'launch.sh'), st.st_mode | 0111)
    return uuid


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
    leader_ip = get_instance_ips(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-leader')[0]
    logging.info('Launching Pipeline and blocking. Check log.txt on leader for stderr and stdout')
    try:
        subprocess.check_call('ssh -o StrictHostKeyChecking=no mesosbox@{} '
                              '/home/mesosbox/shared/launch.sh ">&" log.txt'.format(leader_ip),
                              shell=True)
    except subprocess.CalledProcessError as e:
        logging.info('Pipeline exited prematurely: {}'.format(e))



def collect_metrics(params, start, uuid=str(uuid4())):
    """
    Collect metrics from AWS instances.  AWS limits data collection to 1,440 points or 5 days if
    collected in intervals of 5 minutes.  This metric collection will "page" the results in intervals
    of 4 days (to be safe) in order to collect all the desired metrics.

    instance_ids: list          List of instance IDs
    list_of_metrics: list       List of metric names
    start: float                time.time() of start point
    stop: float                 time.time() of stop point
    region: str                 AWS region metrics are being collected from
    uuid: str                   UUID of metric collection
    """
    list_of_metrics = ['AWS/EC2/CPUUtilization',
                       'CGCloud/MemUsage',
                       'CGCloud/DiskUsage_mnt_ephemeral',
                       'CGCloud/DiskUsage_root',
                       'AWS/EC2/NetworkIn',
                       'AWS/EC2/NetworkOut',
                       'AWS/EC2/DiskWriteOps',
                       'AWS/EC2/DiskReadOps']

    ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')

    while ids:
        # metrics = {metric: [] for metric in list_of_metrics}
        for instance_id in ids:
            for metric in list_of_metrics:
                averages = []
                try:
                    s = start
                    while s < stop:
                        e = s + (4 * 24 * 3600)
                        aws_start = datetime.utcfromtimestamp(s)
                        aws_stop = datetime.utcfromtimestamp(e)
                        met_object = get_metric(metric, instance_id, aws_start, aws_stop)
                        averages.extend([x['Average'] for x in get_datapoints(met_object)])
                        s = e
                    if averages:
                        metrics[metric].append(averages)
                        logging.info('# of Datapoints for metric {} is {}'.format(metric, len(metrics[metric][0])))
                except RuntimeError:
                    if instance_id in instance_ids:
                        instance_ids.remove(instance_id)
        # Remove metrics if no datapoints were collected
        metrics = dict((k, v) for k, v in metrics.iteritems() if v)
        # Save CSV of data
        mkdir_p('{}_{}'.format(uuid, str(datetime.utcnow()).split()[0]))
        for metric in metrics:
            with open('{}_{}/{}.csv'.format(uuid, str(datetime.utcnow()).split()[0], metric.rsplit('/', 1)[1]), 'wb') as f:
                writer = csv.writer(f)
                writer.writerows(metrics[metric])


def main():
    """
    Automation script for running scaling tests for Toil Recompute
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--config', required=True, help='Configuration file for run. Must be in shared_dir')
    parser.add_argument('-c', '--cluster_size', required=True, help='Number of workers desired in the cluster.')
    parser.add_argument('-s', '--sample_size', required=True, type=float, help='Size of the sample deisred in TB.')
    parser.add_argument('-t', '--instance_type', default='c3.8xlarge', help='e.g. m4.large or c3.8xlarge.')
    parser.add_argument('-n', '--cluster_name', required=True, help='Name of cluster.')
    parser.add_argument('--namespace', default='jtvivian', help='CGCloud NameSpace')
    parser.add_argument('--spot_price', default=0.60, help='Change spot price of instances')
    parser.add_argument('-b', '--bucket', default='tcga-data-cgl-recompute', help='Bucket where data is.')
    parser.add_argument('-d', '--shared_dir', required=True,
                        help='Full path to directory with: pipeline script, launch script, config, and master key.')
    params = parser.parse_args()

    # Run sequence
    start = time.time()
    # Get number of samples from config
    with open(params.config, 'r') as f:
        num_samples = len(f.readlines())
    # Launch cluster and pipeline
    uuid = fix_launch(params)
    launch_cluster(params)
    ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')
    launch_pipeline(params)
    # Blocks until all workers are idle
    stop = time.time()
    # Collect metrics from cluster
    collect_metrics(ids, list_of_metrics, start, stop, uuid=uuid)
    # Apply "Insta-kill" alarm to every worker
    map(apply_alarm_to_instance, ids)
    # Kill leader
    logging.info('Killing Leader')
    leader_id = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-leader')[0]
    apply_alarm_to_instance(leader_id, threshold=5)
    # Generate Run Report
    avail_zone = get_avail_zone(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')[0]
    total_cost, avg_hourly_cost = calculate_cost(params.instance_type, ids[0], avail_zone)
    # Report values
    output = ['UUID: {}'.format(uuid),
              'Number of Samples: {}'.format(num_samples),
              'Number of Nodes: {}'.format(params.cluster_size),
              'Cluster Name: {}'.format(params.cluster_name),
              'Source Bucket: {}'.format(params.bucket),
              'Average Hourly Cost: ${}'.format(avg_hourly_cost),
              'Cost per Instance: ${}'.format(total_cost),
              'Availability Zone: {}'.format(avail_zone),
              'Start Time: {}'.format(datetime.isoformat(datetime.utcfromtimestamp(start))),
              'Stop Time: {}'.format(datetime.isoformat(datetime.utcfromtimestamp(stop))),
              'Total Cost of Cluster: ${}'.format(float(total_cost) * int(params.cluster_size)),
              'Cost Per Sample: ${}'.format((float(total_cost) * int(params.cluster_size) / int(num_samples)))]
    with open(os.path.join(str(uuid) + '_{}'.format(str(datetime.utcnow()).split()[0]), 'run_report.txt'), 'w') as f:
        f.write('\n'.join(output))
    # You're done!
    logging.info('\n\nScaling Test Complete.')


if __name__ == '__main__':
    main()