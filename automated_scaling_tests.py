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
from metrics_from_instance import collect_metrics
from calculate_ec2_spot_instance import calculate_cost
from datetime import datetime

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
        total += key.size * 1.0 / (1024 ** 4)
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
                    f_out.write('aws:us-west-2:{}-{} \\\n'.format(uuid, str(datetime.utcnow()).split()[0]))
                elif line.startswith('--s3_dir'):
                    f_out.write('--s3_dir tcga-output/{}_{} \\\n'.format(uuid, str(datetime.utcnow()).split()[0]))
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


def apply_alarm_to_instance(instance_id, threshold=0.5, region='us-west-2', backoff=30):
    """
    Applys an alarm to a given instance that terminates after a consecutive period of 1 hour at 0.5 CPU usage.

    instance_id: str        ID of the EC2 Instance
    region: str             AWS region
    """
    logging.info('Applying Cloudwatch alarm to: {}'.format(instance_id))
    cw = boto.ec2.cloudwatch.connect_to_region(region)
    alarm = boto.ec2.cloudwatch.MetricAlarm(name='CPUAlarm_{}'.format(instance_id),
                                            description='Terminate instance after low CPU.', namespace='AWS/EC2',
                                            metric='CPUUtilization', statistic='Average', comparison='<',
                                            threshold=threshold, period=300, evaluation_periods=1,
                                            dimensions={'InstanceId': [instance_id]},
                                            alarm_actions=['arn:aws:automate:{}:ec2:terminate'.format(region)])
    try:
        cw.put_metric_alarm(alarm)
    except boto.exception.BotoServerError:
        logging.info('Failed to apply alarm due to BotoServerError, retrying in {} seconds'.format(backoff))
        time.sleep(backoff)
        apply_alarm_to_instance(instance_id, backoff=backoff+10)
    except RuntimeError:
        logging.info("ERROR: Couldn't Apply Alarm to: {}. Skipping.".format(instance_id))
        pass


def main():
    """
    Automation script for running scaling tests for Toil Recompute
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
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
    num_samples = create_config(params)
    uuid = fix_launch(params)
    launch_cluster(params)
    add_boto_to_nodes(params)
    ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')
    launch_pipeline(params)
    # Blocks until all workers are idle
    stop = time.time()
    # Collect metrics from cluster
    list_of_metrics = ['AWS/EC2/CPUUtilization',
                       'CGCloud/MemUsage',
                       'CGCloud/DiskUsage_mnt_ephemeral',
                       'CGCloud/DiskUsage_root',
                       'AWS/EC2/NetworkIn',
                       'AWS/EC2/NetworkOut',
                       'AWS/EC2/DiskWriteOps',
                       'AWS/EC2/DiskReadOps']
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