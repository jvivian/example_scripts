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
import errno
from tqdm import tqdm
from boto_lib import get_instance_ids, get_instance_ips, get_avail_zone
from calculate_ec2_spot_instance import calculate_cost
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


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
                           '--cluster-name', params.cluster_name,
                           '--spot-bid', str(params.spot_price),
                           '--leader-on-demand',
                           '--num-threads', str(params.cluster_size),
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
        p = subprocess.Popen('ssh -o StrictHostKeyChecking=no mesosbox@{} '
                             '/home/mesosbox/shared/launch.sh ">&" log.txt'.format(leader_ip),
                              shell=True)
        logging.info('Adding Boto to nodes')
        add_boto_to_nodes(params)
        stdout, stderr = p.communicate()
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


def get_metric(cw, metric, instance_id, start, stop, backoff=30):
    """
    returns metric object associated with a paricular instance ID

    metric_name: str            Name of Metric to be Collected
    instance_id: str            Instance ID
    start: float                ISO format of UTC time start point
    stop: float                 ISO format of UTC time stop point
    region: str                 AWS region
    :return: metric object
    """
    metric_object = None
    # cw = boto.ec2.cloudwatch.connect_to_region(region)
    namespace, metric_name = metric.rsplit('/', 1)
    try:
        metric_object = cw.get_metric_statistics(namespace=namespace,
                                                 metric_name=metric_name,
                                                 dimensions={'InstanceId': instance_id},
                                                 start_time=start,
                                                 end_time=stop,
                                                 period=300,
                                                 statistics=['Average'])
    except BotoServerError:
        if backoff <= 60:
            logging.info('Failed to get metric due to BotoServerError, retrying in {} seconds'.format(backoff))
            time.sleep(backoff)
            get_metric(cw, metric, instance_id, start, stop, backoff=backoff+10)
        else:
            logging.info('Giving up trying to fetch metric {} for instance {}'.format(metric, instance_id))
            raise RuntimeError

    return metric_object


def collect_realtime_metrics(params, start, uuid=str(uuid4()), threshold=0.5, region='us-west-2'):
    """
    Collect metrics from AWS instances in 1 hour intervals in real time.

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
    # Create output directory
    date = str(datetime.utcnow()).split()[0]
    mkdir_p('{}_{}'.format(uuid, date))
    # Create connections to ec2 and cloudwatch
    conn = boto.ec2.connect_to_region(region)
    cw = boto.ec2.cloudwatch.connect_to_region(region)
    # Create initial variables
    ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')
    aws_start = datetime.utcfromtimestamp(start)
    aws_stop = None
    # Begin loop
    logging.info('Metric collection thread has started. Waiting 15 minutes before initial collection.')
    time.sleep(900)
    while ids:
        metric_collection_time = time.time()
        for instance_id in tqdm(ids):
            kill = False
            for metric in list_of_metrics:
                datapoints = []
                try:
                    aws_stop = aws_start + timedelta(hours=1)
                    metric_object = get_metric(cw, metric, instance_id, aws_start, aws_stop)
                    for datum in metric_object:
                        datapoints.append((instance_id, metric, datum['Average'], datum['Timestamp']))
                except RuntimeError:
                    pass
                # Save data in local directory
                if datapoints:
                    with open('{}_{}/{}.csv'.format(uuid, date, os.path.basename(metric)), 'a') as f:
                        writer = csv.writer(f)
                        writer.writerows(datapoints)
                # Check if instance's CPU has been idle the last 15 minutes.
                if metric == 'AWS/EC2/CPUUtilization':
                    averages = [x[2] for x in sorted(datapoints, key=lambda x: x[3])][-3:]
                    # If there is at least 15 minutes of data points and max is below threshold, flag to be killed.
                    if len(averages) == 3:
                        if max(averages) < threshold:
                            kill = True
            # Kill instance if idle
            if kill:
                try:
                    conn.terminate_instances(instance_ids=[instance_id])
                except (EC2ResponseError, BotoServerError) as e:
                    logging.info('Error terminating instance: {}\n{}'.format(instance_id, e))
        # Sleep
        aws_start = aws_stop
        logging.info('Sleeping for one hour since metric collection started.')
        time.sleep(3600 - (time.time() - metric_collection_time))
        ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')


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
    parser.add_argument('-r', '--region', default='us-west-2', help='AWS Region')
    params = parser.parse_args()

    # Run sequence
    start = time.time()
    num_samples = create_config(params)
    uuid = fix_launch(params)
    launch_cluster(params)
    # Launch metric collection thread
    t = threading.Thread(target=collect_realtime_metrics, args=(params, start, uuid))
    t.start()
    # Launch pipeline and block
    script_succeeded = launch_pipeline(params)
    stop = time.time()
    # Join metric thread
    logging.info('Pipeline exited exit status 0: {}. Blocking until all instances terminated.'.format(script_succeeded))
    t.join()
    logging.info('Metric thread joined, all instances terminated.')
    # Kill leader if pipeline was successful
    leader_id = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-leader')[0]
    if script_succeeded:
        logging.info('Killing Leader')
        try:
            conn = boto.ec2.connect_to_region(params.region)
            conn.terminate_instances(instance_ids=[leader_id])
        except (EC2ResponseError, BotoServerError) as e:
            logging.info('Error terminating instance: {}\n{}'.format(leader_id, e))
    else:
        logging.error('Pipeline exited non-zero exit status. Check log.txt file on leader.')
    # Generate Run Report
    logging.info('Calculating costs')
    avail_zone = get_avail_zone(filter_cluster=params.cluster_name, filter_name=params.namespace + '_toil-worker')[0]
    total_cost, avg_hourly_cost = calculate_cost(params.instance_type, avail_zone, instance_id=leader_id)
    # Report values
    logging.info('Writing out run report')
    output = ['UUID: {}'.format(uuid),
              'Number of Samples: {}'.format(num_samples),
              'Number of Nodes: {}'.format(params.cluster_size),
              'Cluster Name: {}'.format(params.cluster_name),
              'Source Bucket: {}'.format(params.bucket),
              'Availability Zone: {}'.format(avail_zone),
              'Start Time: {}'.format(datetime.isoformat(datetime.utcfromtimestamp(start))),
              'Stop Time: {}'.format(datetime.isoformat(datetime.utcfromtimestamp(stop))),
              'Average Hourly Cost: ${}'.format(avg_hourly_cost),
              'Maxmimum Cost per Instance: ${}'.format(total_cost),
              'Maximum Cost (no workers killed until end): ${}'.format(float(total_cost) * int(params.cluster_size)),
              'Maximum Cost Per Sample: ${}'.format((float(total_cost) * int(params.cluster_size) / int(num_samples)))]
    with open('{}_{}/{}'.format(uuid, str(datetime.utcnow()).split()[0], 'run_report.txt'), 'w') as f:
        f.write('\n'.join(output))
    # You're done!
    logging.info('\n\nScaling Test Complete.')


if __name__ == '__main__':
    main()