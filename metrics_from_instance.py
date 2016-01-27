#!/usr/bin/env python2.7
"""
Author: John Vivivan
Date: 1-8-16

Collect metrics from an EC2 Instance

Ideas taken from:
http://www.artur-rodrigues.com/tech/2015/08/04/fetching-real-cpu-load-from-within-an-ec2-instance.html
"""
import csv
from datetime import datetime
import logging
from operator import itemgetter
import os
import boto.ec2
from boto3.session import Session
import errno
from tqdm import tqdm
from uuid import uuid4
from boto_lib import get_instance_ids

logging.basicConfig(level=logging.INFO)
# logging.getLogger().setLevel(100)


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


def get_start_and_stop(instance_id, region='us-west-2'):
    """
    calculates start and stop time of an instance

    instance_id: str    Instance ID
    region: str         Region
    :returns: strs      startTime, endTime
    """
    logging.info('Acquiring start and stop time of instance...')
    start, stop = 0, 0
    conn = boto.ec2.connect_to_region(region)
    reservations = conn.get_all_instances(instance_id)
    for r in reservations:
        for i in r.instances:
            start = i.launch_time
            if i.state == 'terminated' or i.state == 'stopped':
                # Convert stop to proper format
                stop = i.reason.split('(')[1].split(')')[0]
                stop = stop.split()
                stop = stop[0] + 'T' + stop[1] + '.000Z'
            else:
                logging.info('Instance not stopped or terminated yet. Using current UTC time.')
                t = datetime.utcnow().strftime('%Y%m%d%H%M%S')
                stop = t[:4] + '-' + t[4:6] + '-' + t[6:8] + 'T' + t[8:10] + ':' + t[10:12] + ':' + t[12:14] + '.000Z'
    if start == 0:
        raise RuntimeError('Spot Instance {} not found'.format(instance_id))
    return start, stop


def get_metric(metric, instance_id, start, stop, region='us-west-2'):
    """
    returns metric object associated with a paricular instance ID

    metric_name: str            Name of Metric to be Collected
    instance_id: str            Instance ID
    start: float                ISO format of UTC time start point
    stop: float                 ISO format of UTC time stop point
    region: str                 AWS region
    :return: metric object
    """
    session = Session(region_name=region)
    cw = session.client('cloudwatch')
    namespace, metric_name = metric.rsplit('/', 1)
    logging.info('Start: {}\tStop: {}'.format(start, stop))
    return cw.get_metric_statistics(Namespace=namespace,
                                    MetricName=metric_name,
                                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                                    StartTime=start,
                                    EndTime=stop,
                                    Period=300,
                                    Statistics=['Average'])


def get_datapoints(metric_statistic):
    """
    Returns datapoints from metric object
    """
    return sorted(metric_statistic['Datapoints'], key=itemgetter('Timestamp'))


def collect_metrics(instance_ids, list_of_metrics, start, stop, region='us-west-2', uuid=str(uuid4())):
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
    metrics = {metric: [] for metric in list_of_metrics}
    assert instance_ids, 'No instances retrieved. Check filters.'
    for instance_id in tqdm(instance_ids):
        for metric in list_of_metrics:
            averages = []
            try:
                s = start
                while s < stop:
                    e = s + (4 * 24 * 3600)
                    aws_start = datetime.isoformat(datetime.utcfromtimestamp(s)) + 'Z'
                    aws_stop = datetime.isoformat(datetime.utcfromtimestamp(e)) + 'Z'
                    met_object = get_metric(metric, instance_id, aws_start, aws_stop, region)
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
    Script to collect aggregate metrics from a collection of instances.
    """
    # parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # parser.add_argument('-c', '--cluster_name', default=None, help='Name of cluster to filter by.')
    # parser.add_argument('-n', '--instance_name', default=None, help='Name of instnace to filter by.')
    # params = parser.parse_args()
    #
    # ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.instance_name)
    ids = get_instance_ids(filter_cluster='gtex-transfer', filter_name='jtvivian_toil-worker')
    logging.info("IDs being collected: {}".format(ids))
    list_of_metrics = ['AWS/EC2/CPUUtilization',
                       'CGCloud/MemUsage',
                       'CGCloud/DiskUsage_mnt_ephemeral',
                       'CGCloud/DiskUsage_root',
                       'AWS/EC2/NetworkIn',
                       'AWS/EC2/NetworkOut',
                       'AWS/EC2/DiskWriteOps',
                       'AWS/EC2/DiskReadOps']
    collect_metrics(ids, list_of_metrics, start=1452822550.44147, stop=1453859352)


if __name__ == '__main__':
    main()