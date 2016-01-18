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
import itertools
import errno
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from tqdm import tqdm
from uuid import uuid4
from boto_lib import get_instance_ids

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(100)


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


def get_metric(metric, instance_id, region='us-west-2'):
    """
    returns metric object associated with a paricular instance ID

    metric_name: str            Name of Metric to be Collected
    instance_id: str            Instance ID
    region: str                 Region
    :return: metric object
    """
    session = Session(region_name=region)
    cw = session.client('cloudwatch')
    namespace, metric_name = metric.rsplit('/', 1)
    start, stop = get_start_and_stop(instance_id, region=region)
    return cw.get_metric_statistics(Namespace=namespace,
                                    MetricName=metric_name,
                                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                                    StartTime=start,
                                    EndTime=stop,
                                    Period=600,
                                    Statistics=['Average'])


def get_datapoints(metric_statistic):
    """
    Returns datapoints from metric object
    """
    return sorted(metric_statistic['Datapoints'], key=itemgetter('Timestamp'))


def trim_lists(list_of_lists):
    """
    Given a list of lists, returns a list of lists of equal length.
    """
    fixed = []
    smallest = min(len(x) for x in list_of_lists)
    for item in list_of_lists:
        fixed.append(item[:smallest])
    return fixed


def plot_metrics(instance_ids, list_of_metrics, num_samples='NA', sample_size='NA', region='us-west-2'):
    """
    Plots metrics

    instance_ids: list      List of instance IDs
    list_of_metrics: list   List of tuples:  (metric_name, ylabel)
    """
    metrics = {metric_info[0]: [] for metric_info in list_of_metrics}
    assert instance_ids, 'No instances retrieved. Check filters.'
    # Parallelize metric collection for speed
    # def f(instance_id):
    for instance_id in tqdm(instance_ids):
        for metric_info in list_of_metrics:
            metric = metric_info[0]
            met_object = get_metric(metric, instance_id, region)
            averages = [x['Average'] for x in get_datapoints(met_object)]
            if averages:
                metrics[metric].append(averages)
    # with thread_pool(1):
    #     map(f, instance_ids)
    # Remove empty metrics
    metrics = dict((k, v) for k, v in metrics.iteritems() if v)
    list_of_metrics = [(x, y) for x, y in list_of_metrics if x in metrics]
    # Ensure all metrics are the same size
    limit = sys.maxint
    num_instances = 0
    for metric in metrics:
        metrics[metric] = trim_lists(metrics[metric])
        if limit > len(metrics[metric][0]):
            limit = len(metrics[metric][0])
    assert limit > 1, 'Time Series plot cannot be made with only one time point. Wait a few minutes and try again.'
    for metric in metrics:
        metrics[metric] = [x[:limit] for x in metrics[metric]]
        num_instances = len(metrics[metric])
        metrics[metric] = np.array(metrics[metric])
    # Plot
    colors = itertools.cycle(["r", "b", "g", 'y', 'k'])
    time = [x*5.0 / 60 for x in xrange(limit)]
    f, axes = plt.subplots(len(metrics), sharex=True)
    for i, metric_info in enumerate(list_of_metrics):
        metric, ylabel = metric_info
        sns.tsplot(data=metrics[metric], time=time, ax=axes[i], color=next(colors))
        axes[i].set_title(metric.rsplit('/', 1)[1])
        axes[i].set_ylabel(ylabel)
    axes[-1].set_xlabel('Time (hours)')
    f.suptitle('Aggregate Resources for {} Instances\n{} Samples ~ {} TB'.format(
        num_instances, num_samples, sample_size), fontsize=14)
    # plt.show()
    # Fix spacing by modifying defaultSize and doubling as opposed to setting arbitrary figsize
    default_size = f.get_size_inches()
    f.set_size_inches(default_size[0]*2.5, default_size[1]*2.5)
    uuid = uuid4()
    mkdir_p('{}_{}'.format(uuid, str(datetime.utcnow()).split()[0]))
    plt.savefig('{}_{}/Metrics_for_{}_nodes.svg'.format(uuid, str(datetime.utcnow()).split()[0], num_instances),
                format='svg', dpi=600)
    # Save CSV of data
    for metric in metrics:
        with open('{}_{}/{}.csv'.format(uuid, str(datetime.utcnow()).split()[0], metric.rsplit('/', 1)[1]), 'wb') as f:
            writer = csv.writer(f)
            writer.writerows(metrics[metric])


def main():
    """
    Script to collect aggregate metrics from a variety of instances.


    """
    # parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # parser.add_argument('-c', '--cluster_name', default=None, help='Name of cluster to filter by.')
    # parser.add_argument('-n', '--instance_name', default=None, help='Name of instnace to filter by.')
    # params = parser.parse_args()
    #
    # ids = get_instance_ids(filter_cluster=params.cluster_name, filter_name=params.instance_name)
    ids = get_instance_ids(filter_cluster='gtex-transfer', filter_name='jtvivian_toil-worker')
    logging.info("IDs being collected: {}".format(ids))
    list_of_metrics = [('AWS/EC2/CPUUtilization', 'Percent'),
                       ('CGCloud/MemUsage', 'Percent'),
                       ('CGCloud/DiskUsage_mnt_ephemeral', 'Percent'),
                       ('CGCloud/DiskUsage_root', 'Percent'),
                       ('AWS/EC2/NetworkIn', 'Bytes'),
                       ('AWS/EC2/NetworkOut', 'Bytes'),
                       ('AWS/EC2/DiskWriteOps', 'Bytes'),
                       ('AWS/EC2/DiskReadOps', 'Bytes')]
    plot_metrics(ids, list_of_metrics)


if __name__ == '__main__':
    main()