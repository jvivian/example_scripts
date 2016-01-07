#!/usr/bin/env python2.7
"""
Collect metrics from an EC2 Instance

Ideas taken from:
http://www.artur-rodrigues.com/tech/2015/08/04/fetching-real-cpu-load-from-within-an-ec2-instance.html
"""

from datetime import datetime
import logging
from operator import itemgetter
import boto.ec2
from boto3.session import Session
import matplotlib.pyplot as plt
import seaborn as sns


def get_start_and_stop(instance_id, region='us-west-2'):
    """
    calculates start and stop time of an instance

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


def get_metric(metric_name, instance_id, region='us-west-2'):
    session = Session(region_name=region)
    cw = session.client('cloudwatch')
    start, stop = get_start_and_stop(instance_id, region=region)
    return cw.get_metric_statistics(Namespace='AWS/EC2',
                                    MetricName=metric_name,
                                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                                    StartTime=start,
                                    EndTime=stop,
                                    Period=300,
                                    Statistics=['Average'])


def get_datapoints(metric_statistic):
    return sorted(metric_statistic['Datapoints'], key=itemgetter('Timestamp'))


def plot_metrics(instance_id, region='us-west-2'):
    cpu_utilization = get_metric('CPUUtilization', instance_id, region)
    disk_write_operations = get_metric('DiskWriteOps', instance_id, region)
    disk_read_operations = get_metric('DiskReadOps', instance_id, region)
    network_in = get_metric('NetworkIn', instance_id, region)
    network_out = get_metric('NetworkOut', instance_id, region)
    cpu = [x['Average'] for x in get_datapoints(cpu_utilization)]
    disk_w = [x['Average'] for x in get_datapoints(disk_write_operations)]
    disk_r = [x['Average'] for x in get_datapoints(disk_read_operations)]
    disk = [max(x) for x in zip(disk_w, disk_r)]
    net_in = [x['Average'] / 1024 / 1024 for x in get_datapoints(network_in)]
    net_out = [x['Average'] / 1024 / 1024  for x in get_datapoints(network_out)]
    net = [max(x) for x in zip(net_in, net_out)]
    time = [x*5.0 / 60 for x in xrange(len(cpu))]
    # Plotting
    f, axarr = plt.subplots(3, sharex=True)
    axarr[0].plot(time, cpu, color='k')
    axarr[0].set_title('CPU Utilization')
    axarr[0].set_ylabel('Percent')
    axarr[1].plot(time, disk, color='b')
    axarr[1].set_title('Total Disk Operations')
    axarr[1].set_ylabel('Operations')
    axarr[2].plot(time, net, color='r')
    axarr[2].set_title('Total Network')
    axarr[2].set_ylabel('MegaBytes')
    axarr[2].set_xlabel('Time (Hours)')
    # axarr[0].set_xlim([0,25])
    plt.show()


plot_metrics('i-89466c50')