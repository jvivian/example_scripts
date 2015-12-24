#!/usr/bin/env python2.7
"""
Author: John Vivian
Date: 12-15-15

Ideas taken from:
Suman - http://stackoverflow.com/questions/11780262/calculate-running-cumulative-cost-of-ec2-spot-instance
andPei - http://stackoverflow.com/questions/20854533/how-to-find-out-when-an-ec2-instance-was-last-stopped
"""
import argparse
import boto.ec2
import time
import datetime
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
                t = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
                stop = t[:4] + '-' + t[4:6] + '-' + t[6:8] + 'T' + t[8:10] + ':' + t[10:12] + ':' + t[12:14] + '.000Z'
    if start == 0:
        raise RuntimeError('Spot Instance {} not found'.format(instance_id))
    return start, stop


def calculate_cost(instance_type, start_time, end_time, avail_zone, region='us-west-2'):
    # Some values
    logging.info('Calculating costs...')
    max_cost, max_time, total_price, old_time = 0.0, 0.0, 0.0, 0.0
    min_time = float("inf")
    # Connect to EC2 -- requires ~/.boto
    conn = boto.ec2.connect_to_region(region)
    # Get prices for instance, AZ and time range
    prices = conn.get_spot_price_history(instance_type=instance_type, start_time=start_time,
                                         end_time=end_time, availability_zone=avail_zone)
    # Output the prices
    for price in prices:
        timee = time.mktime(datetime.datetime.strptime(price.timestamp, "%Y-%m-%dT%H:%M:%S.000Z").timetuple())
        # Get max and min time from results
        if timee < min_time:
            min_time = timee
        if timee > max_time:
            max_time = timee
        # Get the max cost
        if price.price > max_cost:
            max_cost = price.price
        # Calculate total price
        if not (old_time == 0):
            total_price += (price.price * abs(timee - old_time)) / 3600
        old_time = timee
    # Difference b/w first and last returned times
    time_diff = max_time - min_time
    # Output aggregate, average and max results
    print "For one: {} in Zone: {}".format(instance_type, avail_zone)
    print "From: {} to {}".format(start_time, end_time)
    print "\tTotal cost = $" + str(total_price)
    print "\tMax hourly cost = $" + str(max_cost)
    print "\tAvg hourly cost = $" + str(total_price * 3600 / time_diff)


def main():
    """
    Computes the spot market cost for a stopped or terminated instance given 3 values:
    instance_type, instance_id, avail_zone

    Example:
    python calculate_ec2_spot_instance.py -t c3.8xlarge -i i-b3a1cd6a -a us-west-2a
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-t', '--instance_type', default='c3.8xlarge', help='e.g. m4.large or c3.8xlarge.')
    parser.add_argument('-i', '--instance_id', required=True, help='Instance ID is the second column in EC2 browser.')
    parser.add_argument('-a', '--avail_zone', default='us-west-2a',
                        help='Availability Zone found in instance description.')
    params = parser.parse_args()

    start_time, end_time = get_start_and_stop(params.instance_id)
    calculate_cost(params.instance_type, start_time, end_time, params.avail_zone)


if __name__ == '__main__':
    main()