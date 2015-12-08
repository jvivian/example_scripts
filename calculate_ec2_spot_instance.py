#!/usr/bin/env python2.7
"""
Author: John Vivian
Date: 12-7-15

Ideas taken from:
Suman - http://stackoverflow.com/questions/11780262/calculate-running-cumulative-cost-of-ec2-spot-instance
andPei - http://stackoverflow.com/questions/20854533/how-to-find-out-when-an-ec2-instance-was-last-stopped

"""
# Details of instance & time range you want to find spot prices for
import argparse
import boto.ec2
import time
import datetime


def get_start_and_stop(id, region='us-west-2'):
    """
    calculates start and stop time of an instance

    :returns: strs      startTime, endTime
    """
    start, stop = 0, 0
    conn = boto.ec2.connect_to_region(region)
    reservations = conn.get_all_instances(id)
    for r in reservations:
        for i in r.instances:
            start = i.launch_time
            if i.state == 'terminated' or i.state == 'stopped':
                # Convert stop to proper format
                stop = i.reason.split('(')[1].split(')')[0]
                stop = stop.split()
                stop = stop[0] + 'T' + stop[1] + '.000Z'
            else:
                raise RuntimeError('Instance not stopped or terminated yet. No stop value')
    if start == 0:
        raise RuntimeError('Spot Instance of that type not found')
    return start, stop


def calculate_cost(instanceType, startTime, endTime, aZ, region='us-west-2'):
    # Some values
    max_cost = 0.0
    min_time = float("inf")
    max_time = 0.0
    total_price = 0.0
    old_time = 0.0

    # Connect to EC2 -- requires ~/.boto
    conn = boto.ec2.connect_to_region(region)
    # Get prices for instance, AZ and time range
    print instanceType, startTime, endTime, aZ
    prices = conn.get_spot_price_history(instance_type=instanceType, start_time=startTime,
                                         end_time=endTime, availability_zone=aZ)

    # Output the prices
    # print prices
    print "Historic prices"
    for price in prices:
        timee = time.mktime(datetime.datetime.strptime(price.timestamp,
                                                       "%Y-%m-%dT%H:%M:%S.000Z").timetuple())
        # print "\t" + price.timestamp + " => " + str(price.price)
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
    print "For: one %s" % instanceType
    print "From: %s to %s" % (startTime, endTime)
    print "\tTotal cost = $" + str(total_price)
    print "\tMax hourly cost = $" + str(max_cost)
    print "\tAvg hourly cost = $" + str(total_price * 3600 / time_diff)


def main():
    """
    Computes the spot market cost for an instance given 4 values:

   instanceType, instanceID, availZone
   Examples:
   --instanceType = m1.xlarge
   --instanceID = i-b3a1cd6a
   --availZone = us-east-1c
    """
    parser = argparse.ArgumentParser(description=main.__doc__, add_help=True)
    parser.add_argument('-t', '--instanceType', required=True)
    parser.add_argument('-i', '--instanceID', required=True)
    parser.add_argument('-a', '--availZone', required=True)
    params = parser.parse_args()

    startTime, endTime = get_start_and_stop(params.instanceID)
    calculate_cost(params.instanceType, startTime, endTime, params.availZone)


if __name__ == '__main__':
    main()