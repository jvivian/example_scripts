"""
Author: John Vivian
Date: 2-23-16

Turns real time metrics collected during runs into plots.
"""
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
                    datefmt='%m-%d %H:%M:%S')
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import itertools
from tqdm import tqdm
import time
import boto.ec2


def create_sparse_matrix(df):
    frames = []
    # Make the dataframe searchable by instance ID
    df.sort(columns=['id'], inplace=True)
    df.set_index(keys=['id'], drop=False, inplace=True)
    names = df['id'].unique().tolist()
    # For each instance id, construct a 1D vector of values
    for instance_id in tqdm(names):
        instance_df = df.loc[df['id'] == instance_id]
        instance_df.sort('timestamp', inplace=True)
        instance_df = instance_df.transpose()
        instance_df.columns = instance_df.iloc[2]
        instance_df.drop(['timestamp', 'id'], inplace=True)
        instance_df.index = [instance_id]
        # This line removes duplicate timestamp intervals
        frames.append(instance_df.T.groupby(level=0).first().T)
    return pd.concat(frames)


def parse_directory(directory):
    logging.info('{0} Parsing directory {0}'.format('=' * 10))
    metrics = []
    for root, dirs, files in os.walk('.'):
        metrics.extend(files)
    return [os.path.join(directory, x) for x in metrics if x.endswith('.tsv')]


def create_matrices(metrics):
    logging.info('{0} Creating Matrices {0}'.format('=' * 10))
    matrices = {}
    for metric in metrics:
        df = pd.read_csv(metric, sep='\t', names=['id', 'value', 'timestamp'])
        matrices[os.path.basename(metric).split('.')[0]] = create_sparse_matrix(df)
    return matrices


def plot_metrics(params, matrices):
    logging.info('{0} Plotting Metrics {0}'.format('=' * 10))
    colors = itertools.cycle(["r", "b", "g", 'y', 'k', 'm', 'c'])
    ylabels = ['Percent', 'Operations', 'Percent', 'Percent', 'Operations', 'Percent', 'Bytes', 'Bytes']
    titles = ['CPU Utilization', 'Disk Read Operations', 'Disk Usage (Ephemeral)', 'Disk Usage (Root)',
              'Disk Write Ooperations', 'Memory Usage', 'Network In', 'Network Out']
    num_workers = []

    # Dynamically generate subplots for DRYness and flexibility
    metric_info = zip(sorted(matrices.keys()), ylabels, titles)
    f, axes = plt.subplots(len(metric_info)+1, sharex=True, figsize=(16, 24))
    for i, mi in enumerate(metric_info):
        c = next(colors)
        metric, ylabel, title = mi
        matrix = matrices[metric]
        mean = matrix.mean()
        std = matrix.std().fillna(0)
        x = [t*5.0/60 for t in xrange(len(mean))]
        axes[i].plot(x, mean, linewidth=2, linestyle='dashed', color=c)
        # Replace nagative values
        bottom_fill = mean - std
        bottom_fill[bottom_fill < 0] = 0
        axes[i].fill_between(x, bottom_fill, mean+std, alpha=0.2, linewidth=2, color=c)
        axes[i].set_title(title)
        axes[i].set_ylabel(ylabel)
        num_workers.append(matrix.count())
    # We'll calculate the number of workers by taking the max "count" of each metric's timestamp
    num_workers = pd.DataFrame(num_workers)
    num_instances = num_workers.max()*32
    x = [t*5.0/60 for t in xrange(len(num_instances))]
    axes[-1].plot(x, num_instances)
    axes[-1].set_title("Number of Cores")
    axes[-1].set_xlabel('Time (hours)')
    plt.savefig(os.path.join(params.dir, params.output_name) + '.png', type='png', dpi=300)
    plt.savefig(os.path.join(params.dir, params.output_name) + '.svg', type='svg')


def calculate_cost(conn, instance_type='c3.8xlarge', avail_zone='us-west-2a',
                   start_time=None, end_time=None):
    # Some values
    total, n = 0.0, 0
    # Connect to EC2 -- requires ~/.boto
    # Get prices for instance, AZ and time range
    prices = conn.get_spot_price_history(instance_type=instance_type, start_time=start_time,
                                         end_time=end_time, availability_zone=avail_zone)
    # Output the prices
    for price in prices:
        total += price.price
        n += 1
    # Difference b/w first and last returned times
    stop = time.mktime(datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S").timetuple())
    start = time.mktime(datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").timetuple())
    time_diff = (stop - start) / 3600
    return str(time_diff * (total/n)), str(total / n)


def convert_str_to_datetime(str_time):
    str_time = str_time.replace('-', '').replace(':', '').replace(' ', '')
    str_time = datetime.strptime(str_time, '%Y%m%d%H%M%S')
    return str_time.isoformat()


def calculate_costs(params, matrices):
    logging.info('{0} Calculating Costs {0}'.format('=' * 10))
    costs = []
    conn = boto.ec2.connect_to_region('us-west-2')
    for metric in matrices:
        cost = 0
        matrix = matrices[metric]
        for row in tqdm(matrix.iterrows()):
            row = row[1].dropna()
            start = convert_str_to_datetime(min(row.index))
            end = convert_str_to_datetime(max(row.index))
            total, avg = calculate_cost(conn, start_time=start, end_time=end)
            cost += float(total)
        costs.append(cost)
    logging.info('Cost: ~${}'.format(np.median(costs)))
    with open(os.path.join(params.dir, params.output_name) + '_cost.txt', 'w') as f:
        f.write('Cost:\t~${}'.format(round(np.median(costs), 2)))


def main():
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d', '--dir', required=True, type=str, help='Directory containing metric files')
    parser.add_argument('-o', '--output-name', required=True, type=str, help='Name of output plot')
    parser.add_argument('-c', '--calculate-costs', default=None, action='store_true')
    params = parser.parse_args()

    # Start
    metrics = parse_directory(params.dir)
    matrices = create_matrices(metrics)
    plot_metrics(params, matrices)
    if params.calculate_costs:
        calculate_costs(params, matrices)


if __name__ == '__main__':
    main()