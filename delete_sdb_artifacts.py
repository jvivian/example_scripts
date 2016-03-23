import boto.sdb
import boto

db = boto.sdb.connect_to_region('us-west-2')
conn = boto.connect_s3()

domains = {domain.name: domain for domain in db.get_all_domains() if domain.name.endswith('--files')}
buckets = {bucket.name: bucket for bucket in conn.get_all_buckets() if bucket.name.endswith('--files')}

diff = set(domains.keys()).difference(set(buckets.keys()))

for domain in diff:
    job = domain.replace('--files', '--jobs')
    domains[domain].delete()
    domains[job].delete()
