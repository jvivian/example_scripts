import os
import tarfile

from toil_lib.urls import s3am_upload
from tqdm import tqdm

home_dir = '/pod/home/jvivian/beatAML-transfer/'
sample_dir = '/pod/pstore/projects/BeatAML/fastq-12_07_2016/'
s3_dir = 's3://cgl-beataml-data/'
s3_key = '/pod/home/jvivian/master.key'

samples = os.listdir(sample_dir)
sample_ids = {x.split('_L00')[0] for x in samples}

with open(os.path.join(home_dir, 'beatAML-samples'), 'w') as f:
    f.write('\n'.join(sample_ids))

for sample_id in tqdm(sample_ids):
    subset = [os.path.join(sample_dir, x) for x in samples if x.startswith(sample_id)]

    out_tar = os.path.join(home_dir, sample_id + '.tar.gz')
    with tarfile.open(out_tar, 'w:gz') as tar:
        for sample in subset:
            tar.add(sample, arcname=os.path.basename(sample))

    s3am_upload(out_tar, s3_dir, num_cores=6, s3_key_path=s3_key)

    os.remove(out_tar)
