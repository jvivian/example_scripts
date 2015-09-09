#!/usr/bin/env python2.7
# John Vivian
# 9-9-15
"""
Given a master key and a url, generate a new key.
"""

import hashlib


def generate_unique_key(master_key_path, url):
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
                                  'Key: {}, Length: {}'.format(master_key, len(master_key))
    return hashlib.md5(master_key + url).hexdigest()
