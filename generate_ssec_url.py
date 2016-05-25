#!/usr/bin/env python2.7
"""
Original Author: Hannes Schmidt
Author: John Vivian

Date: 2-25-16

Generate URLs that mimic requester pays with encryption
"""
import base64
import hashlib

with open('master.key', 'r') as f:
    master_key = f.read()


def curl_command( key, expires=600, location=None ):
    """
    Returns a curl command that downloads an encrypted object in 
    
    :param Key key: a Boto Key object representing the key for which to genrate a URL
    :param expires: number of seconds for which the result can be used
    :param str location: the location (slightly different to region) of the key's bucket. 
           If None, a location will be determined automatically which will slow this 
           function down considerably. 
    """
    bucket = key.bucket
    if location is None:
        location = bucket.get_location( )
    if location:
        base_url = 'https://s3-' + location + '.amazonaws.com'
    else:
        base_url = 'https://s3.amazonaws.com'
    url = '/'.join( [ base_url, bucket.name, key.name ] )
    sse_key = hashlib.sha256( master_key + str( url ) ).digest( )
    assert len( sse_key ) == 32
    encoded_sse_key = base64.b64encode( sse_key )
    encoded_sse_key_md5 = base64.b64encode( hashlib.md5( sse_key ).digest( ) )
    prefix = 'x-amz-server-side-encryption-customer-'
    headers = {
        prefix + 'algorithm': 'AES256',
        prefix + 'key': encoded_sse_key,
        prefix + 'key-md5': encoded_sse_key_md5 }
    signature_headers = {
        prefix + 'algorithm': 'AES256',
        'x-amz-request-payer': 'requester'
    }
    return ' '.join( [ 'curl',
                         '--create-dirs',
                         '--continue-at', '-',
                         '--output', key.name,
                         "'" + key.generate_url( expires, headers=signature_headers ) + "'" ] + [
                         "-H '%s:%s'" % (k, v) for k, v in headers.iteritems( ) ] )