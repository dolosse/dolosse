"""
brief: Common functions that we can reuse throughout the project
author: S. V. Paulauskas
date: August 18, 2019
"""
from struct import unpack


def decode_buffer(buf, chunk_type='I', chunk_size=4):
    """
    Unpacks buffer, into chunks of size with type.
    :param buf: The buffer we'll unpack
    :param chunk_type: The type of variable we want at the end we default to Unsigned Int
    :param chunk_size: the chunk size, we default to the size of an Unsigned Int (32 bits)
    :return: An array with the decoded data as the specified type.
    """
    array = []
    for val in iter(lambda: buf.read(chunk_size), b''):
        array.append(unpack(chunk_type, val)[0])
    return array
