"""
compression_station provides the compress/decompress functions
"""
import gzip


def compress(data, compress_level=6):
    compressed = gzip.compress(data=data, compresslevel=compress_level)
    return compressed


def decompress(data):
    decompressed = gzip.decompress(data=data)
    return decompressed
