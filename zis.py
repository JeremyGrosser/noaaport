from collections import namedtuple
import struct
import zlib

ZipHeader = namedtuple('ZipHeader', 'signature version flags compression mtime mdate crc32 compressed_size uncompressed_size filename_len extra_len')

def decompress(data):
    header = ZipHeader(*struct.unpack('<IHHHHHIIIHH', data[:30]))
    filename = data[30:30+header.filename_len]
    extra_start = 30 + header.filename_len
    extra = data[extra_start:header.extra_len]

    if header.signature != 0x04034b50:
        return

    data = data[extra_start+header.extra_len:]
    if header.compression == 8:
        dec = zlib.decompressobj(-15)
        filedata = dec.decompress(data, header.uncompressed_size)
        yield (filename, filedata)
        data = dec.unconsumed_tail
    if data:
        for part in decompress(data):
            yield part
    return
