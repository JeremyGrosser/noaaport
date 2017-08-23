from time import strptime, mktime, time
from collections import namedtuple
import logging
import struct
import sys

import noaaport.log


log = noaaport.log.get_logger('noaaport.nbs')


NBSHeader = namedtuple('NBSHeader', 'seq product_type product_category product_code channel_index filename compressed num_blocks block_number block_size')


class NBSPacket(object):
    HEADER_FMT = '>IBBBB37s?HHI'

    def __init__(self, data):
        header_size = struct.calcsize(self.HEADER_FMT)
        p = struct.unpack(self.HEADER_FMT, data[:header_size])
        self.header = dict(NBSHeader(*p)._asdict())
        self.filename = self.header['filename'].rstrip('\x00')

        if len(data) - header_size != self.header['block_size']:
            log.warning('Packet block size mismatch: expected: %i  got: %i' % (
                self.header['block_size'], len(data) - header_size))
        self.data = data[header_size:]

    def checksum(self):
        s = 0
        for c in self.data:
            s += ord(c)
        return s % 4294967296 # uint32 max

    def __str__(self):
        return self.filename

    def __repr__(self):
        return 'NBSPacket %r' % self.header


class Connection(object):
    def __init__(self, sock):
        self.sock = sock
        self.ident = 'NBS1'

    def __iter__(self):
        self.sock.sendall(self.ident)
        buf = ''
        while True:
            chunk = self.sock.recv(1024)
            if chunk == '':
                break
            buf += chunk
            if len(buf) < 12:
                continue
            
            # Parse the packet envelope
            data_id, data_size, data_checksum = struct.unpack('>III', buf[:12])
            # data_id = 1 (full content)
            # data_id = 2 (file name)
            buf = buf[12:]

            # We only support NBS1 (full content) feeds
            if data_id != 1:
                continue

            while len(buf) < data_size:
                buf += self.sock.recv(data_size)
            packet = NBSPacket(buf[:data_size])
            #if packet.checksum() != data_checksum:
            #    print 'checksum mismatch!'
            yield packet
            buf = buf[data_size:]

        log.error('Connection closed by remote host')
        self.sock.close()


class FileAssembler(object):
    def __init__(self, filename, callback=None):
        self.filename = filename
        self.callback = callback
        self.parts = {}

    def add_part(self, packet):
        self.parts[packet.header['block_number']] = packet.data
        self.check_parts(packet)

    def check_parts(self, packet):
        if self.callback is None:
            return

        if not None in [self.parts.get(i, None) for i in range(1, packet.header['num_blocks'] + 1)]:
            parts = self.parts.items()
            parts.sort(key=lambda x: x[0])
            content = ''.join([x[1] for x in parts])
            self.content = content
            self.callback(self.filename, self.content)
