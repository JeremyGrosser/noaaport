from time import strptime, mktime, time
import sys

import noaaport.log
import noaaport.zis

log = noaaport.log.get_logger('noaaport.emwin')


class Connection(object):
    def __init__(self, sock):
        self.sock = sock
        self.ident = b'ByteBlast Client|NM-emwin@synack.me|V1'
        self.ident = bytes([x ^ 0xFF for x in self.ident])

    def __iter__(self):
        buf = b''
        last_ident = 0
        while True:
            now = int(time())
            if (now - last_ident) > 300:
                log.info('Sending ident packet')
                last_ident = now
                self.sock.sendall(self.ident)

            buf += self.sock.recv(1116)
            if not buf:
                break
            while len(buf) >= 1116:
                if not buf.startswith(b'\xFF\xFF\xFF\xFF\xFF\xFF'):
                    offset = buf.find(b'\xFF\xFF\xFF\xFF\xFF\xFF')
                    if offset == -1:
                        log.info('Sync marker missing! Abort!')
                        buf = ''
                        break
                    buf = buf[offset:]
                    log.info('Discarding %i bytes before sync marker', offset)

                try:
                    packet = Packet(buf[:1116])
                    log.debug(str(packet))
                    yield packet
                except:
                    log.error(sys.exc_info()[1])
                    break
                buf = buf[1116:]
        log.error('Connection closed by remote host')
        self.sock.close()


class Packet(object):
    def __init__(self, data):
        self.data = data
        self.parse()

    def parse(self):
        self.data = bytes([x ^ 0xFF for x in self.data])
        self.header = self.parse_header(self.data[:86])
        self.filename = self.header[b'PF'].decode('ascii')
        self.block = int(self.header[b'PN'])
        self.total_blocks = int(self.header[b'PT'])
        self.checksum = int(self.header[b'CS'])
        self.timestamp = int(mktime(strptime(self.header[b'FD'].decode('ascii'), '%m/%d/%Y %I:%M:%S %p')))
        self.payload = self.data[86:-6]
        if len(self.payload) != 1024:
            raise ValueError('Packet is the wrong size!')
        self.verify_checksum()

    def parse_header(self, data):
        if data[:6] != (b'\x00' * 6):
            raise ValueError('Invalid packet header')
        data = data[6:]

        header = data.rstrip(b' \r\n')
        header = header.split(b'/', 5)
        header = (x for x in header if x)
        header = ((x[:2], x[2:].strip(b' ')) for x in header)
        header = dict(header)
        return header

    def verify_checksum(self):
        checksum = sum(self.payload)
        if int(self.checksum) != checksum:
            raise ValueError('Checksum failed! Got: %i Expecting: %i' % (checksum, self.checksum))

    def dict(self):
        d = {}
        for field in ('filename', 'block', 'total_blocks', 'timestamp'):
            value = getattr(self, field)
            d[field] = value
        return d

    def __str__(self):
        return '%s (%i/%i)' % (self.filename, self.block, self.total_blocks)


class FileAssembler(object):
    def __init__(self, filename):
        self.filename = filename
        self.parts = {}
        self.assembled = []

    def add_part(self, packet):
        self.parts[packet.block] = packet.payload
        self.check_parts(packet)

    def check_parts(self, packet):
        if None not in [self.parts.get(i, None) for i in range(1, packet.total_blocks + 1)]:
            parts = list(self.parts.items())
            parts.sort(key=lambda x: x[0])
            content = b''.join([x[1] for x in parts])
            self.content = content

            if self.filename.endswith('.ZIS'):
                for filename, content in noaaport.zis.decompress(self.content):
                    filename = filename.decode('ascii')
                    self.assembled.insert(0, (self.filename, self.content, filename))
            else:
                self.assembled.insert(0, (self.filename, self.content, None))
