import socket

import noaaport.nbs
import noaaport.log

log = noaaport.log.get_logger('noaaport.stream')


class Streamer(object):
    def __init__(self, hosts, connection_class=noaaport.nbs.Connection, file_assembler_class=noaaport.nbs.FileAssembler):
        self.hosts = hosts
        self.connection_class = connection_class
        self.file_assembler_class = file_assembler_class
        self.files = {}

    def stream(self, host):
        log.info('Connecting to %s:%i', *host)
        self.sock = socket.socket()
        self.sock.connect(host)
        return self.connection_class(self.sock)

    def reliable_stream(self):
        i = 0
        while True:
            try:
                for packet in self.stream(self.hosts[i]):
                    yield packet
            except Exception, e:
                log.error('Stream error %r: %s' % (self.hosts[i], str(e)))
                self.close()
            i = (i + 1) % len(self.hosts)

    def __iter__(self):
        for packet in self.reliable_stream():
            if packet.filename not in self.files:
                self.files[packet.filename] = self.file_assembler_class(packet.filename)
            assembler = self.files[packet.filename]
            assembler.add_part(packet)
            while assembler.assembled:
                filename, content, zip_filename = assembler.assembled.pop()
                if filename in self.files:
                    del self.files[filename]
                yield (filename, content, zip_filename)

    def close(self):
        self.sock.close()
        self.sock = None
