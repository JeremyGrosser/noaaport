import socket

import noaaport.nbs
import noaaport.log

log = noaaport.log.get_logger('noaaport.stream')


class Streamer(object):
    def __init__(self, hosts, callback, connection_class=noaaport.nbs.Connection, file_assembler_class=noaaport.nbs.FileAssembler):
        self.hosts = hosts
        self.callback = callback
        self.connection_class = connection_class
        self.file_assembler_class = file_assembler_class
        self.files = {}

    def stream(self, host):
        log.info('Connecting to %s:%i' % host)
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
            i = (i + 1) % len(self.hosts)

    def handle_incoming(self, filename, content, zip_filename=None):
        if filename in self.files:
            del self.files[filename]

        if zip_filename is not None:
            log.debug(zip_filename)
            self.callback(zip_filename, content)
        else:
            log.debug(filename)
            self.callback(filename, content)

    def run(self):
        try:
            for packet in self.reliable_stream():
                if not packet.filename in self.files:
                    self.files[packet.filename] = self.file_assembler_class(
                        packet.filename, self.handle_incoming)
                assembler = self.files[packet.filename]
                assembler.add_part(packet)
                #log.debug(repr(packet))
        except KeyboardInterrupt:
            log.info('Caught keyboard interrupt, closing connection')
            self.sock.close()
