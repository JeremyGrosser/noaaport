#!/usr/bin/env python
import logging
import socket
import emwin


class Streamer(object):
    def __init__(self, hosts):
        self.hosts = hosts
        self.log = logging.getLogger('emwin')
        self.log.setLevel(logging.DEBUG)
        self.files = {}

    def stream(self, host):
        self.log.info('Connecting to %s:%i' % host)
        self.sock = socket.socket()
        self.sock.connect(host)
        return emwin.Connection(self.sock)

    def reliable_stream(self):
        i = 0
        while True:
            try:
                for packet in self.stream(self.hosts[i]):
                    yield packet
            except Exception, e:
                self.log.error('Stream error %r: %s' % (self.hosts[i], str(e)))
            i = (i + 1) % len(self.hosts)

    def handle_incoming(self, filename, content):
        del self.files[filename]
        self.log.debug(filename)

    def run(self):
        for packet in self.reliable_stream():
            if not packet.filename in self.files:
                self.files[packet.filename] = emwin.FileAssembler(
                    packet.filename, self.handle_incoming)
            assembler = self.files[packet.filename]
            assembler.add_part(packet)


def main():
    s = Streamer([
        ('1.pool.iemwin.net', 2211),
        ('2.pool.iemwin.net', 2211),
        ('1.nbsp.inoaaport.net', 2211),
        ('2.nbsp.inoaaport.net', 2211),
        ('3.nbsp.inoaaport.net', 2211),
        ('texoma.wxpro.net', 2211),
        ('emwin.aprsfl.net', 2211),
    ])
    s.run()


if __name__ == '__main__':
    main()
