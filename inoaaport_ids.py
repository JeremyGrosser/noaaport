#!/usr/bin/env python
import logging
import socket
import os.path
import os
import sys
import shutil
from wmo import WMO

import nbs


class Streamer(object):
    def __init__(self, hosts, feed, outch):
        self.hosts = hosts
        #self.log = logging.getLogger('nbs')
        #self.log.setLevel(logging.DEBUG)
        self.files = {}
        self.count = 0
        self.wmo = WMO(outch)

    def stream(self, host):
        #self.log.info('Connecting to %s:%i' % host)
        self.sock = socket.socket()
        self.sock.connect(host)
        return nbs.Connection(self.sock)

    def reliable_stream(self):
        i = 0
        while True:
            try:
                for packet in self.stream(self.hosts[i]):
                    yield packet
            except Exception, e:
                None
                #self.log.error('Stream error %r: %s' % (self.hosts[i], str(e)))
            i = (i + 1) % len(self.hosts)

    def handle_incoming(self, filename, content):
        self.count += 1
        del self.files[filename]
        #self.log.debug(filename)
        ccblen = 2 * (((ord(content[0]) & 63) << 8) + ord(content[1]))
        self.wmo.emit(content[ccblen:])
        #outfile = "%s/%s" % (self.outdir, filename)
        #if not os.path.exists(self.outdir):
        #    os.makedirs(self.outdir)
        #fd = file(outfile, 'w')
        #fd.write(content[ccblen:])
        #fd.flush()
        #fd.close()
        #try:
        #  hdr = content[ccblen:].splitlines()[0].strip()
        #  self.log.debug("   %s %06d" % (hdr, self.count))
        #  shutil.move(outfile, "/awips2/edex/data/manual/")
        #  #os.system("/awips2/ldm/bin/pqinsert -p \"%s\" -l - %s" % (hdr, outfile))
        #except:
        #  print "INGEST FAILED"
  

    def run(self):
        for packet in self.reliable_stream():
            if not packet.filename in self.files:
                self.files[packet.filename] = nbs.FileAssembler(
                    packet.filename, self.handle_incoming)
            assembler = self.files[packet.filename]
            assembler.add_part(packet)
            #self.log.debug(repr(packet))


def main():
    pqp = os.popen("/awips2/ldm/bin/pqing -f IDS -l /awips2/ldm/var/logs/ids_noaaport.log -v -", 'w')
    s = Streamer([('1.nbsp.inoaaport.net', 2210)], 'IDS', pqp)
    s.run()


if __name__ == '__main__':
    main()
