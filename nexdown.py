#!/awips2/python/bin/python
import logging
import socket
import os.path
import os
import sys
import shutil
import time

import nbs

# For edex notifications
from ufpy.qpidingest import *

class Streamer(object):
    def __init__(self, hosts):
        self.hosts = hosts
        self.log = logging.getLogger('nbs')
        self.log.setLevel(logging.DEBUG)
        self.files = {}
        self.outdir = "/awips2/data_store/radar/{SITE}/{PCODE}/{WMO}_{SITE}_{PCODE}_{TIME}.rad"
        self.count = 0
    def stream(self, host):
        self.log.info('Connecting to %s:%i' % host)
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
                self.log.error('Stream error %r: %s' % (self.hosts[i], str(e)))
            i = (i + 1) % len(self.hosts)

    def handle_incoming(self, filename, content):
        self.count += 1
        del self.files[filename]
        #self.log.debug(filename)
        ccblen = 2 * (((ord(content[0]) & 63) << 8) + ord(content[1]))
        # We need to build awips path name here
        # /awips2/data_store/radar/(SITE)/(PCODE)/(WMO)_(SITE)_(PCODE)_(DATE)_(seq).rad
        #outfile = "%s/%s" % (self.outdir, filename)
        hdr = content[ccblen:].splitlines()
        (wmo,site,ts) = hdr[0].split(' ', 3)
        (dd,hh,mm) = (ts[0:2], ts[2:4], ts[4:6])
        # ['SDUS84 KOUN 260459', '', 'N0HFDR', '', ...
        pil = hdr[2]
        pcode = pil[0:3]
        # dir DATE is receive date
        outdir = "/awips2/data_store/radar/{DATE}/{HR}/{SITE}/{PCODE}".format(DATE=time.strftime("%Y%m%d"), HR=time.strftime("%H"), SITE=site, PCODE=pil[0:3])
        outfile = "{WMO}_{SITE}_{PIL}_{DATE}.rad".format(WMO=wmo, SITE=site, PIL=pil, DATE=ts)
        odof = "%s/%s" % (outdir,outfile)
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        fd = file(odof, 'w')
        fd.write(content)
        fd.flush()
        fd.close()
        npcode = "%s /p%s" % (hdr[0], pil)
        self.log.debug("%s (%s)" % (odof, npcode))
        z.sendmessage(odof, npcode)
  

    def run(self):
        for packet in self.reliable_stream():
            if not packet.filename in self.files:
                self.files[packet.filename] = nbs.FileAssembler(
                    packet.filename, self.handle_incoming)
            assembler = self.files[packet.filename]
            assembler.add_part(packet)
            #self.log.debug(repr(packet))


def main():
    host = "3.nbsp.inoaaport.net"
    port = 2210
    s = Streamer([(host, port)])
    s.run()


if __name__ == '__main__':
    os.environ['TZ'] = 'GMT'
    z = IngestViaQPID()
    main()
