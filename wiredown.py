#!/awips2/python/bin/python
import logging
import socket
import os.path
import os
import sys
import shutil

import nbs

import time

# For edex notifications
from ufpy.qpidingest import *

warnprintdir = "/awips2/local/spool/warnprint"
noticeprintdir = "/awips2/local/spool/noticeprint"


class Streamer(object):
    def __init__(self, hosts):
        self.hosts = hosts
        self.log = logging.getLogger('nbs')
        self.log.setLevel(logging.DEBUG)
        self.files = {}
        self.count = 0
        self.basedir = "/awips2/data_store"

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
        self.log.debug(filename)
        ccblen = 2 * (((ord(content[0]) & 63) << 8) + ord(content[1]))
        hdr = content[ccblen:].splitlines()
        # initialize these to gibberish just in case
        # processing the header fails
        (wmo,site,ts) = ('NPHX99', 'UNKN', time.strftime("%d%H%M"))
        pil = "NONPIL"
        try:
            (wmo,site,ts) = hdr[0].split(' ')[0:3]
            pil = hdr[2]
        except:
            print "WTF\n\tHDR 1: %s\n\tHDR 2: %s\n\tHDR 3: %s\n\tHDR 4: %s\n\tHDR 5: %s\n" % (hdr[0], hdr[1], hdr[2], hdr[3], hdr[4])
        if pil == '@pil':
           # KNCF is currently dropping in @pil as the pilcode
           pil = 'MONMSG'
        if wmo[0:2] == 'NT':
          outdir = "{BASEDIR}/tstmsg/{SITE}".format(BASEDIR=self.basedir, SITE=site)
          outfile = "{WMO}_{SITE}_{DATE}_{WALLTIME}".format(WMO=wmo, SITE=site, DATE=ts, WALLTIME=time.strftime("%Y%m%d%H%M%S"))
        elif pil == 'MONMSG':
          outdir = "{BASEDIR}/tstmsg/{SITE}".format(BASEDIR=self.basedir, SITE=site)
          outfile = "{WMO}_{SITE}_{DATE}_{WALLTIME}".format(WMO=wmo, SITE=site, DATE=ts, WALLTIME=time.strftime("%Y%m%d%H%M%S"))
        else:   
          outdir = "{BASEDIR}/nwws/{DATE}/{HR}/{SITE}".format(BASEDIR=self.basedir, DATE=time.strftime("%Y%m%d"), HR=time.strftime("%H"), SITE=site)
          outfile = "{WMO}_{SITE}_{PIL}_{DATE}.txt".format(WMO=wmo, SITE=site, PIL=pil.replace(' ', '_'), DATE=ts)
        odof = "%s/%s" % (outdir,outfile)
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        fd = file(odof, 'w')
        fd.write(content[ccblen:])
        fd.flush()
        fd.close()
        npcode = "%s /p%s" % (hdr[0], pil)
        self.log.debug("%s (%s)" % (odof, npcode))
        z.sendmessage(odof, npcode)
        if wmo[0] == 'W':
            do_warn(wmo, site, pil, content[ccblen:])
        if wmo[1] == 'N':
            do_notice(wmo, site, pil, content[ccblen:])

  

    def run(self):
        for packet in self.reliable_stream():
            if not packet.filename in self.files:
                self.files[packet.filename] = nbs.FileAssembler(
                    packet.filename, self.handle_incoming)
            assembler = self.files[packet.filename]
            assembler.add_part(packet)
            #self.log.debug(repr(packet))

"""
The idea with the do_{warn,canwarn,notice} functions is that they would be used
for immediate and special handling of warnings and notices.  Presently, I think
I'll have them print out.  In the future, we may do something crazy involving
blinkenlights and buzzers.

Since the WMO codes are highly regular, it is much easier to determine if a 
product is actually important.  First letter = 'W' is a warning.  First letter
is 'N', it's a notice.  Second letter of notice is 'W'?  Well, that's a notice
about a warning, which is generally a cancellation.

See https://www.wmo.int/pages/prog/www/ois/Operational_Information/Publications/WMO_386/AHLsymbols/TableB1.html
for all the goodies

Where WMO codes break down and the PIL takes over is when we see things
like localized forecasts.  KJAN will produce numerous reports for a 
whole slew of products for various cities and they'll have the same WMO
code.  For example, JAN issues CLI products for several cities under
the same WMO code:

fxatext=# select distinct cccid, nnnid, site, wmoid, xxxid, bbbid, nnnid || xxxid as pil from stdtextproducts where nnnid = 'CLI' and site = 'KJAN' and xxxid != 'JAN' ;         
 cccid | nnnid | site | wmoid  | xxxid | bbbid |  pil   
-------+-------+------+--------+-------+-------+--------
 JAN   | CLI   | KJAN | CDUS44 | GLH   |       | CLIGLH
 JAN   | CLI   | KJAN | CDUS44 | HBG   |       | CLIHBG
 JAN   | CLI   | KJAN | CDUS44 | MEI   |       | CLIMEI
 JAN   | CLI   | KJAN | CDUS44 | TVR   |       | CLITVR
 JAN   | CLI   | KJAN | CDUS44 | GWO   |       | CLIGWO
(5 rows)

"""

def do_tstmsg(wmo,site,pil,content):
    """
    Special handling for test messages
    """
    print "* * * %s sent a test message * * *" % site
    return True

def do_warn(wmo,site,pil,content):
    """
    Writes a warning to the warning print spool
    """

    # Special handling for test messages
    if wmo[0:2] == 'NT':  
       do_tstmsg(wmo,site,pil,content)
       return True
    elif pil == 'MONMSG':
        do_tstmsg(wmo,site,pil,content)
        return True
       
    if wmo[0:2] == 'NW':
       print "* * * WARNING CANCELLATION * * *"
    else:
       print "* * * W A R N I N G  R E C E I V E D * * *"
    print content
    ts = time.strftime("%Y%m%d%H%M%S")
    fp = open("%s/%s_%s_%s.txt" % (warnprintdir,wmo,pil.strip(),ts), 'w')
    fp.write(content)
    fp.flush()
    fp.close()
    return True
   
def do_canwarn(wmo,site,pil,content):
    """
    Writes a warning cancellation to the warning print spool.
    """
    do_warn(wmo,site,pil,content)

def do_notice(wmo,site,pil,content):
    """
    Writes a notice to the notice print spool, unless it is
    a warning cancellation, in which case we call do_canwarn
    """


    # Special handling for test messages
    if wmo[0:2] == 'NT':
       do_tstmsg(wmo,site,pil,content)
       return True
    elif pil == 'MONMSG':
        do_tstmsg(wmo,site,pil,content)
        return True

    if wmo[1] == 'W':
        # Special handling for warning cancellations
        do_canwarn(wmo,site,pil,content)
        return True
    print "* * * NOTICE RECEIVED * * *" 
    print content
    ts = time.strftime("%Y%m%d%H%M%S")
    fp = open("%s/%s_%s_%s.txt" % (noticeprintdir,wmo,pil.strip(),ts), 'w')
    fp.write(content)
    fp.flush()
    fp.close()
    return True

def main():
    (host,port,ddir) = ('w.nbsp.inoaaport.net', 2210)
    s = Streamer([(host, port)], ddir)
    s.run()


if __name__ == '__main__':
    os.makedirs(warnprintdir)
    os.makedirs(noticeprintdir)
    os.environ['TZ'] = 'GMT'
    z = IngestViaQPID()
    main()
