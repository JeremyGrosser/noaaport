#!/usr/bin/env python
import emwin

from zipfile import ZipFile
from socket import socket
import os.path
import sys
import os

import logging
log = logging.getLogger('emwin')
log.setLevel(logging.DEBUG)

def main():
    files = {}
    host = ('1.nbsp.inoaaport.net', 2211)

    def handle(filename, content):
        path = '/mnt/media/nws/%s' % filename[:3]
        try:
            os.makedirs(path)
        except:
            pass
        filepath = os.path.join(path, filename[3:])
        log.info('Writing %s' % filepath)

        fd = file(filepath, 'wb')
        fd.write(content)
        fd.close()
        del files[filename]

        return
        # This doesn't work because of null padding issues
        if filename.endswith('.ZIS'):
            try:
                archive = ZipFile(filepath, 'r')
                for afile in archive.namelist():
                    log.info('Unpacking %s from %s' % (afile, filename))
                    data = archive.read(afile)
                    fd = file(os.path.join(path, afile[3:]))
                    fd.write(data)
                    fd.close()
                os.unlink(filepath)
            except:
                log.warning('Unable to unpack %s: %s' % (filename, sys.exc_info()[1]))

    while True:
        log.info('Connecting to %s' % repr(host))
        sock = socket()
        sock.connect(host)

        conn = emwin.Connection(sock)
        for packet in conn:
            #pprint(packet.dict())
            if not packet.filename in files:
                files[packet.filename] = emwin.FileAssembler(packet.filename, handle)
            assembler = files[packet.filename]
            assembler.add_part(packet)

if __name__ == '__main__':
    main()
