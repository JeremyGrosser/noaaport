import itertools
import argparse
import zipfile
import os.path
import os
import io
import sys
import re

import noaaport.stream
import noaaport.emwin
import noaaport.nbs
import noaaport.log


DEFAULT_SERVERS = ['emwin.weathermessage.com:2211']
PROTOCOLS = {
    'nbs':      (noaaport.nbs.Connection, noaaport.nbs.FileAssembler),
    'emwin':    (noaaport.emwin.Connection, noaaport.emwin.FileAssembler),
}


log = noaaport.log.get_logger('noaaport.pipe')


class Pipe(object):
    def __init__(self, servers, protocol='emwin'):
        self.servers = servers
        self.protocol = protocol
        self.stream = None
        self.filters = []

    def __iter__(self):
        if self.protocol not in PROTOCOLS:
            log.crtical('Unknown protocol %r', self.protocol)
            return

        connection_class, file_assembler_class = PROTOCOLS[self.protocol]
        stream = noaaport.stream.Streamer(self.servers, connection_class, file_assembler_class)
        for func in self.filters:
            stream = filter(func, stream)
        return iter(stream)

    def add_filter(self, func):
        self.filters.append(func)

def write_file(filename, content, outdir):
    if filename.endswith('.TXT'):
        wmo = content.split(b'\r')[0]
        msgtype, center, timestamp  = wmo.split(b' ', 2)
        if timestamp.find(b' ') != -1:
            timestamp, bbb = timestamp.split(b' ', 1)
        timestamp = timestamp.decode('ascii')
        center = center.decode('ascii')

        filename = '%s.%s.%s' % (timestamp, center, filename)

    path = os.path.join(outdir, filename)
    with open(path, 'wb') as fd:
        fd.write(content)
        fd.flush()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', nargs='+', help='hostname:port', default=DEFAULT_SERVERS)
    parser.add_argument('--protocol', choices=['nbs', 'emwin'], default='emwin')
    parser.add_argument('--pattern', help='regex to filter filenames from the feed')
    parser.add_argument('--output', help='write output to a directory', default='data')
    parser.add_argument('--log-level', default='WARNING', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_argument('--delimiter', default='\n', help='Delimiter to use between files when writing to stdout')
    args = parser.parse_args()

    noaaport.log.set_level(args.log_level)

    servers = []
    for server in args.server:
        host, port = server.rsplit(':', 1)
        port = int(port)
        servers.append((host, port))

    pipe = Pipe(servers, args.protocol)

    if args.pattern is not None:
        pattern = re.compile(args.pattern, re.IGNORECASE)

        def filename_filter(data):
            filename, content, zip_filename = data
            return bool(pattern.match(zip_filename or filename))
        pipe.add_filter(filename_filter)

    if not os.path.exists(args.output):
        os.makedirs(args.output)

    for filename, content, zip_filename in pipe:
        log.info(zip_filename or filename)
        if filename.endswith('.ZIP'):
            content = content.rsplit(b'\x00', 1)[0]
            bio = io.BytesIO(content)
            try:
                zf = zipfile.ZipFile(bio)
                for name in zf.namelist():
                    content = zf.read(name)
                    write_file(name, content, args.output)
            except Exception as e:
                log.exception('unzip failed')
                write_file(filename, content, args.output)
            bio.close()
        else:
            write_file(filename, content.rstrip(b'\x00'), args.output)
