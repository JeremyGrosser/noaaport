import itertools
import argparse
import os.path
import os
import sys
import re

import noaaport.stream
import noaaport.emwin
import noaaport.nbs
import noaaport.log


DEFAULT_SERVERS = [('1.nbsp.inoaaport.net', 2210)]
PROTOCOLS = {
    'nbs':      (noaaport.nbs.Connection, noaaport.nbs.FileAssembler),
    'emwin':    (noaaport.emwin.Connection, noaaport.nbs.FileAssembler),
}


log = noaaport.log.get_logger('noaaport.pipe')


class Pipe(object):
    def __init__(self, servers=DEFAULT_SERVERS, protocol='nbs'):
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
            stream = itertools.ifilter(func, stream)
        return iter(stream)

    def add_filter(self, func):
        self.filters.append(func)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', nargs='+', help='hostname:port', default=['1.nbsp.inoaaport.net:2210'])
    parser.add_argument('--protocol', choices=['nbs', 'emwin'], default='nbs')
    parser.add_argument('--pattern', help='regex to filter filenames from the feed')
    parser.add_argument('--output', help='write output to a directory rather than stdout')
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

    for filename, content, zip_filename in pipe:
        log.info(zip_filename or filename)
        if args.output is not None:
            if not os.path.exists(args.output):
                os.makedirs(args.output)
            with open(os.path.join(args.output, zip_filename or filename), 'w') as fd:
                fd.write(content)
                fd.flush()
        else:
            sys.stdout.write(content + args.delimiter)
            sys.stdout.flush()
