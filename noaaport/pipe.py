import noaaport.stream
import noaaport.emwin
import noaaport.nbs
import noaaport.log

import functools
import argparse
import os.path
import os
import re


log = noaaport.log.get_logger('noaaport.pipe')


def print_file_callback(filename, content):
    log.info(filename)
    print content


def write_file_callback(filename, content, directory='data/'):
    with open(os.path.join(directory, filename), 'w') as fd:
        fd.write(content)
        fd.flush()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', nargs='+', help='hostname:port', default=['1.nbsp.inoaaport.net:2210'])
    parser.add_argument('--protocol', choices=['nbs', 'emwin'], default='nbs')
    parser.add_argument('--pattern', help='regex to filter filenames from the feed')
    parser.add_argument('--output', help='write output to a directory rather than stdout')
    parser.add_argument('--log-level', default='WARNING', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    args = parser.parse_args()

    noaaport.log.set_level(args.log_level)

    if args.protocol == 'nbs':
        connection_class = noaaport.nbs.Connection
        file_assembler_class = noaaport.nbs.FileAssembler
    elif args.protocol == 'emwin':
        connection_class = noaaport.emwin.Connection
        file_assembler_class = noaaport.emwin.FileAssembler
    else:
        log.critical('Unknown protocol %r, abort.', args.protocol)
        return 1

    if args.output is not None:
        if not os.path.exists(args.output):
            os.makedirs(args.output)
        callback = functools.partial(write_file_callback, directory=args.output)
    else:
        callback = print_file_callback

    if args.pattern is not None:
        old_callback = callback
        pattern = re.compile(args.pattern, re.IGNORECASE)
        def filename_filter(filename, content):
            if not pattern.match(filename):
                return
            else:
                return old_callback(filename, content)
        callback = filename_filter


    servers = []
    for server in args.server:
        host, port = server.rsplit(':', 1)
        servers.append((host, int(port)))

    s = noaaport.stream.Streamer(
        servers, callback, connection_class, file_assembler_class)
    s.run()
