from socket import socket
import emwin


def main():
    sock = socket()
    sock.connect(('1.nbsp.inoaaport.net', 2211))

    files = {}

    def handle(filename, content):
        print 'Writing /tmp/noaa/%s' % filename
        fd = file('/tmp/noaa/%s' % filename, 'wb')
        fd.write(content)
        fd.close()
        del files[filename]

    conn = emwin.Connection(sock)
    for packet in conn:
        #pprint(packet.dict())
        if not packet.filename in files:
            files[packet.filename] = emwin.FileAssembler(packet.filename, handle)
        assembler = files[packet.filename]
        assembler.add_part(packet)

if __name__ == '__main__':
    main()
