#!/usr/bin/python

import os,sys

ETX = chr(0x03)
SOH = chr(0x01)

class WMO(object):
    """
    This class is to be used with python-emwin to format its output messages
    in such a way that we can stuff them into ldm's pqing
    """
    def __init__(self, outch):
        self.count = 0
        # Use the write method of our output channel
        # this makes it easy to go from stdout to a popened
        # file handle to pqing(1)
        self.sw = outch.write

        # send a string of ETXs to make sure we're not in any products
        # and then follow with a proper WMO ending
        self.sw(ETX + ETX + ETX + "\r\r\n" + ETX)

    def seq(self):
        # WMO sequence numbers are from 000 to 999.  We format to three digits in the output
        # this is a little sillier than returning the ++'d counter since we need
        # it to start at 0, have to keep it under 1000, and want it done in one place
        r = self.count
        self.count += 1
        if self.count > 999:
            self.count = 0
        return r

    def emit(self, content):
        # from http://www.nws.noaa.gov/tg/head.php
        # WMO messges start with SOH, the CRCRLF string, a sequence number, and
        # since we're using python-emwin, we need another \r\r\n before the proper
        # address header
        self.sw(SOH + "\r\r\n%03d\r\r\n" % self.seq())
        # and now we emit the content
        self.sw(content)
        # and finally the footer
        self.sw("\r\r\n" + ETX)
