#!/bin/bash

mkdir -p /awips2/local/spool/warnprint /awips2/local/spool/noticeprint
screen  -t wiredown -dmS wiredown /home/awips/python-emwin/wiredown.py
