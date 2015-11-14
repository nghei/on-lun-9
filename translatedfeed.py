#!/usr/bin/env python

import sys
import os
from datetime import datetime
import time
import random
import multiprocessing
import queue
from optparse import OptionParser
import configparser
import zmq

parser = OptionParser()
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--host", dest="host", help="Host IP", default=None)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Set up subscriber and translated feed publisher

data_feed_port = configParser.get("Data Feed", "data_feed_port")

context = zmq.Context()
sub_socket = context.socket(zmq.SUB)
sub_socket.connect("tcp://%s:%s" % (options.host, data_feed_port))

data_whitelist = [ int(x) for x in configParser.get("Main", "data_whitelist").split(",") ]

volumes = {}

for code in data_whitelist:
    sub_socket.setsockopt(zmq.SUBSCRIBE, str(code).encode("utf-8"))
    volumes[code] = 0.0

translated_feed_port = configParser.get("Data Feed", "translated_feed_port")

socket = context.socket(zmq.PUB)
socket.bind("tcp://*:%s" % translated_feed_port)

# Start

try:
    nfeeds = 0
    while True:
        (code, timestamp, px_open, px_high, px_low, px_last, px_volume) = sub_socket.recv().split()
        code = int(code)
        timestamp = float(timestamp)
        px_last = float(px_last)
        px_volume = float(px_volume)
        nfeeds += 1
        if px_volume is not None and px_volume > volumes[code]:
            dVolume = px_volume - volumes[code]
            volumes[code] = px_volume
            print("%d %f %f %f" % (code, timestamp, px_last, dVolume), file=sys.stderr)
            socket.send(("%d %f %f %f" % (code, timestamp, px_last, dVolume)).encode("utf-8"))
except KeyboardInterrupt:
    print("%d Feeds Received." % nfeeds, file=sys.stderr)
finally:
    sub_socket.close()
    socket.close()

