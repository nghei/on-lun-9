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

import bars

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--host", dest="host", help="Host IP", default=None)
parser.add_option("--days", dest="days", type="int", help="Number of Days", default=30)
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

# Constants

market_am_open = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_am_open, "%H:%M:%S%z").time())
market_am_close = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_am_close, "%H:%M:%S%z").time())
market_pm_open = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_pm_open, "%H:%M:%S%z").time())
market_pm_close = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_pm_close, "%H:%M:%S%z").time())

market_open_duration_in_minutes = bars.market_open_duration // 60

# Preprocess

prices_folder = configParser.get("Main", "prices_folder")

dates = sorted(os.listdir(os.path.join(options.directory, prices_folder)))
dates = [d for d in dates if d <= options.date]

if len(dates) > options.days:
    dates = dates[-options.days:]

counts = {}
foos = {}
average_volumes = {}

for d in dates:
    foo = bars.read_all_bars(os.path.join(options.directory, prices_folder, d))
    for code in foo:
        if code not in data_whitelist:
            continue
        if code not in counts:
            counts[code] = 0
        if code not in foos:
            foos[code] = []
        counts[code] += 1
        foos[code] += foo[code]

for code in foos:
    average_volumes[code] = {}
    for i in range(1, market_open_duration_in_minutes + 1):
        v = 0
        for bar in foos[code]:
            if bars.time_since_open(datetime.fromtimestamp(bar.timestamp)) // 60 <= i:
                v += bar.px_volume
        average_volumes[code][i] = v / counts[code]

print("Past volume data loaded.", file=sys.stderr)

# Start

try:
    nfeeds = 0
    while True:
        (code, timestamp, px_open, px_high, px_low, px_last, px_volume) = sub_socket.recv().split()
        code = int(code)
        timestamp = float(timestamp)
        px_open = float(px_open)
        px_high = float(px_high)
        px_low = float(px_low)
        px_last = float(px_last)
        px_volume = float(px_volume)
        nfeeds += 1
        if px_volume is not None and px_volume > volumes[code]:
            volumes[code] = px_volume
            minutes_since_open = bars.time_since_open(datetime.fromtimestamp(timestamp)) // 60 + 1
            minutes_since_open = max(min(minutes_since_open, market_open_duration_in_minutes), 1)
            volume_ratio = px_volume / average_volumes[code][minutes_since_open]
            print("%d %f %f %f %f %f %f %f" % (code, timestamp, px_open, px_high, px_low, px_last, px_volume, volume_ratio), file=sys.stderr)
            socket.send(("%d %f %f %f %f %f %f %f" % (code, timestamp, px_open, px_high, px_low, px_last, px_volume, volume_ratio)).encode("utf-8"))
except KeyboardInterrupt:
    print("%d Feeds Received." % nfeeds, file=sys.stderr)
finally:
    sub_socket.close()
    socket.close()

