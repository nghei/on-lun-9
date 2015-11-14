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
parser.add_option("--start", dest="start", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--end", dest="end", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--code", dest="code", type="int", help="Code", default=None)
parser.add_option("--interval", dest="interval", type="float", help="Interval (Seconds)", default=0.5)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Set up publisher

data_feed_port = configParser.get("Data Feed", "data_feed_port")

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:%s" % data_feed_port)

# Constants

market_am_open = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_am_open, "%H:%M:%S%z").time())
market_am_close = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_am_close, "%H:%M:%S%z").time())
market_pm_open = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_pm_open, "%H:%M:%S%z").time())
market_pm_close = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_pm_close, "%H:%M:%S%z").time())

market_open_duration_in_minutes = bars.market_open_duration // 60

# Preprocess

codes = [options.code]

prices_folder = configParser.get("Main", "prices_folder")

dates = sorted(os.listdir(os.path.join(options.directory, prices_folder)))
dates = [d for d in dates if d >= options.start and d <= options.end]

foos = {}

for d in dates:
    foo = bars.read_all_bars(os.path.join(options.directory, prices_folder, d))
    for code in foo:
        if code in codes:
            if code not in foos:
                foos[code] = []
            foos[code] += foo[code]

# Start

current_date = None
try:
    for bar in foos[options.code]:
        if datetime.fromtimestamp(bar.timestamp).date() != current_date:
            current_date = datetime.fromtimestamp(bar.timestamp).date()
            current_open = bar.px_open
            current_high = bar.px_high
            current_low = bar.px_low
            current_volume = 0
        current_high = max(current_high, bar.px_high)
        current_low = min(current_low, bar.px_low)
        current_volume += bar.px_volume
        print("%d %f %f %f %f %f %f" % (options.code, bar.timestamp, current_open, current_high, current_low, bar.px_last, current_volume), file=sys.stderr)
        socket.send(("%d %f %f %f %f %f %f" % (options.code, bar.timestamp, current_open, current_high, current_low, bar.px_last, current_volume)).encode("utf-8"))
        time.sleep(options.interval)
except KeyboardInterrupt:
    print("Data Feed Shutting Down ...", file=sys.stderr)
finally:
    socket.close()


