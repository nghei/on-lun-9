#!/usr/bin/env python

import sys
import os
import traceback
from datetime import datetime
import time
import random
import multiprocessing
import queue
from optparse import OptionParser
import configparser
import zmq

import proxy
import worker
import realtime
import bars

parser = OptionParser()
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Read Constants

market_am_open = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_am_open, "%H:%M:%S%z").time())
market_am_close = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_am_close, "%H:%M:%S%z").time())
market_pm_open = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_pm_open, "%H:%M:%S%z").time())
market_pm_close = datetime.combine(datetime.today().date(), datetime.strptime(bars.market_pm_close, "%H:%M:%S%z").time())

pause_wait = float(configParser.get("Data Feed", "pause_wait"))
resume_wait = float(configParser.get("Data Feed", "resume_wait"))

# Set up proxies

try:
    with open(configParser.get("Data Feed", "proxy_file"), 'r') as f:
        proxies = f.read().split("\n")
    proxies = [ p for p in proxies if p != "" ]
except:
    proxy_sites = configParser.get("Data Feed", "proxy_sites").split(",")
    proxy_links = int(configParser.get("Data Feed", "proxy_links"))
    proxies = proxy.crawl(proxy_sites, proxy_links)

proxies = proxy.check_proxies(proxies)

try:
    if len(proxies) < 1:
        proxies = [None]
except NameError:
    proxies = [None]

# Set up publisher

data_feed_port = configParser.get("Data Feed", "data_feed_port")

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:%s" % data_feed_port)

# Set up WorkerPool and results

data_whitelist = [ int(x) for x in configParser.get("Main", "data_whitelist").split(",") ]

volumes = {}

for code in data_whitelist:
    volumes[code] = 0.0

class Task:
    def __init__(self, code, index, proxy):
        self.code = code
        self.index = index
        self.proxy = proxy
    def __call__(self):
        try:
#            print("Running %d ..." % self.code, file=sys.stderr)
            (timestamp, px_open, px_high, px_low, px_last, px_volume) = realtime.get_realtime_data(code=self.code, index=self.index, proxy=self.proxy)
            return (self.code, timestamp, px_open, px_high, px_low, px_last, px_volume)
        except Exception as e:
#            traceback.print_exc(file=sys.stderr)
            return (self.code, None, None, None, None, None, None)  # MUST preserve the code even if errors occurred

data_feed_threads = int(configParser.get("Data Feed", "data_feed_threads"))

pool = worker.WorkerPool(data_feed_threads)

# Start

print("Starting Data Feed Using %d Proxies ..." % len(proxies), file=sys.stderr)

try:
    pool.start()
    paused = True
    resume = False
    loop = 0
    count = 0
    nfeeds = 0
    while True:
        loop += 1
        try:
            result = pool.get(timeout=0.5)
        except queue.Empty:
            result = None
#            print("Waiting ...", file=sys.stderr)
        except Exception as e:
            print(e, file=sys.stderr)
        if result:
            (code, timestamp, px_open, px_high, px_low, px_last, px_volume) = result
            if px_volume is not None and px_volume > volumes[code]:
                volumes[code] = px_volume
                print("%d %f %f %f %f %f %f" % (code, timestamp, px_open, px_high, px_low, px_last, px_volume), file=sys.stderr)
                socket.send(("%d %f %f %f %f %f %f" % (code, timestamp, px_open, px_high, px_low, px_last, px_volume)).encode("utf-8"))
                nfeeds += 1
        if paused:
            resume = (datetime.now().timestamp() >= market_am_open.timestamp() - resume_wait and datetime.now().timestamp() < market_am_close.timestamp()) or (datetime.now().timestamp() >= market_pm_open.timestamp() - resume_wait and datetime.now().timestamp() < market_pm_close.timestamp())
            if resume:
                paused = False
                resume = False
                for code in data_whitelist:
                    pool.put(Task(code, count, proxies[count % len(proxies)]))
#                    print("Submitting %d ..." % code, file=sys.stderr)
                    count += 1
        else:
            if result:
                (code, timestamp, px_open, px_high, px_low, px_last, px_volume) = result
                pool.put(Task(code, count, proxies[count % len(proxies)]))
#                print("Submitting %d ..." % code, file=sys.stderr)
                count += 1
            paused = (datetime.now().timestamp() >= market_am_close.timestamp() + pause_wait and datetime.now().timestamp() < market_pm_open.timestamp() - resume_wait) or (datetime.now().timestamp() >= market_pm_close.timestamp() + pause_wait)
except KeyboardInterrupt:
    print("%d Feeds Published." % nfeeds, file=sys.stderr)
    print("Data Feed Shutting Down ...", file=sys.stderr)
finally:
    socket.close()
    pool.terminate()

