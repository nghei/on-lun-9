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

import proxy
import realtime
import worker

parser = OptionParser()
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--workers", type="int", dest="workers", help="number of workers", default=50)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Update proxies

proxy_sites = configParser.get("Data Feed", "proxy_sites").split(",")
proxy_links = int(configParser.get("Data Feed", "proxy_links"))
proxy_file = configParser.get("Data Feed", "proxy_file")

proxies = proxy.check_proxies(proxy.crawl(proxy_sites, proxy_links), workers=options.workers)

with open(proxy_file, 'r') as f:
    original_proxies = f.read().split("\n")
    original_proxies = [ p for p in original_proxies if p != "" ]

proxies = list(set(proxies + original_proxies))

with open(proxy_file, 'w') as f:
    for p in proxies:
        print(p, file=f)

