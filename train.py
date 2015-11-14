#!/usr/bin/env python

import sys
import os
import operator
from datetime import datetime
from optparse import OptionParser
import configparser
import math
import numpy

import batch
import records

import bars, xs

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

ref_date = datetime.strptime(options.date + "2359", "%Y%m%d%H%M")

# Utility Functions

# Codes

# Run

all_dates = sorted(os.listdir(os.path.join(options.directory, symbols_folder)))

samples_folder = configParser.get("Sample", "samples_folder")

dates = [d for d in dates if d <= options.date]
dates = dates[-sample_history_days:]

samples = []

