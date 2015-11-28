#!/usr/bin/env python

import pyximport; pyximport.install()
import sys
import os
import operator
import datetime as dt
from datetime import datetime
from optparse import OptionParser
import configparser
import numpy
import pandas

import features

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--experiment", dest="expt", help="Name of Experiment", default=None)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Constants

experiment_folder = configParser.get(options.expt, "experiment_folder")

history_days = int(configParser.get(options.expt, "history_days"))

features = None
targets = None

model = configParser.get(options.expt, "model")

model_time = configParser.get(options.expt, "feature_times")  #

# Utility Functions

model_re = re.compile("")

def parse_model_params(model):
    pass

# Read Samples

memory = {}

input_hdf = pandas.HDFStore(os.path.join(options.directory, experiment_folder, "samples.h5"))

features_df = None
targets_df = None

input_hdf.close()

all_dates = sorted(list(set([datetime.fromtimestamp(t).date() for t in features_df["timestamp"].drop_duplicates()])))

dates = [d for d in all_dates if d <= datetime.strptime(options.date, "%Y%m%d").date()]

date_count = 0
sample_count = 0

while date_count < history_days:
    split_date = dates[-(target_min_days+date_count)]
    end_date = max([dates[-(target_min_days+date_count-i+1)] for i in range(target_min_days, target_max_days+1)])
    start_date = dates[-(feature_days+target_min_days+date_count)]
    start_date_timestamp = datetime.combine(start_date, dt.time(0, 0)).timestamp()
    end_date_timestamp = datetime.combine(end_date, dt.time(0, 0)).timestamp()
    date_span = dates.index(end_date) - dates.index(start_date) + 1
    start_timestamp = datetime.combine(start_date, features.market_am_open_time.time()).timestamp()
    end_timestamp = datetime.combine(end_date, features.market_pm_close_time.time()).timestamp()
    split_timestamp = datetime.combine(split_date, datetime.strptime(model_time, "%H:%M:%S").time()).timestamp()
    sample_count += 1
    print("(%s, %s, %s, %d)" % (start_date, split_date, end_date, code), file=sys.stderr)
    if start_date <= dates[0]:
        break
    date_count += 1

print("%d samples read." % sample_count, file=sys.stderr)

