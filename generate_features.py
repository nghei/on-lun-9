#!/usr/bin/env python

import sys
import os
import operator
from datetime import datetime
from optparse import OptionParser
import configparser
import math
import numpy
import pandas
import sqlite3

import batch
import records

import features

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Constants

market_open_minutes = features.market_open_minutes

daily_samples_folder = configParser.get("Sample", "daily_samples_folder")

feature_days = int(configParser.get("Train", "feature_days"))

feature_times = configParser.get("Train", "feature_times").split(",")

wavelets = configParser.get("Train", "wavelets").split(",")
wavelet_detail_per_level = int(configParser.get("Train", "wavelet_detail_per_level"))
wavelet_scaling_per_level = float(configParser.get("Train", "wavelet_scaling_per_level"))

targets = configParser.get("Train", "targets").split(",")

# Add features to daily sample

hdf = pandas.HDFStore(os.path.join(options.directory, daily_samples_folder, "%s.h5" % options.date))

hdf_tables = hdf.keys()
for table in hdf_tables:
    if table != 'raw' and table != '/raw':
        del hdf[table]

count = 0

input_df = hdf['raw']
codes = input_df['code'].drop_duplicates()
for code in codes:
    input_series = input_df[input_df['code'] == code].reset_index(drop=True)
    end_date = datetime.fromtimestamp(input_series['timestamp'][feature_days * market_open_minutes])
    for feature_time in feature_times:
        end_timestamp = datetime.combine(end_date.date(), datetime.strptime(feature_time, "%H:%M:%S").time()).timestamp()
        (independent_series, expected_range, dependent_series) = features.split_series(input_series, end_timestamp)
        (output_targets, output_target_titles) = features.generate_targets(dependent_series, expected_range, targets)
        output_target_df_name = "targets_%s" % feature_time
        output_target_df = pandas.DataFrame([output_targets], columns=output_target_titles, dtype=numpy.float64)
        if output_target_df_name not in hdf:
            hdf.put(output_target_df_name, output_target_df, format="table", data_columns=True)
        else:
            hdf.append(output_target_df_name, output_target_df, format="table", data_columns=True)
        for wavelet in wavelets:
            output_features = features.generate_wavelet_features(independent_series, wavelet, wavelet_detail_per_level, wavelet_scaling_per_level)
            output_feature_titles = ["input_%d" % (i + 1) for i in range(0, len(output_features))]
            output_feature_df_name = "%s_%s" % (wavelet, feature_time)
            output_feature_df = pandas.DataFrame([output_features], columns=output_feature_titles)
            if output_feature_df_name not in hdf:
                hdf.put(output_feature_df_name, output_feature_df, format="table", data_columns=True)
            else:
                hdf.append(output_feature_df_name, output_feature_df, format="table", data_columns=True)
    count += 1
    if count >= 100:
        break

for table in hdf.keys():
    if table != 'raw' and table != '/raw':
        hdf[table] = hdf[table].reset_index(drop=True)
    
hdf.close()

print("Features and targets added to %s." % os.path.join(options.directory, daily_samples_folder, "%s.h5" % options.date), file=sys.stderr)

