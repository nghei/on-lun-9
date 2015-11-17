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

feature_days = int(configParser.get(options.expt, "feature_days"))

feature_times = configParser.get(options.expt, "feature_times").split(",")

wavelets = configParser.get(options.expt, "wavelets").split(",")
wavelet_detail_per_level = int(configParser.get(options.expt, "wavelet_detail_per_level"))

target_min_days = int(configParser.get(options.expt, "target_min_days"))
target_max_days = int(configParser.get(options.expt, "target_max_days"))

targets = configParser.get(options.expt, "targets").split(",")

# Utility Functions

# Generate features

input_hdf = pandas.HDFStore(os.path.join(options.directory, experiment_folder, "data.h5"))

price_df = input_hdf["price"]
eligible_df = input_hdf["eligible"]

output_hdf = pandas.HDFStore(os.path.join(options.directory, experiment_folder, "samples.h5"))

memory = {}

for table in output_hdf.keys():
    if table not in memory:
        memory[table] = output_hdf[table]

codes = eligible_df["code"].drop_duplicates()

all_dates = sorted(list(set([datetime.fromtimestamp(t).date() for t in price_df["timestamp"].drop_duplicates()])))

dates = [d for d in all_dates if d <= datetime.strptime(options.date, "%Y%m%d").date()]

date_count = 0
sample_count = 0

while date_count < (target_max_days-target_min_days+1):
    split_date = dates[-(target_min_days+date_count)]
    end_date = max([dates[-(target_min_days+date_count-i+1)] for i in range(target_min_days, target_max_days+1)])
    start_date = dates[-(feature_days+target_min_days+date_count)]
    start_date_timestamp = datetime.combine(start_date, dt.time(0, 0)).timestamp()
    end_date_timestamp = datetime.combine(end_date, dt.time(0, 0)).timestamp()
    date_span = dates.index(end_date) - dates.index(start_date) + 1
    start_timestamp = datetime.combine(start_date, features.market_am_open_time.time()).timestamp()  #
    end_timestamp = datetime.combine(end_date, features.market_pm_close_time.time()).timestamp()  #
    for code in codes:
        code_eligibility = eligible_df[(eligible_df["code"] == code) & (eligible_df["timestamp"] >= start_date_timestamp) & (eligible_df["timestamp"] <= end_date_timestamp)]
        if code_eligibility.shape[0] < date_span:
            continue
        if code <= 99999:
            price_series = price_df[(price_df["code"] == code) & (price_df["timestamp"] >= start_timestamp) & (price_df["timestamp"] <= end_timestamp)].reset_index(drop=True)
        else:
            code1 = code // 100000
            code2 = code % 100000
            tmp_series1 = price_df[(price_df["code"] == code1) & (price_df["timestamp"] >= start_timestamp) & (price_df["timestamp"] <= end_timestamp)].reset_index(drop=True)
            tmp_series2 = price_df[(price_df["code"] == code2) & (price_df["timestamp"] >= start_timestamp) & (price_df["timestamp"] <= end_timestamp)].reset_index(drop=True)
            p1 = tmp_series1["px_open"][0]
            p2 = tmp_series2["px_open"][0]
            price_series = pandas.DataFrame(None, columns=tmp_series1.columns)
            price_series["code"] = pandas.Series(numpy.ones(tmp_series1.shape[0]) * code)
            price_series["timestamp"] = tmp_series1["timestamp"]
            price_series["px_open"] = tmp_series1["px_open"] / p1 / 2 + tmp_series2["px_open"] / p2 / 2
            price_series["px_high"] = tmp_series1["px_high"] / p1 / 2 + tmp_series2["px_high"] / p2 / 2
            price_series["px_low"] = tmp_series1["px_low"] / p1 / 2 + tmp_series2["px_low"] / p2 / 2
            price_series["px_last"] = tmp_series1["px_last"] / p1 / 2 + tmp_series2["px_last"] / p2 / 2
            price_series["px_volume"] = pandas.Series(numpy.zeros(tmp_series1.shape[0]))  #
        for t in feature_times:
            split_timestamp = datetime.combine(split_date, datetime.strptime(t, "%H:%M:%S").time()).timestamp()
            X_series, expected_range, Y_series = features.split_series(price_series, split_timestamp)
            for wavelet in wavelets:
                X = features.generate_wavelet_features(X_series, wavelet, wavelet_detail_per_level, 1)
                X_titles = ["coef_%d" % (i + 1) for i in range(0, X.shape[0])]
                X_df = pandas.DataFrame([X], columns=X_titles, dtype=numpy.float64)
                X_df["start_timestamp"] = start_timestamp
                X_df["split_timestamp"] = split_timestamp
                X_df["end_timestamp"] = end_timestamp
                X_df_name = "%s_%s" % (wavelet, t)
                if X_df_name not in memory:
                    memory[X_df_name] = X_df
                else:
                    memory[X_df_name] = memory[X_df_name].append(X_df, ignore_index=True)
            for target in targets:
                Y, Y_titles = features.generate_targets(Y_series, expected_range, [target])
                Y_df = pandas.DataFrame([Y], columns=Y_titles, dtype=numpy.float64)
                Y_df["start_timestamp"] = start_timestamp
                Y_df["split_timestamp"] = split_timestamp
                Y_df["end_timestamp"] = end_timestamp
                Y_df_name = "%s_%s" % (target, t)
                if Y_df_name not in memory:
                    memory[Y_df_name] = Y_df
                else:
                    memory[Y_df_name] = memory[Y_df_name].append(Y_df, ignore_index=True)
        sample_count += 1
        print("(%s, %s, %s, %d)" % (start_date, split_date, end_date, code), file=sys.stderr)
    if start_date <= dates[0]:
        break
    date_count += 1

input_hdf.close()

output_hdf = pandas.HDFStore(os.path.join(options.directory, experiment_folder, "samples.h5"))

for table in memory:
    memory[table] = memory[table].reset_index(drop=True)
    output_hdf.put(table, memory[table], format="table", data_columns=True)

output_hdf.close()

print("%d samples added to %s." % (sample_count, os.path.join(options.directory, experiment_folder, "samples.h5")), file=sys.stderr)

