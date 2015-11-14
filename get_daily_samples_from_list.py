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

import bars, xs

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--symbols", dest="symbols_file", help="Name of Symbols File", default=None)

(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# 

eligible_codes = []
eligible_pairs = []

with open(options.symbols_file, 'r') as f:
    all_lines = f.read()
lines = all_lines.split("\n")
for line in lines:
    if '|' in line:
        try:
            tokens = line.split("|")
            code1 = int(tokens[0])
            code2 = int(tokens[1])
            eligible_pairs += [(code1, code2)]
        except:
            pass
    else:
        try:
            code = int(line)
            eligible_codes += [code]
        except:
            pass

prices_folder = configParser.get("Main", "prices_folder")

dates = sorted(os.listdir(os.path.join(options.directory, prices_folder)))
dates = [d for d in dates if d <= options.date]

all_dates = dates

min_turnover_days = int(configParser.get("Sample", "min_turnover_days"))

dates = dates[-min_turnover_days:]

foos = {}

for d in dates:
    foo = bars.read_all_bars(os.path.join(options.directory, prices_folder, d))
    for code in foo:
        if code not in eligible_codes:
            continue
        if code not in foos:
            foos[code] = []
        foos[code] += foo[code]

# Finalized list here

print("Finalized List -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

print("Eligible Pairs -", file=sys.stderr)
print(eligible_pairs, file=sys.stderr)

# Clean Auction and Market Open Ticks
# Fill back morning/afternoon Ticks

auction_time = datetime.strptime(bars.market_auction, "%H:%M:%S%z")
market_am_open_time = datetime.strptime(bars.market_am_open, "%H:%M:%S%z")
market_pm_open_time = datetime.strptime(bars.market_pm_open, "%H:%M:%S%z")
market_am_close_time = datetime.strptime(bars.market_am_close, "%H:%M:%S%z")
market_pm_close_time = datetime.strptime(bars.market_pm_close, "%H:%M:%S%z")

for code in eligible_codes:
    toDelete = []
    for i in range(0, len(foos[code])):
        ts = datetime.fromtimestamp(foos[code][i].timestamp)
        if (ts.hour == auction_time.hour and ts.minute == auction_time.minute) or (ts.hour == market_am_open_time.hour and ts.minute == market_am_open_time.minute) or (ts.hour == market_pm_open_time.hour and ts.minute == market_pm_open_time.minute):
            toDelete += [i]
    for i in sorted(toDelete, reverse=True):
        del foos[code][i]
    patched = []
    for i in range(0, len(foos[code])):
        ts = datetime.fromtimestamp(foos[code][i].timestamp)
        last_ts = datetime.fromtimestamp(foos[code][i-1].timestamp) if i > 0 else None
        next_ts = datetime.fromtimestamp(foos[code][i+1].timestamp) if i < len(foos[code]) - 1 else None
        if (ts.hour == market_pm_open_time.hour and ts.minute == market_pm_open_time.minute + 1):
            last_px = None
            if last_ts is None:
                last_px = foos[code][i].px_open
            elif (last_ts.hour == market_pm_close_time.hour and last_ts.minute == market_pm_close_time.minute):
                last_px = foos[code][i-1].px_last
            if last_px is not None:
                tmp_ts = datetime.combine(ts.date(), market_am_open_time.time()).timestamp()
                while tmp_ts % 86400 < market_am_close_time.timestamp() % 86400:
                    tmp_ts += 60
                    patched += [bars.Bar(timestamp=tmp_ts, px_open=last_px, px_high=last_px, px_low=last_px, px_last=last_px, px_volume=0)]
        patched += [foos[code][i]]
        if (ts.hour == market_am_close_time.hour and ts.minute == market_am_close_time.minute):
            if next_ts is None or (next_ts.hour == market_am_open_time.hour and next_ts.minute == market_am_open_time.minute + 1):
                last_px = foos[code][i].px_last
                tmp_ts = ts.timestamp() + (market_pm_open_time - market_am_close_time).seconds
                while tmp_ts % 86400 < market_pm_close_time.timestamp() % 86400:
                    tmp_ts += 60
                    patched += [bars.Bar(timestamp=tmp_ts, px_open=last_px, px_high=last_px, px_low=last_px, px_last=last_px, px_volume=0)]
    foos[code] = patched

# Write Output

daily_samples_folder = configParser.get("Sample", "daily_samples_folder")

try:
    os.makedirs(os.path.join(options.directory, daily_samples_folder))
except OSError:
    pass

hdf = pandas.HDFStore(os.path.join(options.directory, daily_samples_folder, "%s.h5" % options.date))

raw_df = pandas.DataFrame(columns=tuple(['code'] + list(bars.Bar._fields)), dtype=numpy.float64)
for code in eligible_codes:
    tmp_df = pandas.DataFrame(foos[code], columns=bars.Bar._fields)
    tmp_avg = (tmp_df['px_open'] + tmp_df['px_high'] + tmp_df['px_low'] + tmp_df['px_last']) / 4
    tmp_df['px_volume'] = tmp_df['px_volume'] * tmp_avg
    p = tmp_df['px_open'][0]
    tmp_df['px_open'] = tmp_df['px_open'] / p
    tmp_df['px_high'] = tmp_df['px_high'] / p
    tmp_df['px_low'] = tmp_df['px_low'] / p
    tmp_df['px_last'] = tmp_df['px_last'] / p
    tmp_df['code'] = pandas.Series(numpy.ones(tmp_df.shape[0]) * code)
    raw_df = raw_df.append(tmp_df, ignore_index=True)
for pair in eligible_pairs:
    tmp_df1 = pandas.DataFrame(foos[pair[0]], columns=bars.Bar._fields)
    tmp_avg1 = (tmp_df1['px_open'] + tmp_df1['px_high'] + tmp_df1['px_low'] + tmp_df1['px_last']) / 4
    tmp_df1['px_volume'] = tmp_df1['px_volume'] * tmp_avg1
    tmp_df2 = pandas.DataFrame(foos[pair[1]], columns=bars.Bar._fields)
    tmp_avg2 = (tmp_df2['px_open'] + tmp_df2['px_high'] + tmp_df2['px_low'] + tmp_df2['px_last']) / 4
    tmp_df2['px_volume'] = tmp_df2['px_volume'] * tmp_avg2
    p1 = tmp_df1['px_open'][0]
    p2 = tmp_df2['px_open'][0]
    tmp_df1['px_open'] = tmp_df1['px_open'] / p1 / 2 + tmp_df2['px_open'] / p2 / 2
    tmp_df1['px_high'] = tmp_df1['px_high'] / p1 / 2 + tmp_df2['px_high'] / p2 / 2
    tmp_df1['px_low'] = tmp_df1['px_low'] / p1 / 2 + tmp_df2['px_low'] / p2 / 2
    tmp_df1['px_last'] = tmp_df1['px_last'] / p1 / 2 + tmp_df2['px_last'] / p2 / 2
    tmp_df1['code'] = pandas.Series(numpy.ones(tmp_df1.shape[0]) * (pair[0] * 100000 + pair[1]))
    raw_df = raw_df.append(tmp_df1, ignore_index=True)
hdf.put('raw', raw_df, format='table', data_columns=True)

hdf.close()

print("Daily sample saved to %s." % os.path.join(options.directory, daily_samples_folder, "%s.h5" % options.date), file=sys.stderr)

