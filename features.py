#!/bin/env python3

# Note: Hardcoded information present

import sys
import datetime as dtime
from datetime import datetime
from collections import namedtuple
import re
import math
import statsmodels.robust
import numpy
import pandas
import pywt

import bars
import xs
import transform

# Constants

market_am_open_time = datetime.strptime(bars.market_am_open, "%H:%M:%S%z")
market_am_close_time = datetime.strptime(bars.market_am_close, "%H:%M:%S%z")
market_pm_open_time = datetime.strptime(bars.market_pm_open, "%H:%M:%S%z")
market_pm_close_time = datetime.strptime(bars.market_pm_close, "%H:%M:%S%z")

market_open_minutes = (market_am_close_time - market_am_open_time).seconds // 60 + (market_pm_close_time - market_pm_open_time).seconds // 60

market_open_days = 252

# Data Structures

# Utility Functions

def split_series(series, split_timestamp):
    independent_series = series[series['timestamp'] <= split_timestamp]['px_open']
    realized_series = series[series['timestamp'] < split_timestamp]
    expected_range = numpy.sqrt(xs.get_volatility_ohlc(realized_series['px_open'], realized_series['px_high'], realized_series['px_low'], realized_series['px_last']) * market_open_minutes * 4 * numpy.log(2))
    dependent_series = series[series['timestamp'] >= split_timestamp].reset_index(drop=True)  #
    
    return (independent_series, expected_range, dependent_series)

# Wavelet features

def generate_wavelet_features(series, wavelet, detail, scaling):
    coefs = transform.wavelet_transform(series, wavelet)
    res = numpy.array([])
    for level in range(1, len(coefs)):
        res = numpy.append(res, coefs[level][-detail:] * (scaling ** (level - 1)))
    return res

# Dependent variables and helper functions

target_re = re.compile("([A-Za-z0-9_]+)\(([0-9.]+)\)")

def generate_targets(series, expected_range, target_list):
    requests = []
    for target in target_list:
        function_name = None
        args = []
        tmp_target = target
        while True:
            m = target_re.match(tmp_target)
            if m is None:
                break
            if function_name is None:
                function_name = m.group(1)
            args += [float(m.group(2))]
            tmp_target = tmp_target.replace(m.group(2), "", 1)
        requests += [(target, function_name, args)]
    res = numpy.array([])
    res_names = []
    for (target, function_name, args) in requests:
        if function_name == "trailing_stop_trade":
            func = generate_trailing_stop_trade
        ret = func(series, expected_range, *args)
        res = numpy.append(res, ret)
        res_names += ["%s_%d" % (target, i + 1) for i in range(0, len(ret))]
    return (res, res_names)

# args:
# [Stop-loss ratio]
# return:
# [Long trade return, Short trade return]
def generate_trailing_stop_trade(series, expected_range, *args):
    if args:
        stop_loss_ratio = args[0]
        stop_loss_return = numpy.exp(expected_range * stop_loss_ratio)
        n = series.shape[0]
        long_stop = series['px_open'][0]
        long_stopped = False
        short_stop = series['px_open'][0]
        short_stopped = False
        for i in range(0, n):
            if long_stopped and short_stopped:
                break
            else:
                if not long_stopped:
                    if series['px_low'][i] < long_stop:
                        long_stop = series['px_low'][i]
                        long_stopped = True
                    elif series['px_low'][i] / stop_loss_return > long_stop:
                        long_stop = series['px_low'][i] / stop_loss_return
                if not short_stopped:
                    if series['px_high'][i] > short_stop:
                        short_stop = series['px_high'][i]
                        short_stopped = True
                    elif series['px_high'][i] * stop_loss_return < short_stop:
                        short_stop = series['px_high'][i] * stop_loss_return
        if not long_stopped:
            long_stop = series['px_last'][n-1]
        if not short_stopped:
            short_stop = series['px_last'][n-1]
        return numpy.log([long_stop / series['px_open'][0], short_stop / series['px_open'][0]])
    else:
        return numpy.zeros(2) * numpy.NAN

# args:
# [Stop-loss ratio]
# return:
# [Long trade return, Short trade return]
def generate_breakout_fade_trade(series, expected_range, *args):
    if args:
        stop_loss_ratio = args[0]
        stop_loss_return = numpy.exp(expected_range * stop_loss_ratio)
        n = series.shape[0]
        long_opened = False
        long_stopped = False
        short_opened = False
        short_stopped = False
        for i in range(0, n):
            if long_stopped and short_stopped:
                break
            pass
    else:
        return numpy.zeros(2) * numpy.NAN

# args:
# [Pullback ratio, Stop-loss ratio]
# return:
# [Long trade return, short trade return]
def generate_pullback_trade(series, expected_range, *args):
    if args:
        pullback_ratio = args[0]
        pullback_return = numpy.exp(expected_range * pullback_ratio)
        stop_loss_ratio = args[0]
        stop_loss_return = numpy.exp(expected_range * stop_loss_ratio)
        n = series.shape[0]
        for i in range(0, n):
            if long_stopped and short_stopped:
                break
            pass
    else:
        return numpy.zeros(2) * numpy.NAN

