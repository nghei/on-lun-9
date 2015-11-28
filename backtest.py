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
parser.add_option("--start_date", dest="start_date", help="Start Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--end_date", dest="end_date", help="End Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Constants

EQUITY = 1000000

# Utility Functions

def read_all_bars(experiment):
    pass

def add_days(dates, end_date, n):
    pass

def load_model(experiment, date):
    pass

# Preparation

codes = None

bars = None

start_date = None
end_date = None

timestamps = bars["timestamp"].values

models = {}

look_backward_days = None

expt_features = None
expt_targets = None

#####

atr_window = None
risk = None
stop_loss_ratio = None

slippage_factor = None

#####

# Run

executor = backtest_logic.Executor(slippage_factor)
portfolio = backtest_logic.Portfolio(EQUITY)
strategy = strategies.TrailingStopStrategy(portfolio, codes, look_backward_days, expt_features, risk, atr_window, stop_loss_ratio)

for d in dates:
    model_date = add_days(...)
    if model_date is not None:
        # Start of Day
        for code in codes:
            px_open = None
            executor.feed_open(d, code, px_open)
            portfolio.feed_open(d, code, px_open)
            # Execute Stop Losses
            position = portfolio.get_position(code)
            stop_loss_price = strategy.get_stop_loss_price(code)
            if position != 0:
                filled_size, filled_price = executor.execute_stop_at_open(code, -position, stop_loss_price)
                if filled_size != 0:
                    portfolio.feed_fill(code, filled_size, filled_price)
            # Strategy Generates Signals, if any
            strategy.update_model(models[model_date])
            position = strategy.get_signal_at_open()  #
            if position != 0:
                filled_size, filled_price = executor.execute_at_open(code, position)
                if filled_size != 0:
                    portfolio.feed_fill(code, filled_size, filled_price)
                    strategy.notify_fill(code)  #
        # Continuous Trading
        # Replaced by Daily Bar plus assumptions, for speed
        for code in codes:
            executor.feed_bar(d, px_open, px_high, px_low, px_last)
            position = portfolio.get_position(code)
            stop_loss_price = strategy.get_stop_loss_price(code)
            if position != 0:
                filled_size, filled_price = executor.execute_stop_at_day(code, -position, stop_loss_price)
                portfolio.feed_fill(code, filled_size, filled_price)
                strategy.notify_fill(code)
    # End of Day
    for code in codes:
        portfolio.feed_bar(code, d, px_open, px_high, px_low, px_last)
        strategy.update_atr_stop_loss_at_close()
    portfolio.update_equity_curve_at_close()
    if portfolio.equity + portfolio.worth <= 0:
        break  # Broke

if portfolio.trades is not None:
    portfolio.trades.to_csv(...)
if portfolio.equity_curve is not None:
    portfolio.equity_curve.to_csv(...)

