#!/usr/bin/env python

import sys
import os
from datetime import datetime
from optparse import OptionParser
import configparser
import queue

from data_handlers import HistoricalBarDataHandler, HistoricalTickDataHandler, LiveBarDataHandler, LiveTickDataHandler
from strategies import Strategy
from execution_handlers import BarExecutionHandler, TickExecutionHandler, LiveExecutionHandler

parser = OptionParser()
parser.add_option("--backtest", dest="backtest", type="bool", help="Backtesting", default=False)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

# Declare the components with respective parameters
# Logic:
#  - data_handler => BAR, TICK
#  - TICK => strategy => SIGNAL
#  - SIGNAL => portfolio => ORDER
#  - ORDER => broker => ACK
#  - ...

# FIXME

events = Queue()

if options.backtest:
    data_handler = HistoricalBarDataHandler()
else:
    data_handler = LiveBarDataHandler()

strategy = Strategy()

portfolio = Portfolio()

execution_handler = BarExecutionHandler()

# Loop

while True:

    # Must do: get new ticks
    # The effect is to populate the new bar/tick into events
    data_handler.update()

    # Distribute events to handlers accordingly
    # Warning: Slow
    while True:
        try:
            evt = events.get(False)
        except Queue.Empty:
            break
        if evt is not None:
            portfolio.update_clock(evt)
            if evt.TYPE == "TICK":
                strategy.update_price(evt)
                execution_handler.update_price(evt)
            elif evt.TYPE == "BAR":
                execution_handler.update_bar(evt)
            elif evt.TYPE == "SIGNAL":
                portfolio.update_signal(evt)
            elif evt.TYPE == "ORDER":
                if evt.ORDER_TYPE == "SUBMIT":
                    execution_handler.submit_order(evt)
                elif evt.ORDER_TYPE == "CANCEL":
                    execution_handler.cancel_order(evt)
                elif evt.ORDER_TYPE == "AMEND":
                    execution_handler.amend_order(evt)
            elif evt.TYPE == "ACK":
                elif evt.ACK_TYPE in ("SUBMITTED", "CANCELLED", "AMENDED"):
                    portfolio.update_order_status(evt)
                elif evt.ACK_TYPE in ("PARTIAL_FILLED", "FILLED"):
                    portfolio.update_fill(evt)
    # FIXME - Heartbeat Here

