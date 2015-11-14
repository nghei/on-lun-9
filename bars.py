#!/bin/env python3

import sys
import os
import math
from datetime import datetime
from collections import namedtuple
from enum import Enum
import numpy

# Constants

market_auction = "09:20:00+0800"
market_am_open = "09:30:00+0800"
market_am_close = "12:00:00+0800"
market_pm_open = "13:00:00+0800"
market_pm_close = "16:00:00+0800"

market_open_duration = 19800

# Data Structures

Bar = namedtuple("Bar", ["timestamp", "px_open", "px_high", "px_low", "px_last", "px_volume"])
Tick = namedtuple("Tick", ["timestamp", "px_last", "px_volume"])
CorporateAction = namedtuple("CorporateAction", ["timestamp", "action_type", "rights_ratio", "rights_price", "dividend_amount", "exchange_rate"])

# Utility Functions

# Time Calculations

market_am_open_ts = datetime.strptime(market_am_open, "%H:%M:%S%z").timestamp() % 86400
market_am_close_ts = datetime.strptime(market_am_close, "%H:%M:%S%z").timestamp() % 86400
market_pm_open_ts = datetime.strptime(market_pm_open, "%H:%M:%S%z").timestamp() % 86400
market_pm_close_ts = datetime.strptime(market_pm_close, "%H:%M:%S%z").timestamp() % 86400

class Timeframe(Enum):
    ONE_MINUTE = 60
    FIVE_MINUTE = 300
    FIFTEEN_MINUTE = 900
    THIRTY_MINUTE = 1800
    HOURLY = 3600    #
    AM_PM = "am-pm"  #
    DAILY = market_open_duration

def time_since_open(t):
    ts = t.timestamp() % 86400
    if ts >= market_pm_open_ts:
        ts -= (market_pm_open_ts - market_am_close_ts)
    ts -= market_am_open_ts
    return ts

def is_am(t):
    ts = t.timestamp() % 86400
    return (ts >= market_am_open_ts and ts < market_am_close_ts)

def diff_bars(t1, t2, timeframe=Timeframe.ONE_MINUTE):
    # Best-efforts: if (t1, t2) belong to different days, treat as consecutive days.
    # Change to datetime
    if type(t1) is float:
        t1 = datetime.fromtimestamp(t1)
    if type(t2) is float:
        t2 = datetime.fromtimestamp(t2)
    # Ensure t1 < t2
    if t1 > t2:
        tmp = t1
        t1 = t2
        t2 = tmp
        neg = True
    else:
         neg = False
    d1 = t1.date()
    d2 = t2.date()
    if d1 != d2:
        return True
    if timeframe == Timeframe.DAILY:
        return False
    elif timeframe == Timeframe.AM_PM:
        return is_am(t1) != is_am(t2)
    else:
        add_ts = (1e-6 if t1.timestamp() % 86400 in [market_am_open_ts, market_pm_open_ts] else 0)
        return (t1.timestamp() - 1e-6 + add_ts) // timeframe.value != (t2.timestamp() - 1e-6) // timeframe.value

# Corporate Actions

def read_corporate_actions(filename, code):
    code = int(code)
    corporate_actions = []
    with open(filename, 'r') as f:
        all_linea = f.read()
    lines = all_lines.split("\n")
    for line in lines:
        try:
            (px_code, announcement_timestamp, timestamp, action_type, rights_ratio, rights_price, dividend_amount, dividend_currency, exchange_rate) = line.split(",")
            px_code = int(px_code)
            if px_code != code:
                continue
            timestamp = float(timestamp)
            rights_ratio = float(rights_ratio)
            rights_price = float(rights_price)
            dividend_amount = float(dividend_amount)
            exchange_rate = float(exchange_rate)
            corporate_actions += [CorporateAction(timestamp=timestamp, action_type=action_type, rights_ratio=rights_ratio, rights_price=rights_price, dividend_amount=dividend_amount, exchange_rate=exchange_rate)]
        except ValueError:
            continue
    return sorted(corporate_actions, key=lambda x: x.timestamp)

# Bars

def read_bars(filename, code):
    code = int(code)
    bars = []
    with open(filename, 'r') as f:
        all_lines = f.read()
    lines = all_lines.split("\n")
    for line in lines:
        try:
            (px_code, timestamp, px_open, px_high, px_low, px_last, px_volume) = line.split(",")
            px_code = int(px_code)
            if px_code != code:
                continue
            timestamp = float(timestamp)
            px_open = float(px_open)
            px_high = float(px_high)
            px_low = float(px_low)
            px_last = float(px_last)
            px_volume = float(px_volume)
            bars += [Bar(timestamp=timestamp, px_open=px_open, px_high=px_high, px_low=px_low, px_last=px_last, px_volume=px_volume)]
        except ValueError:
            continue
    return sorted(bars, key=lambda x: x.timestamp)

def read_all_bars(filename):
    bars = {}
    with open(filename, 'r') as f:
        all_lines = f.read()
    lines = all_lines.split("\n")
    for line in lines:
        try:
            (px_code, timestamp, px_open, px_high, px_low, px_last, px_volume) = line.split(",")
            px_code = int(px_code)
            if px_code not in bars:
                bars[px_code] = []
            timestamp = float(timestamp)
            px_open = float(px_open)
            px_high = float(px_high)
            px_low = float(px_low)
            px_last = float(px_last)
            px_volume = float(px_volume)
            bars[px_code] += [Bar(timestamp=timestamp, px_open=px_open, px_high=px_high, px_low=px_low, px_last=px_last, px_volume=px_volume)]
        except ValueError:
            continue
    for code in bars:
        bars[code] = sorted(bars[code], key=lambda x: x.timestamp)
    return bars

def adjust_bars(bars, corporate_actions=None):
    adjusted_bars = list(bars)
    if not corporate_actions:
        return adjusted_bars
    for corporate_action in corporate_actions[::-1]:
        if corporate_action.action_type in ['D', 'SD']:
            div_amount = corporate_action.dividend_amount * corporate_action.exchange_rate
            if math.isnan(div_amount):
                continue
            for i in range(0, len(adjusted_bars)):
                if adjusted_bars[i].timestamp < corporate_action.timestamp:
                    new_bar = Bar(timestamp=adjusted_bars[i].timestamp, px_open=adjusted_bars[i].px_open-div_amount, px_high=adjusted_bars[i].px_high-div_amount, px_low=adjusted_bars[i].px_low-div_amount, px_last=adjusted_bars[i].px_last-div_amount, px_volume=adjusted_bars[i].px_volume)
                    adjusted_bars[i] = new_bar
        elif corporate_action.action_type in ['B', 'C', 'S', 'R']:
            if math.isnan(corporate_action.rights_ratio) or math.isnan(corporate_action.rights_price):
                continue
            paf = None
            for i in range(0, len(adjusted_bars))[::-1]:
                if adjusted_bars[i].timestamp < corporate_action.timestamp:
                    if not paf:
                        paf = (adjusted_bars[i].px_last + (corporate_action.rights_ratio - 1) * (corporate_action.rights_price)) / (corporate_action.rights_ratio * adjusted_bars[i].px_last)
                    new_bar = Bar(timestamp=adjusted_bars[i].timestamp, px_open=adjusted_bars[i].px_open*paf, px_high=adjusted_bars[i].px_high*paf, px_low=adjusted_bars[i].px_low*paf, px_last=adjusted_bars[i].px_last*paf, px_volume=adjusted_bars[i].px_volume/paf)
                    adjusted_bars[i] = new_bar
    return adjusted_bars

def join_bars(bars):
    start_timestamp = (1 << 31)
    timestamp = -(1 << 31)
    px_open = None
    px_high = 0
    px_low = 1e9
    px_last = None
    px_volume = 0
    for bar in bars:
        if bar.timestamp < start_timestamp:
            start_timestamp = bar.timestamp
            px_open = bar.px_open
        if bar.timestamp > timestamp:
            timestamp = bar.timestamp
            px_last = bar.px_last
        if bar.px_high > px_high:
            px_high = bar.px_high
        if bar.px_low < px_low:
            px_low = bar.px_low
        px_volume += bar.px_volume
    return Bar(timestamp=timestamp, px_open=px_open, px_high=px_high, px_low=px_low, px_last=px_last, px_volume=px_volume)

def group_bars(bars=[], timeframe=Timeframe.ONE_MINUTE):
    grouped_bars = []
    foo = []
    for bar in bars[::-1]:
        if len(foo) > 0:
            if diff_bars(bar.timestamp, foo[-1].timestamp, timeframe=timeframe):
                joined_bar = join_bars(foo)
                grouped_bars = [joined_bar] + grouped_bars
                foo = []
        foo = [bar] + foo
    if len(foo) > 0:
        joined_bar = join_bars(foo)
        grouped_bars = [joined_bar] + grouped_bars
    return grouped_bars

# Ticks to Bars

def read_ticks(filename, code):
    code = int(code)
    ticks = []
    with open(filename, 'r') as f:
        all_lines = f.read()
    lines = all_lines.split("\n")
    for line in lines:
        try:
            (px_code, timestamp, px_last, px_volume) = line.split(",")
            px_code = int(px_code)
            if px_code != code:
                continue
            timestamp = float(timestamp)
            px_last = float(px_last)
            px_volume = float(px_volume)
            ticks += [Tick(timestamp=timestamp, px_last=px_last, px_volume=px_volume)]
        except ValueError:
            continue
    return sorted(ticks, key=lambda x: x.timestamp)

def adjust_ticks(ticks=[], corporate_actions=None):
    adjusted_ticks = list(ticks)
    if not corporate_actions:
        return adjusted_ticks
    for corporate_action in corporate_actions[::-1]:
        if corporate_action.action_type in ['D', 'SD']:
            div_amount = corporate_action.dividend_amount * corporate_action.exchange_rate
            if math.isnan(div_amount):
                continue
            for i in range(0, len(adjusted_ticks)):
                if adjusted_ticks[i].timestamp < corporate_action.timestamp:
                    new_tick = Tick(timestamp=adjusted_ticks[i].timestamp, px_last=adjusted_ticks[i].px_last-div_amount, px_volume=adjusted_ticks[i].px_volume)
                    adjusted_ticks[i] = new_tick
        elif corporate_action.action_type in ['B', 'C', 'S', 'R']:
            if math.isnan(corporate_action.rights_ratio) or math.isnan(corporate_action.rights_price):
                continue
            paf = None
            for i in range(0, len(adjusted_ticks))[::-1]:
                if adjusted_ticks[i].timestamp < corporate_action.timestamp:
                    if not paf:
                        paf = (adjusted_ticks[i].px_last + (corporate_action.rights_ratio - 1) * (corporate_action.rights_price)) / (corporate_action.rights_ratio * adjusted_ticks[i].px_last)
                    new_tick = Tick(timestamp=adjusted_ticks[i].timestamp, px_last=adjusted_ticks[i].px_last*paf, px_volume=adjusted_ticks[i].px_volume/paf)
                    adjusted_ticks[i] = new_tick
    return adjusted_ticks

def add_tick(bar=None, tick=None):
    if not tick:
        return bar
    if not bar:
        return Bar(timestamp=tick.timestamp, px_open=tick.px_last, px_high=tick.px_last, px_low=tick.px_last, px_last=tick.px_last, px_volume=tick.px_volume)
    else:
        return Bar(timestamp=tick.timestamp, px_open=bar.px_open, px_high=max(bar.px_high, tick.px_last), px_low=min(bar.px_low, tick.px_last), px_last=tick.px_last, px_volume=(bar.px_volume + tick.px_volume))

def join_ticks(ticks):
    start_timestamp = (1 << 31)
    timestamp = -(1 << 31)
    px_open = None
    px_high = 0
    px_low = 1e9
    px_last = None
    px_volume = 0
    for tick in ticks:
        if tick.timestamp < start_timestamp:
            start_timestamp = tick.timestamp
            px_open = tick.px_last
        if tick.timestamp > timestamp:
            timestamp = tick.timestamp
            px_last = tick.px_last
        if tick.px_last > px_high:
            px_high = tick.px_last
        if tick.px_ladt < px_low:
            px_low = tick.px_last
        px_volume += tick.px_volume
    return Bar(timestamp=timestamp, px_open=px_open, px_high=px_high, px_low=px_low, px_last=px_last, px_volume=px_volume)

def group_ticks(ticks=[], timeframe=Timeframe.ONE_MINUTE):
    grouped_ticks = []
    foo = []
    for tick in ticks[::-1]:
        pass
    return grouped_ticks

# DataFrame operations

def flatten(df, timeframe=Timeframe.ONE_MINUTE):
    if diff_bars(bar.timestamp, foo[-1].timestamp, timeframe=timeframe):
        pass

