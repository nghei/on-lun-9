#!/bin/env python3

# Note: Hardcoded information present

import sys
import re
from datetime import datetime
import requests
import eventlet
import urllib.parse
from lxml import etree
import lxml.html
import json

# Utility Functions

def parse_float(s):
    m = re.match("(?P<figure>[0-9]+([.][0-9]+|))\s*(?P<unit>K|M|B|)", s.upper())
    figure = float(m.group("figure"))
    unit = m.group("unit")
    if unit == "B":
        figure *= 1e9
    elif unit == "M":
        figure *= 1e6
    elif unit == "K":
        figure *= 1e3
    return figure

def get_realtime_data(code, index=0, proxy=None, timeout=5):
    index %= 3
    if not proxy:
        proxies = None
    elif proxy[0:5] == "https":
        proxies = { "https" : proxy }
    else:
        proxies = { "http" : proxy }
    if index == 0:
        # http://money18.on.cc/js/real/hk/quote/00001_r.js
        params = { "stockcode" : code }
        headers = { "Referer" : "http://money18.on.cc/" }
        try:
            eventlet.monkey_patch(all=False, os=False, select=False, socket=True, thread=False, time=False)
            with eventlet.Timeout(timeout):
                req = requests.get("http://money18.on.cc/securityQuote/genStockXML.php", params=params, headers=headers, proxies=proxies, timeout=timeout)
        except:
            return None
        if not req or req.status_code != requests.codes.ok:
            return None
        try:
            xml = req.text.encode("utf-8")
            quote = etree.fromstring(xml)
            opens = quote.xpath("/quote/stock/open")
            highs = quote.xpath("/quote/stock/high")
            lows = quote.xpath("/quote/stock/low")
            prices = quote.xpath("/quote/stock/price")
            volumes = quote.xpath("/quote/stock/volume")
            return (datetime.now().timestamp(), float(prices[0].text if opens[0].text in ["null"] else opens[0].text), float(prices[0].text if highs[0].text in ["null"] else highs[0].text), float(prices[0].text if lows[0].text in ["null"] else lows[0].text), float(prices[0].text), float(volumes[0].text))
        except:
            return None
    elif index == 1:
        try:
            eventlet.monkey_patch(all=False, os=False, select=False, socket=True, thread=False, time=False)
            with eventlet.Timeout(timeout):
                req = requests.get("http://qt.gtimg.cn/q=r_hk%05d" % code, proxies=proxies, timeout=timeout)
        except:
            return None
        if not req or req.status_code != requests.codes.ok:
            return None
        try:
            tokens = req.text.split("~")
            return (datetime.now().timestamp(), float(tokens[5]), float(tokens[33]), float(tokens[34]), float(tokens[3]), float(tokens[36]))
        except:
            return None
    elif index == 2:
        params = { "list" : "rt_hk%05d" % code }
        headers = { "Referer" : "http://stock.finance.sina.com.cn/hkstock/quotes/%05d.html" % code }
        try:
            eventlet.monkey_patch(all=False, os=False, select=False, socket=True, thread=False, time=False)
            with eventlet.Timeout(timeout):
                req = requests.get("http://hq.sinajs.cn/", params=params, headers=headers, proxies=proxies, timeout=timeout)
        except:
            return None
        if not req or req.status_code != requests.codes.ok:
            return None
        try:
            tokens = req.text.split(",")
            return (datetime.now().timestamp(), float(tokens[2]), float(tokens[4]), float(tokens[5]), float(tokens[6]), float(tokens[12]))
        except:
            return None
    else:
        return None

def get_realtime_bid_ask(code, index=0, proxy=None, timeout=5):
    index %= 2
    if not proxy:
        proxies = None
    elif proxy[0:5] == "https":
        proxies = { "https" : proxy }
    else:
        proxies = { "http" : proxy }
    if index == 0:
        params = { "list" : "rt_hk%05d" % code }
        headers = { "Referer" : "http://stock.finance.sina.com.cn/hkstock/quotes/%05d.html" % code }
        try:
            eventlet.monkey_patch(all=False, os=False, select=False, socket=True, thread=False, time=False)
            with eventlet.Timeout(timeout):
                req = requests.get("http://hq.sinajs.cn/", params=params, headers=headers, proxies=proxies, timeout=timeout)
        except:
            return None
        if not req or req.status_code != requests.codes.ok:
            return None
        try:
            tokens = req.text.split(",")
            return (datetime.now().timestamp(), float(tokens[9]), float(tokens[10]))
        except:
            return None
    else:
        return None

