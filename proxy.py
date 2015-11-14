#!/bin/env python3

import sys
import re
from datetime import datetime
from collections import deque
from concurrent.futures import *
import requests
import lxml.html
import html.parser
import urllib.parse

import realtime

# Utility Functions

def crawl(urls, links=100):
    res = set()
    crawled = set()
    q = deque(urls)
    parser = html.parser.HTMLParser()
    while len(q) > 0 and len(crawled) < links:
        url = q.popleft()
        if url in crawled:
            continue
        try:
            req = requests.get(url)
        except:
            continue
        if req.status_code != requests.codes.ok:
            continue
        print(url, file=sys.stderr)
        req.encoding = 'utf-8'
        contents = parser.unescape(req.text)
        newLinks = page_links(contents)
        for l in newLinks:
            q.extend([l])
        newProxies = page_leech(contents)
        for p in newProxies:
            res.add(p)
        crawled.add(url)
    return list(res)

def page_leech(contents):
    res = set()
    matches = re.findall("<td>(?P<host>[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+)</td><td>(?P<port>[0-9]+)</td>", contents)
    if matches:
        for match in matches:
            res.add("http://%s:%s" % match)
    matches = re.findall("(?P<host>[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+):(?P<port>[0-9]+)", contents)
    if matches:
        for match in matches:
            res.add("http://%s:%s" % match)
    return res

def page_links(contents):
    res = set()
    contents = contents.encode('utf-8')
    root = lxml.html.fromstring(contents)
    hrefs = root.xpath("//a[@href]")
    if hrefs:
        for href in hrefs:
            link = href.attrib["href"]
            if "blogspot" in urllib.parse.urlparse(link).netloc:
                res.add(link)
    return res

def check_proxy(proxy, tries=16, timeout=5):
    for i in range(0, tries):
        res = realtime.get_realtime_data(1, index=i, proxy=proxy, timeout=timeout)
        if not res:
            return None
    return proxy

def check_proxies(proxies, workers=50):
    res = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        try:
            fs = [ executor.submit(check_proxy, proxy) for proxy in proxies ]
            for f in as_completed(fs):
                r = f.result()
                if r:
                    print(r, file=sys.stderr)
                    res.append(r)
        except KeyboardInterrupt:
           print("KeyboardInterrupt Detected. Stopping Proxy Checks ...", file=sys.stderr)
        finally:
            return res

