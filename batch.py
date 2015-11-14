#!/bin/env python3

# Note: Hardcoded information present

import sys
import datetime as dtime
from datetime import datetime
from collections import namedtuple
import math
import re
import xlrd
import requests
from lxml import etree
import lxml.html
import urllib.parse
import json

# Data Structures

CommodityFutures = namedtuple("CommodityFutures", ["px_open", "px_high", "px_low", "px_last", "px_volume", "px_turnover", "px_settlement", "open_interest"])

BulkCommodity = namedtuple("BulkCommodity", ["px_last"])

CorporateAction = namedtuple("CorporateAction", ["ex_timestamp", "financial_year", "financial_period", "announcement_timestamp", "action_type", "amount", "currency", "dilution_ratio"])

Buyback = namedtuple("Buyback", ["px_high", "px_low", "px_volume", "px_value"])

ShortSelling = namedtuple("ShortSelling", ["am_short_volume", "am_short_value", "daily_short_volume", "daily_short_value"])

BrokerActivity = namedtuple("BrokerActivity", ["bid_volume", "bid_value", "bid_vwap", "ask_volume", "ask_value", "ask_vwap"])

Industry = namedtuple("Industry", ["industry", "sector"])

IssuedShares = namedtuple("IssuedShares", ["issued_shares", "issued_shares_h"])

InstitutionalShares = namedtuple("InstitutionalShares", ["institutional_holders", "institutional_shares", "three_month_new", "three_month_closed", "three_month_increased", "three_month_decreased"])

Ratings = namedtuple("Ratings", ["buy_analysts", "outperform_analysts", "hold_analysts", "underperform_analysts", "sell_analysts", "target_high", "target_low", "target_median", "target"])

Fundamentals = namedtuple("Fundamentals", ["financial_period_start", "financial_period_end", "reporting_date", "currency", "extraordinary_income", "eps", "diluted_eps", "dps", "sdps", "cash", "accounts_receivable", "inventory", "current_assets", "loans", "ppe", "intangibles", "associates", "noncurrent_assets", "total_assets", "accounts_payable", "bank_debt", "current_liabilities", "long_term_debt", "deposits", "total_liabilities", "minority_equity", "share_capital", "reserve", "total_equity", "interest_revenue", "interest_payout", "net_interest_revenue", "other_revenue", "revenue", "cogs", "gross_profit", "sales_expense", "general_expense", "credit_provisions", "depreciation", "operating_income", "associates_operating_income", "ebit", "interest_expense", "ebt", "tax", "minority_interest", "net_income", "dividend", "retained_earnings", "operating_cash_flow", "capex", "investing_cash_flow", "financing_cash_flow", "fx_translation"])

CompoundKey = namedtuple("CompoundKey", ["code", "financial_year", "financial_period"])

ForecastedFundamentals = namedtuple("ForecastedFundamentals", ["revenue_analysts", "revenue_high", "revenue_low", "revenue", "eps_analysts", "eps_high", "eps_low", "eps", "dps"])

# Utility Functions

def try_float(s):
    if not s:
        return float('nan')
    s = str(s)
    multiplier_map = { "K" : 1e3, "M" : 1e6, "B" : 1e9 }
    multiplier = 1
    for m in multiplier_map:
        if m in s:
            multiplier = multiplier_map[m]
            s = s.replace(m, "")
            break
    try:
        f = float(s)
        return f * multiplier
    except ValueError:
        return float('nan')

def try_int(s):
    if not s:
        return float('nan')
    try:
        s = str(s)
        f = int(float(s))
        return f
    except ValueError:
        return float('nan')

def try_int_0(s):
    if not s:
        return float('nan')
    try:
        f = int(float(s))
        return f
    except ValueError:
        return 0

# Intraday Data

def get_intraday_data(code):
    res = {}
    params = { "q" : "%04d" % code, "x" : "HKG", "i" : "60", "p" : "1d", "f" : "d,o,h,l,c" }
    req = requests.get("http://www.google.com/finance/getprices", params=params, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    lines = req.text.split("\n")
    headers = None
    start_timestamp = None
    for line in lines:
        m = re.match("[A-Z]", line)
        if m:
            if line[0:7] == "COLUMNS":
                (values, headers) = line.split("=")
                headers = headers.lower().split(",")
            else:
                continue
        else:
            try:
                tokens = line.split(",")
                if len(tokens) != 5:
                    continue
                for (header, token) in zip(headers, tokens):
                    if header == "date":
                        if token[0] == "a":
                            start_timestamp = int(token[1:])
                            current_timestamp = start_timestamp
                        else:
                            current_timestamp = start_timestamp + int(token) * 60
                        res[current_timestamp] = {}
                    else:
                        res[current_timestamp][header] = float(token)
                        res[current_timestamp]["volume"] = 0
            except ValueError:
                continue
    timestamps = sorted(res.keys())
    req = requests.get("http://data.gtimg.cn/flashdata/hk/minute/hk%05d.js" % code, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    lines = req.text.split("\\n\\\n")
    base_date = None
    cumulative_volume = 0
    for line in lines:
        m = re.match("date:(?P<dt>[0-9]+)", line)
        if m:
            base_date = m.group("dt")
        else:
            m = re.match("(?P<time>[0-9]{4}) (?P<px>[0-9.]+) (?P<volume>[0-9]+)", line)
            if m:
                timestamp = int(datetime.strptime(base_date + m.group("time") + "+0800", "%y%m%d%H%M%z").timestamp())
                if m.group("time") == "1559" or m.group("time") == "1159":
                    continue
                elif (m.group("time") >= "0930" and m.group("time") < "1159") or (m.group("time") >= "1300" and m.group("time") < "1559"):
                    timestamp += 60
                new_volume = float(m.group("volume"))
                current_volume = new_volume - cumulative_volume
                cumulative_volume = new_volume
                if timestamp not in timestamps:
                    res[timestamp] = {}  # Note that Google may be missing some timestamps
                    timestamp_index = -1
                    for i in range(0, len(timestamps)):
                        if timestamps[i] >= timestamp:
                            break
                        timestamp_index += 1
                    if timestamp_index < 0:
                        current_price = float(m.group("px"))
                        res[timestamp]["open"] = current_price
                        res[timestamp]["high"] = current_price
                        res[timestamp]["low"] = current_price
                        res[timestamp]["close"] = current_price
                    else:
                        res[timestamp]["open"] = res[timestamps[timestamp_index]]["close"]
                        res[timestamp]["high"] = res[timestamps[timestamp_index]]["close"]
                        res[timestamp]["low"] = res[timestamps[timestamp_index]]["close"]
                        res[timestamp]["close"] = res[timestamps[timestamp_index]]["close"]
                res[timestamp]["volume"] = current_volume
    return res

def get_intraday_data_china(code):
    pass

# Indices / Futures / Commodities

def get_intraday_data_index(code):
    req = requests.get("http://finance.yahoo.com/_td_charts_api/resource/charts;range=1d;ticker=%s" % code, timeout=60)
    if req.status_code != requests.codes.ok:
        if req.text == "Empty dataset":
            return {}
        raise Exception("Failed to Load: %s" % req.url)
    data = json.loads(req.text)
    timestamps = data["data"]["timestamp"]
    res = {}
    for (t, o, h, l, c, v) in zip(data["data"]["timestamp"], data["data"]["indicators"]["quote"][0]["open"], data["data"]["indicators"]["quote"][0]["high"], data["data"]["indicators"]["quote"][0]["low"], data["data"]["indicators"]["quote"][0]["close"], data["data"]["indicators"]["quote"][0]["volume"]):
        if o is None or h is None or l is None or c is None or v is None:
            continue
        res[t] = {}
        res[t]["open"] = o
        res[t]["high"] = h
        res[t]["low"] = l
        res[t]["close"] = c
        res[t]["volume"] = v
    return res

def get_china_commodity_futures_data(date):
    parsedDate = datetime.strptime(date, "%Y%m%d")
    res = {}
    # CZCE
    req = requests.get("http://www.czce.com.cn/portal/exchange/%d/datadaily/%s.txt" % (parsedDate.year, date), timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    recordRe = re.compile("(?P<code>[A-Za-z0-9]+),(?P<prev>[0-9.]+),(?P<open>[0-9.]+),(?P<high>[0-9.]+),(?P<low>[0-9.]+),(?P<last>[0-9.]+),(?P<settlement>[0-9.]+),[0-9.-]+,[0-9.-]+,(?P<volume>[0-9.]+),(?P<oi>[0-9.-]+),[0-9.-]+,(?P<turnover>[0-9.]+)")
    lines = req.text.split("\n")
    for line in lines:
        m = re.match(recordRe, line)
        if m:
            code = m.group("code")
            if code not in res:
                res[code] = {}
            res[code]["open"] = try_float(m.group("open"))
            res[code]["high"] = try_float(m.group("high"))
            res[code]["low"] = try_float(m.group("low"))
            res[code]["last"] = try_float(m.group("last"))
            res[code]["volume"] = try_int(m.group("volume"))
            res[code]["turnover"] = try_float(m.group("turnover"))
            res[code]["settlement"] = try_float(m.group("settlement"))
            res[code]["oi"] = try_int(m.group("oi"))
    # DCE
    params = { "action" : "Pu00012_download", "Pu00011_Input.trade_date" : date, "Pu00011_Input.variety" : "all", "Pu00011_Input.trade_type" : 0 }
    req = requests.get("http://www.dce.com.cn/PublicWeb/MainServlet", params=params, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    req.encoding = "GB2312"
    recordRe = re.compile("(?P<prefix>[^ ]+) +(?P<suffix>[0-9]{4}) +(?P<open>[0-9.-]+) +(?P<high>[0-9.-]+) +(?P<low>[0-9.-]+) +(?P<last>[0-9.-]+) +(?P<prev>[0-9.-]+) +(?P<settlement>[0-9.-]+) +[0-9.-]+ +[0-9.-]+ +(?P<volume>[0-9.-]+) +(?P<oi>[0-9.-]+) +[0-9.-]+ +(?P<turnover>[0-9.-]+)")
    lines = req.text.split("\r\n")
    for line in lines:
        m = re.match(recordRe, line)
        if m:
            code = m.group("prefix") + m.group("suffix")
            if code not in res:
                res[code] = {}
            res[code]["open"] = try_float(m.group("open"))
            res[code]["high"] = try_float(m.group("high"))
            res[code]["low"] = try_float(m.group("low"))
            res[code]["last"] = try_float(m.group("last"))
            res[code]["volume"] = try_int(m.group("volume"))
            res[code]["turnover"] = try_float(m.group("turnover"))
            res[code]["settlement"] = try_float(m.group("settlement"))
            res[code]["oi"] = try_int(m.group("oi"))
    # SHFE
    req = requests.get("http://www.shfe.com.cn/data/dailydata/kx/kx%s.dat" % date, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    recordRe = re.compile("(?P<prefix>[A-Za-z]+)_f")
    suffixRe = re.compile("(?P<suffix>[0-9]{4})")
    root = json.loads(req.text)
    try:
        for record in root["o_curinstrument"]:
            m = re.match(recordRe, record["PRODUCTID"])
            ms = re.match(suffixRe, record["DELIVERYMONTH"])
            if m and ms:
                code = m.group("prefix") + ms.group("suffix")
                if code not in res:
                    res[code] = {}
                res[code]["open"] = record["OPENPRICE"]
                res[code]["high"] = record["HIGHESTPRICE"]
                res[code]["low"] = record["LOWESTPRICE"]
                res[code]["last"] = record["CLOSEPRICE"]
                res[code]["volume"] = record["VOLUME"]
                res[code]["turnover"] = res[code]["last"] * res[code]["volume"]
                res[code]["settlement"] = record["SETTLEMENTPRICE"]
                res[code]["oi"] = record["OPENINTEREST"]
    except:
        raise Exception("Failed to Load: %s" % req.url)
    return res

def get_china_bulk_commodities_data(date, timeout=60):
    ref_date = datetime.strptime(date + "2359+0800", "%Y%m%d%H%M%z")
    res = {}
    # sci99
    site = "http://index.sci99.com/"
    siteRe = re.compile("/channel/(?P<sector>[A-Za-z]+)/")
    linkRe = re.compile("[/]{0,1}channel/[A-Za-z]+/.+")
    req = requests.get(site, timeout=timeout)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    links = root.xpath("//li/a[@href]")
    sectors = {}
    ppi = None
    for a in links:
        txt = a.text_content().strip()
        href = a.attrib["href"]
        m = re.match(siteRe, href)
        if m:
            abs_href = urllib.parse.urljoin(site, m.group())
            if m.group("sector") == "ppi":
                ppi = abs_href
            else:
                sectors[txt] = abs_href
    pages = {}
    for sector in sorted(sectors):
        req = requests.get(sectors[sector], timeout=timeout)
        if req.status_code != requests.codes.ok:
            continue
        root = lxml.html.fromstring(req.text)
        links = root.xpath("//dl/dt/a[@href]|//dl/dd/a[@href]")
        for a in links:
            txt = a.text_content().strip()
            href = a.attrib["href"]
            pages[txt] = urllib.parse.urljoin(site, href)
    for page in sorted(pages):
        all_pages = [pages[page]]
        visited_pages = []
        while all_pages:
            first_page = all_pages[0]
            all_pages = all_pages[1:]
            if first_page in visited_pages:
                continue
            print(first_page, file=sys.stderr)
            visited_pages += [first_page]
            req = requests.get(first_page, timeout=timeout)
            if req.status_code != requests.codes.ok:
                continue
            root = lxml.html.fromstring(req.text)
            tables = root.xpath("//table/tbody")
            for table in tables:
                for tr in table.xpath("tr"):
                    tds = tr.xpath("td")
                    if len(tds) == 5:
                        try:
                            px_date = tds[0].text_content().strip()
                            timestamp = datetime.strptime(px_date + "2359+0800", "%Y-%m-%d%H%M%z").timestamp()
                            px_last = try_float(tds[1].text_content().strip())
                            if page not in res:
                                res[page] = {}
                            res[page][timestamp] = px_last
                        except:
                            pass
            links = root.xpath("//a[@href]")
            for a in links:
                href = a.attrib["href"]
                m = re.match(linkRe, href)
                if m:
                    link = urllib.parse.urljoin(site, m.group())
                    if link not in visited_pages:
#                        all_pages += [link]  # Historical data only. Comment for faster access
                        pass
    # Baltic
    codes = ["BDI", "BCI", "BPI", "BSI", "BHSI", "BDTI", "BCTI"]
    for code in codes:
        params = { "ticker" : code }
        headers = { "Referer" : "http://marine-transportation.capitallink.com/", "X-Requested-With" : "XMLHttpRequest" }
        req = requests.get("http://marine-transportation.capitallink.com/indices/json_baltic_exchange.php", params=params, headers=headers, timeout=timeout)
        if req.status_code != requests.codes.ok:
            raise Exception("Failed to Load: %s" % req.url)
        if code not in res:
            res[code] = {}
        root = json.loads(req.text)
        for rec in root:
            res[code][datetime.combine(datetime.fromtimestamp(rec[0] / 1e3).date(), dtime.time(23, 59)).timestamp()] = rec[1]
    # Container Indices
    jsonRe = re.compile("^[^(]+\((?P<json>.+)\);$")
    rowRe = re.compile("[A-Z](?P<num>[0-9]*)")
    codes = ["CCFI", "SCFI"]
    for code in codes:
        site = "http://en.sse.net.cn/indices/%snew.jsp" % code.lower()
        jsonSite = "http://index.chineseshipping.com.cn/servlet/%sGetContrast" % code.lower()
        tries = timeout
        while tries > 0:
            try:
                req = requests.get(site, timeout=timeout)
                break
            except:
                tries -= 1
        if req.status_code != requests.codes.ok:
            raise Exception("Failed to Load: %s" % req.url)
        root = lxml.html.fromstring(req.text)
        tables = root.xpath("//table")
        targetTable = None
        for table in tables:
            if "Description" in table.text_content() or "Service" in table.text_content():
                targetTable = table
                break
        if targetTable is None:
            raise Exception("Invalid Page Format: %s" % req.url)
        headers = []
        for row in table.xpath("tr"):
            cols = row.xpath("td")
            slot_count = 0
            for col in cols:
                spans = col.xpath("span")
                if len(spans) > 0:
                    slot_count += 1
            if slot_count <= 0:
                continue
            headers += [cols[0].text_content().strip().replace(",", "")]
        tries = timeout
        while tries > 0:
            try:
                req = requests.get(jsonSite, timeout=timeout)
                break
            except:
                tries -= 1
        if req.status_code != requests.codes.ok:
            raise Exception("Failed to Load: %s" % req.url)
        m = re.match(jsonRe, req.text)
        if not m:
            raise Exception("Invalid Page Format: %s" % req.url)
        root = json.loads(m.group("json"))
        timestamp = datetime.strptime(root[code.lower()]["date"] + "2359+0800", "%Y-%m-%d%H%M%z").timestamp()
        values = [None] * (len(headers) - 1)
        for row in root[code.lower()]["data"]:
            m = re.match(rowRe, row)
            if not m:
                continue
            idx = try_int_0(m.group("num"))
            if math.isnan(idx):
                idx = 0
            try:
                values[idx] = root[code.lower()]["data"][row]
            except IndexError as e:
                continue
        if not timestamp:
            raise Exception("Invalid Page Format: %s" % req.url)
        for (h, v) in zip(headers, values):
            if not v:
                continue
            if "%s|%s" % (code, h) not in res:
                res["%s|%s" % (code, h)] = {}
            res["%s|%s" % (code, h)][timestamp] = v
    return res

def test(date, timeout=60):
    ref_date = datetime.strptime(date + "2359+0800", "%Y%m%d%H%M%z")
    res = {}
    # 100ppi
    site = "http://www.100ppi.com/"
    suffix = "/cindex/"
    siteRe = re.compile("/cindex/(?P<sector>[A-Za-z0-9-]+[.]html)")
    req = requests.get(urllib.parse.urljoin(site, suffix), timeout=timeout)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    links = root.xpath("//dd/a[@href]")
    pages = {}
    for a in links:
        txt = a.text_content().strip().replace(",", "")
        href = a.attrib["href"]
        m = re.match(siteRe, href)
        if m:
            abs_href = urllib.parse.urljoin(site, m.group())
            pages[txt] = abs_href
    for page in sorted(pages):
        print(pages[page], file=sys.stderr)
        tries = timeout
        while tries > 0:
            try:
                req = requests.get(pages[page], timeout=timeout)
                break
            except:
                tries -= 1
        if req.status_code != requests.codes.ok:
            continue
        root = lxml.html.fromstring(req.text)
        tables = root.xpath("//div/table")
        if len(tables) < 1:
            continue
        values = {}
        for table in tables:
            if "日期" in table.text_content():
                try:
                    trs = table.xpath("tr")
                    for (date_html, value_html) in zip(trs[0], trs[1]):
                        date_txt = date_html.text_content().strip()
                        parsed_date = None
                        try:
                            parsed_date = datetime.strptime("%d-%s2359+0800" % (ref_date.year, date_txt), "%Y-%m-%d%H%M%z")
                        except:
                            pass
                        if not parsed_date or parsed_date > ref_date:
                            continue
                        value = try_float(value_html.text_content().strip())
                        if page not in res:
                            res[page] = {}
                        res[page][parsed_date.timestamp()] = value
                except:
                    continue
                break
    return res

# Exchange Rates

def get_exchange_rates(date):
    parsedDate = datetime.strptime(date, "%Y%m%d")
    req = requests.get("http://www.hkex.com.hk/eng/market/sec_tradinfo/stampfx/%d/Documents/%s.xls" % (parsedDate.year, date), stream=True, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    workbook = xlrd.open_workbook(file_contents=req.raw.read(decode_content=True))
    worksheet = workbook.sheet_by_name(workbook.sheet_names()[0])
    for i in range(0, worksheet.nrows):
        if "U.S." in worksheet.cell_value(i, 0):
            usd_value = worksheet.cell_value(i, 2)
        elif "Ren" in worksheet.cell_value(i, 0):
            rmb_value = worksheet.cell_value(i, 2)
    return { "USD" : usd_value, "RMB" : rmb_value }

# Share Buybacks

def get_buybacks(date):
    res = {}
    parsedDate = datetime.strptime(date, "%Y%m%d")
    req = requests.get("http://www.hkexnews.hk/reports/sharerepur/documents/SRRPT%s.xls" % date, stream=True, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    workbook = xlrd.open_workbook(file_contents=req.raw.read(decode_content=True))
    worksheet = workbook.sheet_by_name(workbook.sheet_names()[0])
    amountRe = re.compile("(?P<currency>[A-Z]+) (?P<amount>[0-9.,]+)")
    for i in range(0, worksheet.nrows):
        code = try_int(worksheet.cell_value(i, 1))
        if math.isnan(code):
            continue
        timestamp = datetime.strptime(worksheet.cell_value(i, 3) + "2359+0800", "%Y/%m/%d%H%M%z").timestamp()
        (px_high, px_low, px_volume, px_value) = (float('nan'), float('nan'), float('nan'), float('nan'))
        px_volume = try_int(worksheet.cell_value(i, 4).replace(",", ""))
        m = re.match(amountRe, worksheet.cell_value(i, 5))
        if m:
            px_high = try_float(m.group("amount").replace(",", ""))
        m = re.match(amountRe, worksheet.cell_value(i, 6))
        if m:
            px_low = try_float(m.group("amount").replace(",", ""))
        else:
            px_low = px_high
        m = re.match(amountRe, worksheet.cell_value(i, 7))
        if m:
            px_value = try_float(m.group("amount").replace(",", ""))
        if math.isnan(px_high) or math.isnan(px_low) or math.isnan(px_volume) or math.isnan(px_value):
            continue
        res[code] = Buyback(px_high=px_high, px_low=px_low, px_volume=px_volume, px_value=px_value)
    return res

# Short Selling

def short_selling_helper(prefix):
    prefixes = { "am" : "MS", "daily" : "AS" }
    recordRe = re.compile(" +(?P<code>[0-9]+) .+ {3,}(?P<volume>[0-9,]+) {3,}(?P<value>[0-9,]+)")
    res = {}
    req = requests.get("https://www.hkex.com.hk/eng/stat/smstat/ssturnover/ncms/%sHTMAIN.HTM" % prefixes[prefix], timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    txts = root.xpath("//pre")
    if len(txts) < 1:
        raise Exception("Invalid Page Format: %s" % req.url)
    txt = txts[0].text_content().strip()
    lines = txt.split("\r\n")
    for line in lines:
        m = re.match(recordRe, line)
        if not m:
            continue
        code = int(float(m.group("code")))
        if code not in res:
            res[code] = {}
        volume = int(float(m.group("volume").replace(",", "")))
        value = float(m.group("value").replace(",", ""))
        res[code]["volume"] = volume
        res[code]["value"] = value
    return res

def get_short_selling(date):
    tmp = {}
    am = short_selling_helper("am")
    daily = short_selling_helper("daily")
    for code in am:
        if code not in tmp:
            tmp[code] = {}
        tmp[code]["am_short_volume"] = am[code]["volume"]
        tmp[code]["am_short_value"] = am[code]["value"]
    for code in daily:
        if code not in tmp:
            tmp[code] = {}
            tmp[code]["am_short_volume"] = 0
            tmp[code]["am_short_value"] = 0.0
        tmp[code]["daily_short_volume"] = daily[code]["volume"]
        tmp[code]["daily_short_value"] = daily[code]["value"]
    res = {}
    for code in tmp:
        try:
            res[code] = ShortSelling(am_short_volume=tmp[code]["am_short_volume"], am_short_value=tmp[code]["am_short_value"], daily_short_volume=tmp[code]["daily_short_volume"], daily_short_value=tmp[code]["daily_short_value"])
        except:
            continue
    return res

# Broker Activity

def get_broker_activity(code):
    mapping = { "bid" : "BrokerBuy", "ask" : "BrokerSell" }
    tmp = {}
    req = requests.get("http://data.tsci.com.cn/RDS.aspx?Code=E%05d&PkgType=11036&val=200" % code, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = json.loads(req.text)
    for index in mapping:
        if mapping[index] not in root:
            return {}
        for record in root[mapping[index]]:
            if record["BrokerNo"] not in tmp:
                tmp[record["BrokerNo"]] = {}
            tmp[record["BrokerNo"]]["%s_volume" % index] = try_float(record["shares"])
            tmp[record["BrokerNo"]]["%s_value" % index] = try_float(record["turnover"])
            tmp[record["BrokerNo"]]["%s_vwap" % index] = float('nan') if tmp[record["BrokerNo"]]["%s_volume" % index] == 0 else try_float(record["AV"])
    res = {}
    for record in tmp:
        res[record] = BrokerActivity(bid_volume=float('nan') if "bid_volume" not in tmp[record] else tmp[record]["bid_volume"], bid_value=float('nan') if "bid_value" not in tmp[record] else tmp[record]["bid_value"], bid_vwap=float('nan') if "bid_vwap" not in tmp[record] else tmp[record]["bid_vwap"], ask_volume=float('nan') if "ask_volume" not in tmp[record] else tmp[record]["ask_volume"], ask_value=float('nan') if "ask_value" not in tmp[record] else tmp[record]["ask_value"], ask_vwap=float('nan') if "ask_vwap" not in tmp[record] else tmp[record]["ask_vwap"])
    return res

# Corporate Actions

def parse_corporate_actions_tokens(tokens):
    zeroDivRe = re.compile("[Nn]o [Dd]ividend")
    divRe = re.compile("[Dd]: *(?P<currency>[A-Za-z]+) (?P<amount>[0-9.]+)")
    specialDivRe = re.compile("[Ss][Dd]: *(?P<currency>[A-Za-z]+) (?P<amount>[0-9.]+)")
    splitRe = re.compile("[Ss]: *(?P<original>[0-9.]+)-for-(?P<new>[0-9.]+)")
    bonusRe = re.compile("[Bb]: *(?P<numerator>[0-9.]+)-for-(?P<denominator>[0-9.]+)")
    consolidationRe = re.compile("[Cc]: *(?P<new>[0-9.]+)-for-(?P<original>[0-9.]+)")
    rightsRe = re.compile("[Rr]: *(?P<numerator>[0-9.]+)-for-(?P<denominator>[0-9.]+)@(?P<currency>[A-Za-z]+) (?P<price>[0-9.]+)")
    allRe = (zeroDivRe, divRe, specialDivRe, splitRe, bonusRe, consolidationRe, rightsRe)
    try:
        announcementDate = datetime.strptime(tokens[0] + "+0800", "%Y/%m/%d%z")
        exDate = datetime.strptime(tokens[5] + "+0800", "%Y/%m/%d%z")
        try:
            financialYear = int(re.match("(?P<year>[0-9]{4})/[0-9]+", tokens[1]).group("year"))
        except AttributeError:
            financialYear = None
        financialPeriod = tokens[2]
        actionType = None
        amount = None
        currency = None
        dilutionRatio = None
        for r in allRe:
            m = re.match(r, tokens[3])
            if m:
                if r == zeroDivRe:
                    actionType = 'D'
                    amount = 0
                    dilutionRatio = 1.0
                elif r == divRe:
                    actionType = 'D'
                    amount = float(m.group("amount"))
                    currency = m.group("currency")
                    dilutionRatio = 1.0
                elif r == specialDivRe:
                    actionType = 'SD'
                    amount = float(m.group("amount"))
                    currency = m.group("currency")
                    dilutionRatio = 1.0
                elif r == splitRe:
                    actionType = 'S'
                    dilutionRatio = float(m.group("new")) / float(m.group("original"))
                elif r == bonusRe:
                    actionType = 'S'
                    dilutionRatio = 1.0 + float(m.group("numerator")) / float(m.group("denominator"))
                elif r == consolidationRe:
                    actionType = 'S'
                    dilutionRatio = float(m.group("new")) / float(m.group("original"))
                elif r == rightsRe:
                    actionType = 'R'
                    amount = float(m.group("price"))
                    currency = m.group("currency")
                    dilutionRatio = 1.0 + float(m.group("numerator")) / float(m.group("denominator"))
                break
        if not actionType:
            return None
        else:
            return CorporateAction(ex_timestamp=exDate.timestamp(), financial_year=financialYear, financial_period=financialPeriod, announcement_timestamp=announcementDate.timestamp(), action_type=actionType, amount=amount, currency=currency, dilution_ratio=dilutionRatio)
    except ValueError:
        return None

def get_corporate_actions(code):
    params = { "CFType" : 9, "symbol" : code }
    req = requests.get("http://www.aastocks.com/en/Stock/CompanyFundamental.aspx", params=params, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    cacs = None
    tables = root.xpath("//table")
    for table in tables:
        if "Dividend Type" in table.text_content():
            cacs = table
            break
    if cacs is None:
        raise Exception("Invalid Page Format: %s" % req.url)
    if "No related information" in cacs.text_content():
        return []
    corporateActions = []
    for row in cacs.xpath("tr"):
        cols = row.xpath("td")
        tokens = tuple(c.text_content().strip() for c in cols)
        if len(tokens) != 8:
            continue
        parsed = parse_corporate_actions_tokens(tokens)
        if parsed:
            corporateActions += [parsed]
    ordering = { 'R' : 0, 'S' : 1, 'SD' : 2, 'D' : 3 }
    corporateActions = sorted(corporateActions, key=lambda x: (x.ex_timestamp, ordering[x.action_type]))
    # Timestamp Hack
    realCorporateActions = []
    firstTimestamp = None
    count = 0
    for c in corporateActions:
        if c.ex_timestamp != firstTimestamp:
            realCorporateActions += [c]
            firstTimestamp = c.ex_timestamp
            count = 0
        else:
            count += 1
            realCorporateActions += [CorporateAction(ex_timestamp=c.ex_timestamp+count, financial_year=c.financial_year, financial_period=c.financial_period, announcement_timestamp=c.announcement_timestamp, action_type=c.action_type, amount=c.amount, currency=c.currency, dilution_ratio=c.dilution_ratio)]
    return realCorporateActions

# Industry

def get_industry(code, exchange_code="XHKG"):
    countries = { "XHKG" : "HK", "XSHG" : "CN" }
    req = requests.get("http://quotes.wsj.com/%s/%s/%d/company-people" % (countries[exchange_code], exchange_code, code), timeout=60)  # HK/XHKG/
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    notFounds = root.xpath("//h1[text()='Company Not Found']")
    if len(notFounds) > 0:
        return Industry(industry="-", sector="-")
    spans = root.xpath("//div/span[@class='data_lbl']|//div/span[@class='data_data']")
    tmp = {}
    current_key = None
    for span in spans:
        if span.attrib["class"] == "data_lbl":
            current_key = span.text_content().strip()
        elif span.attrib["class"] == "data_data":
            if not current_key:
                continue
            else:
                current_value = span.text_content().strip()
                if current_key not in tmp:
                    tmp[current_key] = current_value
    return Industry(industry=tmp["Industry"].replace(",", ""), sector=tmp["Sector"].replace(",", ""))

# Number of Employees

def get_employees(code, exchange_code="XHKG"):
    countries = { "XHKG" : "HK", "XSHG" : "CN" }
    req = requests.get("http://quotes.wsj.com/%s/%s/%d/company-people" % (countries[exchange_code], exchange_code, code), timeout=60)  # HK/XHKG/
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    notFounds = root.xpath("//h1[text()='Company Not Found']")
    if len(notFounds) > 0:
        return float('nan')
    spans = root.xpath("//div/span[@class='data_lbl']|//div/span[@class='data_data']")
    tmp = {}
    current_key = None
    for span in spans:
        if span.attrib["class"] == "data_lbl":
            current_key = span.text_content().strip()
        elif span.attrib["class"] == "data_data":
            if not current_key:
                continue
            else:
                current_value = span.text_content().strip()
                if current_key not in tmp:
                    tmp[current_key] = current_value
    return try_int(tmp["Employees"].replace(",", ""))

# Issued Shares

def get_issued_shares(code):
    params = { "CFType" : 3, "symbol" : code }
    req = requests.get("http://www.aastocks.com/en/Stock/CompanyFundamental.aspx", params=params, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    tables = root.xpath("//table")
    targetTable = None
    for table in tables:
        if "Issued Capital" in table.text_content().strip():
            targetTable = table
            break
    if targetTable is None:
        raise Exception("Invalid Page Format: %s" % req.url)
    issuedSharesRe = re.compile("Issued Capital.+")
    issuedSharesHRe = re.compile("Issue Cap-H.+")
    issuedShares = float('nan')
    issuedSharesH = float('nan')
    rows = targetTable.xpath("tr")
    for row in rows:
        contents = row.xpath("td")
        tokens = tuple(c.text_content().strip() for c in contents)
        if len(tokens) == 2:
            m = re.match(issuedSharesRe, tokens[0])
            if m:
                issuedShares = try_float(tokens[1].replace(",", ""))
                continue
            m = re.match(issuedSharesHRe, tokens[0])
            if m:
                issuedSharesH = try_float(tokens[1].replace(",", ""))
                continue
    if math.isnan(issuedShares):
        raise Exception("Invalid Page Format: %s" % req.url)
    if math.isnan(issuedSharesH):
        issuedSharesH = issuedShares
    return IssuedShares(issued_shares=issuedShares, issued_shares_h=issuedSharesH)

# Institutional Shares

def get_institutional_shares(code):
    params = { "symbol" : "%04d.HK" % code }
    req = requests.get("http://www.reuters.com/finance/stocks/financialHighlights", params=params, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    tables = root.xpath("//table")
    targetTable = None
    for table in tables:
        if "% Shares Owned" in table.text_content().strip():
            targetTable = table
            break
    if targetTable is None:
        redirectedToLookup = "http://www.reuters.com/finance/stocks/lookup" in req.url
        if redirectedToLookup:
            return InstitutionalShares(institutional_holders=0, institutional_shares=0, three_month_new=0, three_month_closed=0, three_month_increased=0, three_month_decreased=0)
        divs = root.xpath("//div")
        targetDiv = None
        for div in divs:
            if "Institutional Holders" in div.text_content().strip():
                targetDiv = div
                break
        if targetDiv is None:
            raise Exception("Invalid Page Format: %s" % req.url)
        else:
            return InstitutionalShares(institutional_holders=0, institutional_shares=0, three_month_new=0, three_month_closed=0, three_month_increased=0, three_month_decreased=0)
    mapped = {}
    rows = targetTable.xpath("tbody/tr")
    for row in rows:
        contents = row.xpath("td")
        tokens = tuple(c.text_content().strip() for c in contents)
        if len(tokens) == 2:
            try:
                mapped[tokens[0]] = int(float(tokens[1].replace(",", "").replace("%", "")))
            except:
                pass
    try:
        return InstitutionalShares(institutional_holders=mapped["# of Holders:"], institutional_shares=mapped["Total Shares Held:"], three_month_new=mapped["# New Positions:"], three_month_closed=mapped["# Closed Positions:"], three_month_increased=mapped["# Increased Positions:"], three_month_decreased=mapped["# Reduced Positions:"])
    except KeyError:
        raise Exception("Invalid Page Format: %s" % req.url)

# Fundamentals

def sina_helper(code, fundamentals_type, period):
    pages = ["FinanceStandard", "BalanceSheet", "FinanceStatus", "CashFlow"]
    secondParams = ["financeStanderd", "balanceSheet", "financeStatus", "cashFlow"]
    periods = ["zero", "1", "2", "3"]
    params = { "symbol" : code, secondParams[fundamentals_type] : periods[period] }
    headers = { "Referer" : "http://stock.finance.sina.com.cn/" }
    req = requests.get("http://stock.finance.sina.com.cn/hkstock/api/jsonp.php//FinanceStatusService.get%sForjs" % pages[fundamentals_type], params=params, headers=headers, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    req.encoding = "gb2312"
    jsonRe = re.compile("^\((?P<json>.+)\);$")
    m = re.match(jsonRe, req.text)
    if not m:
        raise Exception("Error loading %s" % pages[fundamentals_type])
    values = json.loads(m.group("json"))
    return values

def get_fundamentals(code):
    currency_map = { "港币" : "HKD", "人民币" : "RMB", "美元" : "USD", "欧元" : "EUR", "加元" : "CAD" }
    period_map = { "一季报" : "1stQuarterly", "中报" : "Interim", "三季报" : "3rdQuarterly", "年报" : "Final" }
    reporting_map = {}
    tmp = {}
    for fundamentals_type in range(0, 4):
        for period in range(0, 4):
            sina = sina_helper(code, fundamentals_type, period)
            if not sina:
                continue
            for c in sina:
                # Key
                financial_period_raw = c[3 if fundamentals_type == 0 else 1]
                financial_period = period_map[financial_period_raw]
                if financial_period not in tmp:
                    tmp[financial_period] = {}
                financial_period_start = c[0]
                financial_period_end = c[1 if fundamentals_type == 0 else 0]
                financial_year = None
                try:
                    financial_year = (datetime.strptime(financial_period_start, "%Y-%m-%d") - dtime.timedelta(days=1)).year + 1
                except:
                    pass
                if not financial_year:
                    continue
                # Specific Items
                if fundamentals_type == 0:
                    reporting_date = c[2]
                    reporting_timestamp = datetime.strptime(c[2] + "2359+0800", "%Y-%m-%d%H%M%z").timestamp()
                    if financial_period_end not in reporting_map:
                        reporting_map[financial_period_end] = reporting_timestamp
                    if reporting_map[financial_period_end] not in tmp[financial_period]:
                        tmp[financial_period][reporting_map[financial_period_end]] = {}
                    tmp[financial_period][reporting_map[financial_period_end]]["currency"] = currency_map[c[15]]
                    tmp[financial_period][reporting_map[financial_period_end]]["financialPeriodStart"] = c[0]
                    tmp[financial_period][reporting_map[financial_period_end]]["financialPeriodEnd"] = financial_period_end
                    tmp[financial_period][reporting_map[financial_period_end]]["reportingDate"] = reporting_date
                    tmp[financial_period][reporting_map[financial_period_end]]["financialYear"] = financial_year
                    tmp[financial_period][reporting_map[financial_period_end]]["extraordinaryIncome"] = 0 if math.isnan(try_float(c[8])) else try_float(c[8]) * 1e6
                    tmp[financial_period][reporting_map[financial_period_end]]["eps"] = try_float(c[9]) * 1e-2
                    tmp[financial_period][reporting_map[financial_period_end]]["dilutedEps"] = tmp[financial_period][reporting_map[financial_period_end]]["eps"] if math.isnan(try_float(c[10])) else try_float(c[10]) * 1e-2
                    tmp[financial_period][reporting_map[financial_period_end]]["sdps"] = try_float(c[11]) * 1e-2
                elif fundamentals_type == 1:
                    if financial_period_end not in reporting_map:
                        continue
                    if reporting_map[financial_period_end] not in tmp[financial_period]:
                        tmp[financial_period][reporting_map[financial_period_end]] = {}
                    if len(c) == 27:
                        tmp[financial_period][reporting_map[financial_period_end]]["cash"] = try_float(c[19]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["accountsReceivable"] = try_float(c[17]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["inventory"] = try_float(c[18]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["currentAssets"] = try_float(c[3]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["loans"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["ppe"] = try_float(c[13]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["associates"] = try_float(c[15]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["intangibles"] = try_float(c[12]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["noncurrentAssets"] = try_float(c[2]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["totalAssets"] = try_float(c[23]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["accountsPayable"] = try_float(c[20]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["currentLiabilities"] = try_float(c[4]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["bankDebt"] = 0 if math.isnan(try_float(c[21])) else try_float(c[21]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["longTermDebt"] = 0 if math.isnan(try_float(c[6])) else try_float(c[6]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["deposits"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["totalLiabilities"] = tmp[financial_period][reporting_map[financial_period_end]]["currentLiabilities"] + tmp[financial_period][reporting_map[financial_period_end]]["longTermDebt"]  #
                        tmp[financial_period][reporting_map[financial_period_end]]["minorityEquity"] = 0 if math.isnan(try_float(c[7])) else try_float(c[7]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["shareCapital"] = try_float(c[9]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["reserve"] = try_float(c[10]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["totalEquity"] = try_float(c[11]) * 1e6
                    elif len(c) == 32:
                        tmp[financial_period][reporting_map[financial_period_end]]["cash"] = try_float(c[2]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["accountsReceivable"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["inventory"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["currentAssets"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["loans"] = try_float(c[6]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["ppe"] = try_float(c[11]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["intangibles"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["associates"] = try_float(c[8]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["noncurrentAssets"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["totalAssets"] = try_float(c[13]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["accountsPayable"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["currentLiabilities"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["bankDebt"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["longTermDebt"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["deposits"] = try_float(c[16]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["totalLiabilities"] = try_float(c[19]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["minorityEquity"] = 0 if math.isnan(try_float(c[24])) else try_float(c[24]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["shareCapital"] = try_float(c[21]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["reserve"] = try_float(c[22]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["totalEquity"] = try_float(c[23]) * 1e6
                    else:
                        pass
                elif fundamentals_type == 2:
                    if financial_period_end not in reporting_map:
                        continue
                    if reporting_map[financial_period_end] not in tmp[financial_period]:
                        tmp[financial_period][reporting_map[financial_period_end]] = {}
                    if len(c) == 22:
                        tmp[financial_period][reporting_map[financial_period_end]]["interestRevenue"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["interestPayout"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["netInterestRevenue"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["otherRevenue"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["revenue"] = try_float(c[2]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["cogs"] = try_float(c[13]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["grossProfit"] = try_float(c[18]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["salesExpense"] = try_float(c[15]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["generalExpense"] = 0 if math.isnan(try_float(c[16])) else try_float(c[16]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["creditProvisions"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["depreciation"] = try_float(c[14]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["operatingIncome"] = try_float(c[19]) * 1e6  #
                        tmp[financial_period][reporting_map[financial_period_end]]["associatesOperatingIncome"] = try_float(c[20]) * 1e6  #
                        tmp[financial_period][reporting_map[financial_period_end]]["interestExpense"] = 0 if math.isnan(try_float(c[17])) else try_float(c[17]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["ebt"] = try_float(c[3]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["ebit"] = tmp[financial_period][reporting_map[financial_period_end]]["ebt"] + tmp[financial_period][reporting_map[financial_period_end]]["interestExpense"]  #
                        tmp[financial_period][reporting_map[financial_period_end]]["tax"] = try_float(c[4]) * 1e6 * -1  #
                        tmp[financial_period][reporting_map[financial_period_end]]["netIncomePlusMinorityInterest"] = try_float(c[5]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["minorityInterest"] = try_float(c[6]) * 1e6 * -1  #
                        tmp[financial_period][reporting_map[financial_period_end]]["netIncome"] = try_float(c[7]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["dividend"] = 0 if math.isnan(try_float(c[8])) else try_float(c[8]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["retainedEarnings"] = try_float(c[9]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["dps"] = 0 if math.isnan(try_float(c[12])) else try_float(c[12]) * 1e-2
                    elif len(c) == 20:
                        tmp[financial_period][reporting_map[financial_period_end]]["interestRevenue"] = try_float(c[2]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["interestPayout"] = try_float(c[3]) * 1e6 * -1  #
                        tmp[financial_period][reporting_map[financial_period_end]]["netInterestRevenue"] = try_float(c[4]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["otherRevenue"] = try_float(c[5]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["revenue"] = try_float(c[6]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["cogs"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["grossProfit"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["salesExpense"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["generalExpense"] = try_float(c[7]) * 1e6 * -1  #
                        tmp[financial_period][reporting_map[financial_period_end]]["creditProvisions"] = try_float(c[8]) * 1e6 * -1  #
                        tmp[financial_period][reporting_map[financial_period_end]]["depreciation"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["operatingIncome"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["associatesOperatingIncome"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["ebit"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["interestExpense"] = float('nan')
                        tmp[financial_period][reporting_map[financial_period_end]]["ebt"] = try_float(c[10]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["operatingIncome"] = tmp[financial_period][reporting_map[financial_period_end]]["ebt"] + try_float(c[9]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["tax"] = try_float(c[11]) * 1e6 * -1  #
                        tmp[financial_period][reporting_map[financial_period_end]]["netIncomePlusMinorityInterest"] = try_float(c[12]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["minorityInterest"] = try_float(c[13]) * 1e6 * -1  #
                        tmp[financial_period][reporting_map[financial_period_end]]["netIncome"] = try_float(c[14]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["dividend"] = 0 if math.isnan(try_float(c[15])) else try_float(c[15]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["retainedEarnings"] = try_float(c[16]) * 1e6
                        tmp[financial_period][reporting_map[financial_period_end]]["dps"] = float('nan')
                elif fundamentals_type == 3:
                    if financial_period_end not in reporting_map:
                        continue
                    if reporting_map[financial_period_end] not in tmp[financial_period]:
                        tmp[financial_period][reporting_map[financial_period_end]] = {}
                    tmp[financial_period][reporting_map[financial_period_end]]["operatingCashFlow"] = try_float(c[2]) * 1e6
                    tmp[financial_period][reporting_map[financial_period_end]]["capex"] = 0 if math.isnan(try_float(c[9])) else try_float(c[9]) * 1e6
                    tmp[financial_period][reporting_map[financial_period_end]]["investingCashFlow"] = try_float(c[3]) * 1e6
                    tmp[financial_period][reporting_map[financial_period_end]]["financingCashFlow"] = try_float(c[4]) * 1e6
                    tmp[financial_period][reporting_map[financial_period_end]]["fxTranslation"] = try_float(c[8]) * 1e6
                    tmp[financial_period][reporting_map[financial_period_end]]["beginningCash"] = try_float(c[6]) * 1e6
                    tmp[financial_period][reporting_map[financial_period_end]]["incrementalCash"] = try_float(c[5]) * 1e6
    res = {}
    for fp in tmp:
        for fe in tmp[fp]:
            try:
                financial_year = tmp[fp][fe]["financialYear"]
                key = CompoundKey(code=code, financial_year=financial_year, financial_period=fp)
                fundamentals = Fundamentals\
                               (\
                                 financial_period_start=tmp[fp][fe]["financialPeriodStart"],\
                                 financial_period_end=tmp[fp][fe]["financialPeriodEnd"],\
                                 reporting_date=tmp[fp][fe]["reportingDate"],\
                                 currency=tmp[fp][fe]["currency"],\
                                 extraordinary_income=tmp[fp][fe]["extraordinaryIncome"],\
                                 eps=tmp[fp][fe]["eps"],\
                                 diluted_eps=tmp[fp][fe]["dilutedEps"],\
                                 dps=tmp[fp][fe]["dps"],\
                                 sdps=tmp[fp][fe]["sdps"],\
                                 cash=tmp[fp][fe]["cash"],\
                                 accounts_receivable=tmp[fp][fe]["accountsReceivable"],\
                                 inventory=tmp[fp][fe]["inventory"],\
                                 current_assets=tmp[fp][fe]["currentAssets"],\
                                 loans=tmp[fp][fe]["loans"],\
                                 ppe=tmp[fp][fe]["ppe"],\
                                 intangibles=tmp[fp][fe]["intangibles"],\
                                 associates=tmp[fp][fe]["associates"],\
                                 noncurrent_assets=tmp[fp][fe]["noncurrentAssets"],\
                                 total_assets=tmp[fp][fe]["totalAssets"],\
                                 accounts_payable=tmp[fp][fe]["accountsPayable"],\
                                 current_liabilities=tmp[fp][fe]["currentLiabilities"],\
                                 bank_debt=tmp[fp][fe]["bankDebt"],\
                                 long_term_debt=tmp[fp][fe]["longTermDebt"],\
                                 deposits=tmp[fp][fe]["deposits"],\
                                 total_liabilities=tmp[fp][fe]["totalLiabilities"],\
                                 minority_equity=tmp[fp][fe]["minorityEquity"],\
                                 share_capital=tmp[fp][fe]["shareCapital"],\
                                 reserve=tmp[fp][fe]["reserve"],\
                                 total_equity=tmp[fp][fe]["totalEquity"],\
                                 interest_revenue=tmp[fp][fe]["interestRevenue"],\
                                 interest_payout=tmp[fp][fe]["interestPayout"],\
                                 net_interest_revenue=tmp[fp][fe]["netInterestRevenue"],\
                                 other_revenue=tmp[fp][fe]["otherRevenue"],\
                                 revenue=tmp[fp][fe]["revenue"],\
                                 cogs=tmp[fp][fe]["cogs"],\
                                 gross_profit=tmp[fp][fe]["grossProfit"],\
                                 sales_expense=tmp[fp][fe]["salesExpense"],\
                                 general_expense=tmp[fp][fe]["generalExpense"],\
                                 credit_provisions=tmp[fp][fe]["creditProvisions"],\
                                 depreciation=tmp[fp][fe]["depreciation"],\
                                 operating_income=tmp[fp][fe]["operatingIncome"],\
                                 associates_operating_income=tmp[fp][fe]["associatesOperatingIncome"],\
                                 ebit=tmp[fp][fe]["ebit"],\
                                 interest_expense=tmp[fp][fe]["interestExpense"],\
                                 ebt=tmp[fp][fe]["ebt"],\
                                 tax=tmp[fp][fe]["tax"],\
                                 minority_interest=tmp[fp][fe]["minorityInterest"],\
                                 net_income=tmp[fp][fe]["netIncome"],\
                                 dividend=tmp[fp][fe]["dividend"],\
                                 retained_earnings=tmp[fp][fe]["retainedEarnings"],\
                                 operating_cash_flow=tmp[fp][fe]["operatingCashFlow"],\
                                 capex=tmp[fp][fe]["capex"],\
                                 investing_cash_flow=tmp[fp][fe]["investingCashFlow"],\
                                 financing_cash_flow=tmp[fp][fe]["financingCashFlow"],\
                                 fx_translation=tmp[fp][fe]["fxTranslation"]\
                               )
                res[key] = fundamentals
            except:
                continue
    return res

def get_fundamentals_ltm(code):
    fundamentals = get_fundamentals(code)
    ltm = {}
    for key in fundamentals:
        if key.financial_period == "Final":
            ltm[key] = fundamentals[key]
            continue
        last_final_key = CompoundKey(code=key.code, financial_year=key.financial_year-1, financial_period="Final")
        last_ltm_key = CompoundKey(code=key.code, financial_year=key.financial_year-1, financial_period=key.financial_period)
        if last_final_key not in fundamentals or last_ltm_key not in fundamentals:
            continue
        ltm_fundamentals = Fundamentals\
                           (\
                            financial_period_start=fundamentals[key].financial_period_start,\
                            financial_period_end=fundamentals[key].financial_period_end,\
                            reporting_date=fundamentals[key].reporting_date,\
                            currency=fundamentals[key].currency,\
                            extraordinary_income=fundamentals[key].extraordinary_income,
                            eps=fundamentals[last_final_key].eps-fundamentals[last_ltm_key].eps+fundamentals[key].eps,\
                            diluted_eps=fundamentals[last_final_key].diluted_eps-fundamentals[last_ltm_key].diluted_eps+fundamentals[key].diluted_eps,\
                            dps=fundamentals[key].dps,\
                            sdps=fundamentals[key].sdps,\
                            cash=fundamentals[key].cash,\
                            accounts_receivable=fundamentals[key].accounts_receivable,\
                            inventory=fundamentals[key].inventory,\
                            current_assets=fundamentals[key].current_assets,\
                            loans=fundamentals[key].loans,\
                            ppe=fundamentals[key].ppe,\
                            intangibles=fundamentals[key].intangibles,\
                            associates=fundamentals[key].associates,\
                            noncurrent_assets=fundamentals[key].noncurrent_assets,\
                            total_assets=fundamentals[key].total_assets,\
                            accounts_payable=fundamentals[key].accounts_payable,\
                            current_liabilities=fundamentals[key].current_liabilities,\
                            bank_debt=fundamentals[key].bank_debt,\
                            long_term_debt=fundamentals[key].long_term_debt,\
                            deposits=fundamentals[key].deposits,\
                            total_liabilities=fundamentals[key].total_liabilities,\
                            minority_equity=fundamentals[key].minority_equity,\
                            share_capital=fundamentals[key].share_capital,\
                            reserve=fundamentals[key].reserve,\
                            total_equity=fundamentals[key].total_equity,\
                            interest_revenue=fundamentals[last_final_key].interest_revenue-fundamentals[last_ltm_key].interest_revenue+fundamentals[key].interest_revenue,\
                            interest_payout=fundamentals[last_final_key].interest_payout-fundamentals[last_ltm_key].interest_payout+fundamentals[key].interest_payout,\
                            net_interest_revenue=fundamentals[last_final_key].net_interest_revenue-fundamentals[last_ltm_key].net_interest_revenue+fundamentals[key].net_interest_revenue,\
                            other_revenue=fundamentals[last_final_key].other_revenue-fundamentals[last_ltm_key].other_revenue+fundamentals[key].other_revenue,\
                            revenue=fundamentals[last_final_key].revenue-fundamentals[last_ltm_key].revenue+fundamentals[key].revenue,\
                            cogs=fundamentals[last_final_key].cogs-fundamentals[last_ltm_key].cogs+fundamentals[key].cogs,\
                            gross_profit=fundamentals[last_final_key].gross_profit-fundamentals[last_ltm_key].gross_profit+fundamentals[key].gross_profit,\
                            sales_expense=fundamentals[last_final_key].sales_expense-fundamentals[last_ltm_key].sales_expense+fundamentals[key].sales_expense,\
                            general_expense=fundamentals[last_final_key].general_expense-fundamentals[last_ltm_key].general_expense+fundamentals[key].general_expense,\
                            credit_provisions=fundamentals[last_final_key].credit_provisions-fundamentals[last_ltm_key].credit_provisions+fundamentals[key].credit_provisions,\
                            depreciation=fundamentals[last_final_key].depreciation-fundamentals[last_ltm_key].depreciation+fundamentals[key].depreciation,\
                            operating_income=fundamentals[last_final_key].operating_income-fundamentals[last_ltm_key].operating_income+fundamentals[key].operating_income,\
                            associates_operating_income=fundamentals[last_final_key].associates_operating_income-fundamentals[last_ltm_key].associates_operating_income+fundamentals[key].associates_operating_income,\
                            ebit=fundamentals[last_final_key].ebit-fundamentals[last_ltm_key].ebit+fundamentals[key].ebit,\
                            interest_expense=fundamentals[last_final_key].interest_expense-fundamentals[last_ltm_key].interest_expense+fundamentals[key].interest_expense,\
                            ebt=fundamentals[last_final_key].ebt-fundamentals[last_ltm_key].ebt+fundamentals[key].ebt,\
                            tax=fundamentals[last_final_key].tax-fundamentals[last_ltm_key].tax+fundamentals[key].tax,\
                            minority_interest=fundamentals[last_final_key].minority_interest-fundamentals[last_ltm_key].minority_interest+fundamentals[key].minority_interest,\
                            net_income=fundamentals[last_final_key].net_income-fundamentals[last_ltm_key].net_income+fundamentals[key].net_income,\
                            dividend=fundamentals[last_final_key].dividend-fundamentals[last_ltm_key].dividend+fundamentals[key].dividend,\
                            retained_earnings=fundamentals[last_final_key].retained_earnings-fundamentals[last_ltm_key].retained_earnings+fundamentals[key].retained_earnings,\
                            operating_cash_flow=fundamentals[last_final_key].operating_cash_flow-fundamentals[last_ltm_key].operating_cash_flow+fundamentals[key].operating_cash_flow,\
                            capex=fundamentals[last_final_key].capex-fundamentals[last_ltm_key].capex+fundamentals[key].capex,\
                            investing_cash_flow=fundamentals[last_final_key].investing_cash_flow-fundamentals[last_ltm_key].investing_cash_flow+fundamentals[key].investing_cash_flow,\
                            financing_cash_flow=fundamentals[last_final_key].financing_cash_flow-fundamentals[last_ltm_key].financing_cash_flow+fundamentals[key].financing_cash_flow,\
                            fx_translation=fundamentals[last_final_key].fx_translation-fundamentals[last_ltm_key].fx_translation+fundamentals[key].fx_translation\
                           )
        ltm[key] = ltm_fundamentals
    return ltm

# Forecasted Data

def get_exchange_code_ft(code, country="HK"):
    if country == "HK":
        return "HKG"
    elif country == "CN":
        if code < 600000:
            return "SZ"
        else:
            return "SHH"
    else:
        return None

def get_exchange_code_wsj(code, country="HK"):
    if country == "HK":
        return "XHKG"
    elif country == "CN":
        if code < 600000:
            return "XSHE"
        else:
            return "XSHG"
    else:
        return None

def ft_helper(code, exchange_code="HKG"):
    params = { "s" : "%d:%s" % (code, exchange_code) }
    headers = { "Referer" : "http://markets.ft.com/" }
    req = requests.get("http://markets.ft.com/research/Markets/Tearsheets/Forecasts", params=params, headers=headers, timeout=60)
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    scripts = root.xpath("//script[@type='text/javascript']")
    targetScript = None
    for script in scripts:
        if script.text and "HOVERBODIES" in script.text:
            targetScript = script
            break
    if targetScript is None:
        raise Exception("Invalid Page Format: %s" % req.url)
    hoverRe = re.compile(".+BindToolTipHover[^']+'(?P<group>[^']+)'[^']+")
    hoverBodyRe = re.compile(".+HOVERBODIES\[\"[^\"]+\"\][^\"]+\"(?P<hypertext>[^\"]+)\"[^\"]+")
    lines = targetScript.text.split("\n")
    res = {}
    forecastType = None
    for line in lines:
        hoverMatch = re.match(hoverRe, line)
        hoverBodyMatch = re.match(hoverBodyRe, line)
        if hoverMatch:
            forecastType = hoverMatch.group("group")
            if forecastType not in res:
                res[forecastType] = {}
        if hoverBodyMatch:
            title = None
            table = lxml.html.fromstring(hoverBodyMatch.group("hypertext"))
            titles = table.xpath("//span")
            rows = table.xpath("//tr")
            if titles:
                title = titles[0].text.strip()
            for row in rows:
                headings = row.xpath("th")
                contents = row.xpath("td")
                if len(headings) + len(contents) < 1:
                    continue
                if len(headings) > 0:
                    heading = headings[0].text.strip()
                else:
                    heading = contents[0].text.strip()
                    contents = contents[1:]
                if len(contents) < 1:
                    title = heading
                else:
                    content = contents[0].text.strip()
                    if title:
                        if title not in res[forecastType]:
                            res[forecastType][title] = {}
                        res[forecastType][title][heading] = content
    return res

def wsj_helper(code, exchange_code="XHKG"):
    countries = { "XHKG" : "HK", "XSHG" : "CN" }
    res = {}
    req = requests.get("http://quotes.wsj.com/%s/%s/%d/research-ratings" % (countries[exchange_code], exchange_code, code), timeout=60)  # HK/XHKG/
    if req.status_code != requests.codes.ok:
        raise Exception("Failed to Load: %s" % req.url)
    root = lxml.html.fromstring(req.text)
    tables = root.xpath("//tbody")
    targetTable = None
    notFounds = root.xpath("//h1[text()='Company Not Found']")
    notAvailables = root.xpath("//span[@class='data_none']")
    for table in tables:
        contents = table.text_content().strip()
        if "High" in contents and "Low" in contents and "Median" in contents and "Average" in contents:
            targetTable = table
            break
    if targetTable is None:
        if len(notAvailables) < 1 and len(notFounds) < 1:
            raise Exception("Invalid Page Format: %s" % req.url)
    if len(notAvailables) > 0 or len(notFounds) > 0:
        res["High"] = 'nan'
        res["Low"] = 'nan'
        res["Median"] = 'nan'
        res["Average"] = 'nan'
        return res
    rows = targetTable.xpath("tr")
    for row in rows:
        columns = row.xpath("td")
        tokens = tuple(c.text_content().strip() for c in columns)
        if len(tokens) < 1:
            continue
        res[tokens[0]] = tokens[1]
    return res

def get_forecasted_fundamentals(code):  # Sales, EPS, DPS
    res = {}
    temp = {}
    ft = ft_helper(code)
    revenueRe = re.compile(".*[Rr]evenue.*")
    epsRe = re.compile(".*[Ee]arnings.*")
    dpsRe = re.compile(".*[Dd]ividend.*")
    allRe = [revenueRe, epsRe, dpsRe]
    for forecastType in ft:
        for r in allRe:
            m = re.match(r, forecastType)
            if not m:
                continue
            for title in ft[forecastType]:
                titleMatch = re.match("(?P<year>[0-9]{4})[^0-9]+", title)
                if not titleMatch:
                    continue
                key = CompoundKey(code=code, financial_year=int(titleMatch.group("year")), financial_period="Final")
                if key not in temp:
                    temp[key] = {}
                try:
                    reported = float(ft[forecastType][title]["Reported"].replace(",", ""))
                except:
                    if r == dpsRe:
                        temp[key]["dps"] = try_float(ft[forecastType][title]["Consensus"])
                    else:
                        prefix = ("revenue" if r == revenueRe else "eps")
                        temp[key][prefix + "Analysts"] = try_int_0(ft[forecastType][title]["Analysts"])
                        temp[key][prefix + "High"] = try_float(ft[forecastType][title]["High"].replace(",", ""))
                        temp[key][prefix + "Low"] = try_float(ft[forecastType][title]["Low"].replace(",", ""))
                        temp[key][prefix] = try_float(ft[forecastType][title]["Consensus"].replace(",", ""))
    for key in temp:
        try:
            res[key] = ForecastedFundamentals(revenue_analysts=temp[key]["revenueAnalysts"], revenue_high=temp[key]["revenueHigh"], revenue_low=temp[key]["revenueLow"], revenue=temp[key]["revenue"], eps_analysts=temp[key]["epsAnalysts"], eps_high=temp[key]["epsHigh"], eps_low=temp[key]["epsLow"], eps=temp[key]["eps"], dps=temp[key]["dps"])
        except KeyError:
            continue
    return res

def get_ratings(code, country="HK"):
    temp = {}
    # Target Ratings
    ft = ft_helper(code, exchange_code=get_exchange_code_ft(code, country))
    ratingsRe = re.compile(".*[Rr]ecommendation.*")
    for forecastType in ft:
        m = re.match(ratingsRe, forecastType)
        if not m:
            continue
        for title in ft[forecastType]:
            titleMatch = re.match("[Ll]atest", title)
            if not titleMatch:
                continue
            temp["buyAnalysts"] = int(ft[forecastType][title]["Buy"])
            temp["outperformAnalysts"] = int(ft[forecastType][title]["Outperform"])
            temp["holdAnalysts"] = int(ft[forecastType][title]["Hold"])
            temp["underperformAnalysts"] = int(ft[forecastType][title]["Underperform"])
            temp["sellAnalysts"] = int(ft[forecastType][title]["Sell"])
            break
    # Target Prices
    wsj = wsj_helper(code, exchange_code=get_exchange_code_wsj(code, country))
    temp["targetHigh"] = try_float(wsj["High"].replace("$", "").replace("¥", ""))
    temp["targetLow"] = try_float(wsj["Low"].replace("$", "").replace("¥", ""))
    temp["targetMedian"] = try_float(wsj["Median"].replace("$", "").replace("¥", ""))
    temp["target"] = try_float(wsj["Average"].replace("$", "").replace("¥", ""))
    # Defaults
    defaultKeys = ("buyAnalysts", "outperformAnalysts", "holdAnalysts", "underperformAnalysts", "sellAnalysts")
    defaultValues = tuple([0]) * 5
    for (key, value) in zip(defaultKeys, defaultValues):
        if key not in temp:
            temp[key] = value
    # Combined
    return Ratings(buy_analysts=temp["buyAnalysts"], outperform_analysts=temp["outperformAnalysts"], hold_analysts=temp["holdAnalysts"], underperform_analysts=temp["underperformAnalysts"], sell_analysts=temp["sellAnalysts"], target_high=temp["targetHigh"], target_low=temp["targetLow"], target_median=temp["targetMedian"], target=temp["target"])

