#!/usr/bin/env python

import sys
import os
from datetime import datetime
from optparse import OptionParser
import configparser
import queue

import batch
import records
import worker

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--workers", dest="workers", type="int", help="Number of Processes", default=50)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

data_whitelist = [ int(x) for x in configParser.get("Main", "data_whitelist").split(",") ]
index_whitelist = configParser.get("Main", "index_whitelist").split(",")

try:
    os.makedirs(options.directory)
except OSError:
    pass

# Exchange Rates

exchange_rates_file = configParser.get("Main", "exchange_rates_file")

try:
    records.create_records(os.path.join(options.directory, exchange_rates_file), ("code", "timestamp", "px_last"))
except:
    pass

try:
    rates = batch.get_exchange_rates(options.date)
    if rates:
        timestamp = int(datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp())
        recs = [ (currency, timestamp, rates[currency]) for currency in rates.keys() ]
        records.insert_records(os.path.join(options.directory, exchange_rates_file), records=recs)
    print("Exchange Rates Downloaded.", file=sys.stderr)
except:
    print("Exchange Rates not available for %s." % options.date, file=sys.stderr)

# Prices

prices_folder = configParser.get("Main", "prices_folder")

try:
    os.makedirs(os.path.join(options.directory, prices_folder))
except OSError:
    pass

class PriceTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            intraday_data = batch.get_intraday_data(self.code)
            return (self.code, intraday_data)
        except:
            return (self.code, None)

prices = {}

try:
    pricePool = worker.WorkerPool(options.workers)
    pricePool.start()
    count = 0
    for code in data_whitelist:
        pricePool.put(PriceTask(code))
    while count < len(data_whitelist):
        try:
            result = pricePool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, intraday_data) = result
            if not intraday_data:
                pricePool.put(PriceTask(code))
                print("Retrying prices for %d ..." % code, file=sys.stderr)
            else:
                prices[code] = intraday_data
                count += 1
                print("Downloaded prices for %d." % code, file=sys.stderr)
except KeyboardInterrupt:
    print("Price Download Interrupted.", file=sys.stderr)
finally:
    pricePool.terminate()

checkDate = datetime.strptime(options.date, "%Y%m%d").date()
for code in data_whitelist:
    toDelete = []
    for timestamp in prices[code].keys():
        if datetime.fromtimestamp(timestamp).date() != checkDate:
            toDelete += [timestamp]
    for t in toDelete:
        del prices[code][t]

toDelete = []
for code in data_whitelist:
    if not prices[code]:
        toDelete += [code]
for c in toDelete:
    del prices[c]

if prices:
    with open(os.path.join(options.directory, prices_folder, options.date), 'w') as f:
        print("code,timestamp,open,high,low,close,volume", file=f)
        for code in data_whitelist:
            if code in prices:
                for timestamp in sorted(prices[code].keys()):
                    print("%04d,%d,%f,%f,%f,%f,%f" % (code, timestamp, prices[code][timestamp]["open"], prices[code][timestamp]["high"], prices[code][timestamp]["low"], prices[code][timestamp]["close"], prices[code][timestamp]["volume"]), file=f)
else:
    print("No Prices for %s. Skipping file generation." % options.date, file=sys.stderr)

del pricePool
del prices

print("Prices Downloaded.", file=sys.stderr)

"""
# Indices

indices_folder = configParser.get("Main", "indices_folder")

try:
    os.makedirs(os.path.join(options.directory, indices_folder))
except OSError:
    pass

class IndexTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            index_data = batch.get_intraday_data_index(self.code)
            return (self.code, index_data)
        except:
            return (self.code, None)

indices = {}

try:
    indexPool = worker.WorkerPool(options.workers)
    indexPool.start()
    count = 0
    for code in index_whitelist:
        indexPool.put(IndexTask(code))
    while count < len(index_whitelist):
        try:
            result = indexPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, index_data) = result
            if index_data is None:
                indexPool.put(IndexTask(code))
            else:
                indices[code] = index_data
                count += 1
except KeyboardInterrupt:
    print("Index Download Interrupted.", file=sys.stderr)
finally:
    indexPool.terminate()

toDelete = []
for c in indices:
    if not indices[c]:
        toDelete += [c]

for c in toDelete:
    del indices[c]

if indices:
    with open(os.path.join(options.directory, indices_folder, options.date), 'w') as f:
        print("code,timestamp,open,high,low,close,volume", file=f)
        for code in index_whitelist:
            if code in indices:
                for timestamp in sorted(indices[code].keys()):
                    print("%s,%d,%f,%f,%f,%f,%f" % (code, timestamp, indices[code][timestamp]["open"], indices[code][timestamp]["high"], indices[code][timestamp]["low"], indices[code][timestamp]["close"], indices[code][timestamp]["volume"]), file=f)
else:
    print("No Index Data for %s. Skipping file generation." % options.date, file=sys.stderr)

del indexPool
del indices

print("Indices Downloaded.", file=sys.stderr)
"""

# Broker Activity

broker_activity_folder = configParser.get("Main", "broker_activity_folder")

try:
    os.makedirs(os.path.join(options.directory, broker_activity_folder))
except OSError:
    pass

class BrokerActivityTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            broker_activity_data = batch.get_broker_activity(self.code)
            return (self.code, broker_activity_data)
        except:
            return (self.code, None)

broker_activities = {}

try:
    brokerActivityPool = worker.WorkerPool(options.workers)
    brokerActivityPool.start()
    count = 0
    for code in data_whitelist:
        brokerActivityPool.put(BrokerActivityTask(code))
    while count < len(data_whitelist):
        try:
            result = brokerActivityPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, broker_activity_data) = result
            if broker_activity_data is None:
                brokerActivityPool.put(BrokerActivityTask(code))
                print("Retrying broker activity for %d ..." % code, file=sys.stderr)
            else:
                broker_activities[code] = broker_activity_data
                count += 1
                print("Downloaded broker activity for %d." % code, file=sys.stderr)
except KeyboardInterrupt:
    print("Broker Activity Download Interrupted.", file=sys.stderr)
finally:
    brokerActivityPool.terminate()

toDelete = []
for code in data_whitelist:
    if not broker_activities[code]:
        toDelete += [code]
for c in toDelete:
    del broker_activities[c]

if not os.path.exists(os.path.join(options.directory, broker_activity_folder, options.date)):
    records.create_records(os.path.join(options.directory, broker_activity_folder, options.date), ["code", "timestamp"] + list(batch.BrokerActivity._fields))

brokerActivityRecords = []
if broker_activities:
    timestamp = int(datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp())
    for code in broker_activities:
        for broker in broker_activities[code]:
            brokerActivityRecords += [ tuple(["%d|%s" % (code, broker), timestamp]) + broker_activities[code][broker] ]
records.insert_records(os.path.join(options.directory, broker_activity_folder, options.date), brokerActivityRecords)

del brokerActivityPool
del broker_activities
del brokerActivityRecords

print("Broker Activity Downloaded.", file=sys.stderr)

# China Commodity Futures

china_commodity_futures_file = configParser.get("Main", "china_commodity_futures_file")

if not os.path.exists(os.path.join(options.directory, china_commodity_futures_file)):
    records.create_records(os.path.join(options.directory, china_commodity_futures_file), ["code", "timestamp"] + list(batch.CommodityFutures._fields))

try:
    china_commodity_futures = batch.get_china_commodity_futures_data(options.date)
    recs = []
    timestamp = int(datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp())
    for code in china_commodity_futures:
        recs += [ (code, timestamp) + batch.CommodityFutures(px_open=china_commodity_futures[code]["open"], px_high=china_commodity_futures[code]["high"], px_low=china_commodity_futures[code]["low"], px_last=china_commodity_futures[code]["last"], px_volume=china_commodity_futures[code]["volume"], px_turnover=china_commodity_futures[code]["turnover"], px_settlement=china_commodity_futures[code]["settlement"], open_interest=china_commodity_futures[code]["oi"]) ]
    records.insert_records(os.path.join(options.directory, china_commodity_futures_file), records=recs)
    del china_commodity_futures
except:
    print("No China Commodity Futures Data for %s." % options.date, file=sys.stderr)

print("China Commodity Futures Data Downloaded.", file=sys.stderr)

"""
# China Bulk Commodities

china_bulk_commodities_file = configParser.get("Main", "china_bulk_commodities_file")

if not os.path.exists(os.path.join(options.directory, china_bulk_commodities_file)):
    records.create_records(os.path.join(options.directory, china_bulk_commodities_file), ["code", "timestamp"] + list(batch.BulkCommodity._fields))

try:
    china_bulk_commodities = batch.get_china_bulk_commodities_data(options.date, timeout=360)
    recs = []
    for code in sorted(china_bulk_commodities):
        for timestamp in sorted(china_bulk_commodities[code]):
            recs += [ (code, timestamp) + batch.BulkCommodity(px_last=china_bulk_commodities[code][timestamp]) ]
    records.insert_records(os.path.join(options.directory, china_bulk_commodities_file), records=recs)
    del china_bulk_commodities
except Exception as e:
    raise e
    print("No China Bulk Commodities Data for %s." % options.date, file=sys.stderr)

print("China Bulk Commodities Data Downloaded.", file=sys.stderr)
"""

# Short Selling

short_selling_file = configParser.get("Main", "short_selling_file")

if not os.path.exists(os.path.join(options.directory, short_selling_file)):
    records.create_records(os.path.join(options.directory, short_selling_file), ["code", "timestamp"] + list(batch.ShortSelling._fields))

shorts = batch.get_short_selling(options.date)
if shorts:
    recs = []
    timestamp = int(datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp())
    for code in data_whitelist:
        if code in shorts:
            recs += [ (code, timestamp) + shorts[code] ]
        else:
            recs += [ (code, timestamp) + (0, 0.0, 0, 0.0) ]
    records.insert_records(os.path.join(options.directory, short_selling_file), records=recs)

del shorts

print("Short Selling Data Downloaded.", file=sys.stderr)

# Industry

industry_file = configParser.get("Main", "industry_file")

class IndustryTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            industry_data = batch.get_industry(self.code)
            return (self.code, industry_data)
        except:
            return (self.code, None)

industries = {}

try:
    industriesPool = worker.WorkerPool(options.workers)
    industriesPool.start()
    count = 0
    for code in data_whitelist:
        industriesPool.put(IndustryTask(code))
    while count < len(data_whitelist):
        try:
            result = industriesPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, industry_data) = result
            if not industry_data:
                industriesPool.put(IndustryTask(code))
            else:
                industries[code] = industry_data
                count += 1
except KeyboardInterrupt:
    print("Industry Download Interrupted.", file=sys.stderr)
finally:
    industriesPool.terminate()

if not os.path.exists(os.path.join(options.directory, industry_file)):
    records.create_records(os.path.join(options.directory, industry_file), ["code", "timestamp"] + list(batch.Industry._fields))

records.insert_records(os.path.join(options.directory, industry_file), [tuple([code, datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp()]) + industries[code] for code in data_whitelist])

del industriesPool
del industries

print("Industries Downloaded.", file=sys.stderr)

# Number of Employees

employees_file = configParser.get("Main", "employees_file")

class EmployeesTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            employees_data = batch.get_employees(self.code)
            return (self.code, employees_data)
        except:
            return (self.code, None)

employees = {}

try:
    employeesPool = worker.WorkerPool(options.workers)
    employeesPool.start()
    count = 0
    for code in data_whitelist:
        employeesPool.put(EmployeesTask(code))
    while count < len(data_whitelist):
        try:
            result = employeesPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, employees_data) = result
            if not employees_data:
                employeesPool.put(EmployeesTask(code))
            else:
                employees[code] = employees_data
                count += 1
except KeyboardInterrupt:
    print("Employees Download Interrupted.", file=sys.stderr)
finally:
    employeesPool.terminate()

if not os.path.exists(os.path.join(options.directory, employees_file)):
    records.create_records(os.path.join(options.directory, employees_file), ["code", "timestamp", "employee_count"])

records.insert_records(os.path.join(options.directory, employees_file), [tuple([code, datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp(), employees[code]]) for code in data_whitelist])

del employeesPool
del employees

print("Employees Downloaded.", file=sys.stderr)

# Corporate Actions

corporate_actions_file = configParser.get("Main", "corporate_actions_file")

class CorporateActionsTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            corporate_actions_data = batch.get_corporate_actions(self.code)
            return (self.code, corporate_actions_data)
        except:
            return (self.code, None)

corporate_actions = {}

try:
    corporateActionsPool = worker.WorkerPool(options.workers)
    corporateActionsPool.start()
    count = 0
    for code in data_whitelist:
        corporateActionsPool.put(CorporateActionsTask(code))
    while count < len(data_whitelist):
        try:
            result = corporateActionsPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, corporate_actions_data) = result
            if corporate_actions_data is None:
                corporateActionsPool.put(CorporateActionsTask(code))
                print("Retrying corporate actions for %d ..." % code, file=sys.stderr)
            else:
                corporate_actions[code] = corporate_actions_data
                count += 1
                print("Downloaded corporate actions for %d." % code, file=sys.stderr)
except KeyboardInterrupt:
    print("Corporate Actions Download Interrupted.", file=sys.stderr)
finally:
    corporateActionsPool.terminate()

if not os.path.exists(os.path.join(options.directory, corporate_actions_file)):
    records.create_records(os.path.join(options.directory, corporate_actions_file), ["code"] + list(batch.CorporateAction._fields))

corporateActionsRecords = []
for code in data_whitelist:
    corporateActionsRecords += [ tuple([code]) + c for c in corporate_actions[code] ]
records.insert_records(os.path.join(options.directory, corporate_actions_file), corporateActionsRecords)

del corporateActionsPool
del corporate_actions
del corporateActionsRecords

print("Corporate Actions Downloaded.", file=sys.stderr)

# Issued Shares

shares_file = configParser.get("Main", "shares_file")

class SharesTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            shares_data = batch.get_issued_shares(self.code)
            return (self.code, shares_data)
        except:
            return (self.code, None)

shares = {}

try:
    sharesPool = worker.WorkerPool(options.workers)
    sharesPool.start()
    count = 0
    for code in data_whitelist:
        sharesPool.put(SharesTask(code))
    while count < len(data_whitelist):
        try:
            result = sharesPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, shares_data) = result
            if not shares_data:
                sharesPool.put(SharesTask(code))
            else:
                shares[code] = shares_data
                count += 1
except KeyboardInterrupt:
    print("Shares Download Interrupted.", file=sys.stderr)
finally:
    sharesPool.terminate()

if not os.path.exists(os.path.join(options.directory, shares_file)):
    records.create_records(os.path.join(options.directory, shares_file), ["code", "timestamp"] + list(batch.IssuedShares._fields))

records.insert_records(os.path.join(options.directory, shares_file), [tuple([code, datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp()]) + shares[code] for code in data_whitelist])

del sharesPool
del shares

print("Shares Downloaded.", file=sys.stderr)

# Institutional Shares

institutions_file = configParser.get("Main", "institutions_file")

class InstitutionsTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            institutions_data = batch.get_institutional_shares(self.code)
            return (self.code, institutions_data)
        except:
            return (self.code, None)

institutions = {}

try:
    institutionsPool = worker.WorkerPool(options.workers)
    institutionsPool.start()
    count = 0
    for code in data_whitelist:
        institutionsPool.put(InstitutionsTask(code))
    while count < len(data_whitelist):
        try:
            result = institutionsPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, institutions_data) = result
            if not institutions_data:
                institutionsPool.put(InstitutionsTask(code))
            else:
                institutions[code] = institutions_data
                count += 1
except KeyboardInterrupt:
    print("Institutional Shares Download Interrupted.", file=sys.stderr)
finally:
    institutionsPool.terminate()

if not os.path.exists(os.path.join(options.directory, institutions_file)):
    records.create_records(os.path.join(options.directory, institutions_file), ["code", "timestamp"] + list(batch.InstitutionalShares._fields))

records.insert_records(os.path.join(options.directory, institutions_file), [tuple([code, datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp()]) + institutions[code] for code in data_whitelist])

del institutionsPool
del institutions

print("Institutional Shares Downloaded.", file=sys.stderr)

# (Actual) Fundamentals

fundamentals_file = configParser.get("Main", "fundamentals_file")

class FundamentalsTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            fundamentals_data = batch.get_fundamentals(self.code)
            return (self.code, fundamentals_data)
        except:
            return (self.code, None)

fundamentals = {}

try:
    fundamentalsPool = worker.WorkerPool(options.workers)
    fundamentalsPool.start()
    count = 0
    for code in data_whitelist:
        fundamentalsPool.put(FundamentalsTask(code))
    while count < len(data_whitelist):
        try:
            result = fundamentalsPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, fundamentals_data) = result
            if fundamentals_data is None:  #
                fundamentalsPool.put(FundamentalsTask(code))
                print("Retrying fundamentals for %d ..." % code, file=sys.stderr)
            else:
                for key in fundamentals_data:
                    fundamentals[key] = fundamentals_data[key]
                count += 1
                print("Downloaded fundamentals for %d." % code, file=sys.stderr)
except KeyboardInterrupt:
    print("Fundamentals Download Interrupted.", file=sys.stderr)
finally:
    fundamentalsPool.terminate()

if not os.path.exists(os.path.join(options.directory, fundamentals_file)):
    records.create_records(os.path.join(options.directory, fundamentals_file), ["key", "timestamp"] + list(batch.Fundamentals._fields))

records.insert_records(os.path.join(options.directory, fundamentals_file), [ tuple(["|".join(map(str, key)), datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp()]) + fundamentals[key] for key in fundamentals])

del fundamentalsPool
del fundamentals

print("Fundamentals Downloaded.", file=sys.stderr)

# (Actual) Fundamentals - LTM

fundamentals_ltm_file = configParser.get("Main", "fundamentals_ltm_file")

class FundamentalsLtmTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            fundamentals_ltm_data = batch.get_fundamentals_ltm(self.code)
            return (self.code, fundamentals_ltm_data)
        except:
            return (self.code, None)

fundamentals_ltm = {}

try:
    fundamentalsLtmPool = worker.WorkerPool(options.workers)
    fundamentalsLtmPool.start()
    count = 0
    for code in data_whitelist:
        fundamentalsLtmPool.put(FundamentalsLtmTask(code))
    while count < len(data_whitelist):
        try:
            result = fundamentalsLtmPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, fundamentals_ltm_data) = result
            if fundamentals_ltm_data is None:  #
                fundamentalsLtmPool.put(FundamentalsLtmTask(code))
                print("Retrying fundamentals - LTM for %d ..." % code, file=sys.stderr)
            else:
                for key in fundamentals_ltm_data:
                    fundamentals_ltm[key] = fundamentals_ltm_data[key]
                count += 1
                print("Downloaded fundamentals - LTM for %d." % code, file=sys.stderr)
except KeyboardInterrupt:
    print("Fundamentals - LTM Download Interrupted.", file=sys.stderr)
finally:
    fundamentalsLtmPool.terminate()

if not os.path.exists(os.path.join(options.directory, fundamentals_ltm_file)):
    records.create_records(os.path.join(options.directory, fundamentals_ltm_file), ["key", "timestamp"] + list(batch.Fundamentals._fields))

records.insert_records(os.path.join(options.directory, fundamentals_ltm_file), [ tuple(["|".join(map(str, key)), datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp()]) + fundamentals_ltm[key] for key in fundamentals_ltm])

del fundamentalsLtmPool
del fundamentals_ltm

print("Fundamentals - LTM Downloaded.", file=sys.stderr)

# Forecasted Fundamentals

forecasts_file = configParser.get("Main", "forecasts_file")

class ForecastsTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            forecasts_data = batch.get_forecasted_fundamentals(self.code)
            return (self.code, forecasts_data)
        except:
            return (self.code, None)

forecasts = {}

try:
    forecastsPool = worker.WorkerPool(options.workers)
    forecastsPool.start()
    count = 0
    for code in data_whitelist:
        forecastsPool.put(ForecastsTask(code))
    while count < len(data_whitelist):
        try:
            result = forecastsPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, forecasts_data) = result
            if forecasts_data is None:  #
                forecastsPool.put(ForecastsTask(code))
                print("Retrying forecasts for %d ..." % code, file=sys.stderr)
            else:
                for key in forecasts_data:
                    forecasts[key] = forecasts_data[key]
                count += 1
                print("Downloaded forecasts for %d." % code, file=sys.stderr)
except KeyboardInterrupt:
    print("Forecasts Download Interrupted.", file=sys.stderr)
finally:
    forecastsPool.terminate()

if not os.path.exists(os.path.join(options.directory, forecasts_file)):
    records.create_records(os.path.join(options.directory, forecasts_file), ["key", "timestamp"] + list(batch.ForecastedFundamentals._fields))

records.insert_records(os.path.join(options.directory, forecasts_file), [ tuple(["|".join(map(str, key)), datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp()]) + forecasts[key] for key in forecasts])

del forecastsPool
del forecasts

print("Forecasted Fundamentals Downloaded.", file=sys.stderr)

# Ratings

ratings_file = configParser.get("Main", "ratings_file")

class RatingsTask:
    def __init__(self, code):
        self.code = code
    def __call__(self):
        try:
            ratings_data = batch.get_ratings(self.code)
            return (self.code, ratings_data)
        except:
            return (self.code, None)

ratings = {}

try:
    ratingsPool = worker.WorkerPool(options.workers)
    ratingsPool.start()
    count = 0
    for code in data_whitelist:
        ratingsPool.put(RatingsTask(code))
    while count < len(data_whitelist):
        try:
            result = ratingsPool.get(timeout=1)
        except queue.Empty:
            result = None
        if result:
            (code, ratings_data) = result
            if not ratings_data:
                ratingsPool.put(RatingsTask(code))
                print("Retrying ratings for %d ..." % code, file=sys.stderr)
            else:
                ratings[code] = ratings_data
                count += 1
                print("Downloaded ratings for %d." % code, file=sys.stderr)
except KeyboardInterrupt:
    print("Ratings Download Interrupted.", file=sys.stderr)
finally:
    ratingsPool.terminate()

if not os.path.exists(os.path.join(options.directory, ratings_file)):
    records.create_records(os.path.join(options.directory, ratings_file), ["code", "timestamp"] + list(batch.Ratings._fields))

records.insert_records(os.path.join(options.directory, ratings_file), [tuple([code, datetime.strptime(options.date + "2359+0800", "%Y%m%d%H%M%z").timestamp()]) + ratings[code] for code in data_whitelist])

del ratingsPool
del ratings

print("Ratings Downloaded.", file=sys.stderr)

# TODO: prepare end-of-day files for research programs to read from

# TODO: prepare end-of-day files for live programs to read from, before next day starts

