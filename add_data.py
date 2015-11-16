#!/usr/bin/env python

import sys
import os
import operator
import datetime as dt
from datetime import datetime
from optparse import OptionParser
import configparser
import numpy
import pandas

import batch
import records

import bars, xs

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
parser.add_option("--experiment", dest="expt", help="Name of Experiment", default=None)
(options, args) = parser.parse_args()

configParser = configparser.ConfigParser()
configParser.read(options.config)

ref_date = datetime.strptime(options.date + "2359", "%Y%m%d%H%M")

# Utility Functions

financial_period_map = { "1stQuarterly" : 0, "Interim" : 1, "3rdQuarterly" : 2, "Final" : 3 }

def get_latest_industry(f, code):
    recs = records.get_records(f, str(code))
    recs = [ r for r in recs if datetime.fromtimestamp(r[1]) <= ref_date ]
    if len(recs) < 1:
        return None
    return recs[-1][2]

def get_latest_sector(f, code):
    recs = records.get_records(f, str(code))
    recs = [ r for r in recs if datetime.fromtimestamp(r[1]) <= ref_date ]
    if len(recs) < 1:
        return None
    return recs[-1][3]

def get_latest_ratings(f, code):
    recs = records.get_records(f, str(code))
    recs = [ r for r in recs if datetime.fromtimestamp(r[1]) <= ref_date ]
    if len(recs) < 1:
        return None
    ratings = batch.Ratings(buy_analysts=int(recs[-1][2]), outperform_analysts=int(recs[-1][3]), hold_analysts=int(recs[-1][4]), underperform_analysts=int(recs[-1][5]), sell_analysts=int(recs[-1][6]), target_high=float(recs[-1][7]), target_low=float(recs[-1][8]), target_median=float(recs[-1][9]), target=float(recs[-1][10]))
    return ratings

def get_latest_forecasts(f, code):
    recs = records.get_records(f, str(code))
    recs = [ r for r in recs if datetime.fromtimestamp(r[1]) <= ref_date ]
    forecasts = {}
    if len(recs) < 1:
        return forecasts
    for r in recs:
        raw_key = r[0].split("|")
        key = batch.CompoundKey(code=int(raw_key[0]), financial_year=int(raw_key[1]), financial_period=raw_key[2])
        if key not in forecasts:
            forecasts[key] = batch.ForecastedFundamentals(revenue_analysts=int(r[2]), revenue_high=float(r[3]), revenue_low=float(r[4]), revenue=float(r[5]), eps_analysts=int(r[6]), eps_high=float(r[7]), eps_low=float(r[8]), eps=float(r[9]), dps=float(r[10]))
    return forecasts

def get_latest_fundamentals(f, code):
    recs = records.get_records(f, str(code))
    recs = [ r for r in recs if datetime.fromtimestamp(r[1]) <= ref_date ]
    fundamentals = {}
    if len(recs) < 1:
        return {}  #
    for r in recs:
        raw_key = r[0].split("|")
        key = batch.CompoundKey(code=int(raw_key[0]), financial_year=int(raw_key[1]), financial_period=raw_key[2])
        if key not in fundamentals:
            fundamentals[key] = batch.Fundamentals(financial_period_start=recs[-1][2], financial_period_end=recs[-1][3], reporting_date=recs[-1][4], currency=recs[-1][5], extraordinary_income=float(recs[-1][6]), eps=float(recs[-1][7]), diluted_eps=float(recs[-1][8]), dps=float(recs[-1][9]), sdps=float(recs[-1][10]), cash=float(recs[-1][11]), accounts_receivable=float(recs[-1][12]), inventory=float(recs[-1][13]), current_assets=float(recs[-1][14]), loans=float(recs[-1][15]), ppe=float(recs[-1][16]), intangibles=float(recs[-1][17]), associates=float(recs[-1][18]), noncurrent_assets=float(recs[-1][19]), total_assets=float(recs[-1][20]), accounts_payable=float(recs[-1][21]), bank_debt=float(recs[-1][22]), current_liabilities=float(recs[-1][23]), long_term_debt=float(recs[-1][24]), deposits=float(recs[-1][25]), total_liabilities=float(recs[-1][26]), minority_equity=float(recs[-1][27]), share_capital=float(recs[-1][28]), reserve=float(recs[-1][29]), total_equity=float(recs[-1][30]), interest_revenue=float(recs[-1][31]), interest_payout=float(recs[-1][32]), net_interest_revenue=float(recs[-1][33]), other_revenue=float(recs[-1][34]), revenue=float(recs[-1][35]), cogs=float(recs[-1][36]), gross_profit=float(recs[-1][37]), sales_expense=float(recs[-1][38]), general_expense=float(recs[-1][39]), credit_provisions=float(recs[-1][40]), depreciation=float(recs[-1][41]), operating_income=float(recs[-1][42]), associates_operating_income=float(recs[-1][43]), ebit=float(recs[-1][44]), interest_expense=float(recs[-1][45]), ebt=float(recs[-1][46]), tax=float(recs[-1][47]), minority_interest=float(recs[-1][48]), net_income=float(recs[-1][49]), dividend=float(recs[-1][50]), retained_earnings=float(recs[-1][51]), operating_cash_flow=float(recs[-1][52]), capex=float(recs[-1][53]), investing_cash_flow=float(recs[-1][54]), financing_cash_flow=float(recs[-1][55]), fx_translation=float(recs[-1][56]))
    res = {}
    latest_period = sorted(fundamentals, key=lambda x: x.financial_year * 4 + financial_period_map[x.financial_period])[-1]
    res[latest_period] = fundamentals[latest_period]
    return res

def get_average_turnover(bars):
    total_turnover = 0
    for bar in bars:
        total_turnover += bar.px_last * bar.px_volume
    return total_turnover / len(bars)

def get_latest_shares(f, code):
    recs = records.get_records(f, str(code))
    recs = [ r for r in recs if datetime.fromtimestamp(r[1]) <= ref_date ]
    if len(recs) < 1:
        return 0
    return float(recs[-1][2])

def get_latest_exchange_rates(f):
    currencies = ["RMB", "USD"]
    res = { "HKD" : 1 }
    for currency in currencies:
        recs = records.get_records(f, currency)
        res[currency] = float(recs[-1][2])
    return res

def get_corporate_action_occurrence(f, code, start_date, end_date):
    recs = records.get_records(f, str(code))
    recs = [ r for r in recs if datetime.strftime(datetime.fromtimestamp(r[1]), "%Y%m%d") >= start_date and datetime.strftime(datetime.fromtimestamp(r[1]), "%Y%m%d") <= end_date ]
    return len(recs) > 0

# Codes

all_codes = [ int(x) for x in configParser.get("Main", "data_whitelist").split(",") ]

data_whitelist = [ int(x) for x in configParser.get(options.expt, "codes").split(",") ]
data_whitelist = sorted([ x for x in data_whitelist if x in all_codes ])

exclude_list = [ int(x) for x in configParser.get("Sample", "exclude").split(",") ]
data_whitelist = [ x for x in data_whitelist if x not in exclude_list ]

eligible_codes = data_whitelist
all_prices = {}
all_industries = {}
all_sectors = {}
all_issued_shares = {}
all_exchange_rates = {}
all_ratings = {}

# Industry

industry_file = configParser.get("Main", "industry_file")

exclude_industries = configParser.get("Sample", "exclude_industries").split(",")
exclude_sectors = configParser.get("Sample", "exclude_sectors").split(",")

toDelete = []
for code in eligible_codes:
    industry = get_latest_industry(os.path.join(options.directory, industry_file), code)
    sector = get_latest_sector(os.path.join(options.directory, industry_file), code)
    if industry in exclude_industries or sector in exclude_sectors:
        toDelete += [code]
    else:
        all_industries[code] = industry
        all_sectors[code] = sector

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Filtered by Industry -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Make a duplicate list for prices

price_whitelist = [ x for x in data_whitelist ]

print("Price List -", file=sys.stderr)
print(price_whitelist, file=sys.stderr)

# Turnover / Missing Data

prices_folder = configParser.get("Main", "prices_folder")

dates = sorted(os.listdir(os.path.join(options.directory, prices_folder)))
dates = [d for d in dates if d <= options.date]

all_dates = dates

min_turnover_days = int(configParser.get("Sample", "min_turnover_days"))
min_turnover = float(configParser.get("Sample", "min_turnover"))

dates = dates[-min_turnover_days:]

foos = {}

toDelete = []

for d in dates:
    foo = bars.read_all_bars(os.path.join(options.directory, prices_folder, d))
    for code in foo:
        if code not in price_whitelist:
            continue
        if code not in foos:
            foos[code] = []
        foos[code] += foo[code]
    for code in price_whitelist:
        if code not in foo:
            if code not in toDelete:
                toDelete += [code]

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Filtered for Missing Data -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

grouped_bars = {}

for code in foos:
    grouped_bars[code] = bars.group_bars(foos[code], timeframe=bars.Timeframe.DAILY)

toDelete = []
for code in grouped_bars:
    average_turnover = get_average_turnover(grouped_bars[code])
    if average_turnover < min_turnover:
        toDelete += [code]
    else:
        all_prices[code] = grouped_bars[code][-1]

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

# Ratings

ratings_file = configParser.get("Main", "ratings_file")

min_analysts_count = int(configParser.get("Sample", "min_analysts_count"))

toDelete = []
for code in eligible_codes:
    ratings = get_latest_ratings(os.path.join(options.directory, ratings_file), code)
    analysts_count = (ratings.buy_analysts + ratings.outperform_analysts + ratings.hold_analysts + ratings.underperform_analysts + ratings.sell_analysts) if ratings is not None else 0
    if analysts_count < min_analysts_count:
        toDelete += [code]
    else:
        all_ratings[code] = ratings

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Filtered by No. of Analysts -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Issued Shares

shares_file = configParser.get("Main", "shares_file")

min_market_cap = float(configParser.get("Sample", "min_market_cap"))

toDelete = []
for code in eligible_codes:
    shares = get_latest_shares(os.path.join(options.directory, shares_file), code)
    market_cap = all_prices[code].px_last * shares
    if market_cap < min_market_cap:
        toDelete += [code]
    else:
        all_issued_shares[code] = shares

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Filtered by Market Cap -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Corporate Actions

corporate_actions_file = configParser.get("Main", "corporate_actions_file")

toDelete = []
for code in eligible_codes:
    has_cacs = get_corporate_action_occurrence(os.path.join(options.directory, corporate_actions_file), code, dates[0], dates[-1])
    if has_cacs:
        toDelete += [code]

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Filtered by Corporate Actions -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Finalized list here

print("Finalized List -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Calculate volatilities

all_volatilities = {}

max_log_volatility_diff = float(configParser.get("Sample", "max_log_volatility_diff"))

for code in eligible_codes:
    try:  # May not be able to calculate volatility
        all_volatilities[code] = xs.get_volatility(grouped_bars[code])
    except:
        continue

eligible_pairs = []

for code1 in eligible_codes:
    for code2 in eligible_codes:
        if code1 >= code2:
            continue
        if all_industries[code1] != all_industries[code2] or all_sectors[code1] != all_sectors[code2]:
            continue
        try:
            if numpy.fabs(numpy.log(all_volatilities[code1]) - numpy.log(all_volatilities[code2])) / 2 <= max_log_volatility_diff:
                eligible_pairs += [(code1, code2)]
        except:
           continue

print("Eligible Pairs -", file=sys.stderr)
print(eligible_pairs, file=sys.stderr)

# Clean Auction and Market Open Ticks
# Fill back morning/afternoon Ticks

auction_time = datetime.strptime(bars.market_auction, "%H:%M:%S%z")
market_am_open_time = datetime.strptime(bars.market_am_open, "%H:%M:%S%z")
market_pm_open_time = datetime.strptime(bars.market_pm_open, "%H:%M:%S%z")
market_am_close_time = datetime.strptime(bars.market_am_close, "%H:%M:%S%z")
market_pm_close_time = datetime.strptime(bars.market_pm_close, "%H:%M:%S%z")

for code in price_whitelist:
    if code not in foos:
        continue
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

experiment_folder = configParser.get(options.expt, "experiment_folder")

try:
    os.makedirs(os.path.join(options.directory, experiment_folder))
except OSError:
    pass

today_timestamp = datetime.combine(ref_date.date(), dt.time(0, 0)).timestamp()

hdf = pandas.HDFStore(os.path.join(options.directory, experiment_folder, "data.h5"))

if "price" not in hdf:
    price_df = pandas.DataFrame(columns=tuple(['code'] + list(bars.Bar._fields)), dtype=numpy.float64)
else:
    price_df = hdf["price"][hdf["price"]["timestamp"] < today_timestamp]

for code in price_whitelist:
    if code not in foos:
        continue
    tmp_prices = [ b for b in foos[code] if datetime.fromtimestamp(b.timestamp).date() == ref_date.date() ]
    if len(tmp_prices) == 0:
        continue
    tmp_df = pandas.DataFrame(tmp_prices, columns=bars.Bar._fields, dtype=numpy.float64)
    tmp_df['code'] = pandas.Series(numpy.ones(tmp_df.shape[0]) * code)
    price_df = price_df.append(tmp_df, ignore_index=True)

hdf.put("price", price_df, format="table", data_columns=True)

if "eligible" not in hdf:
    eligible_df = pandas.DataFrame(columns=("timestamp", "code"), dtype=numpy.float64)
else:
    eligible_df = hdf["eligible"][hdf["eligible"]["timestamp"] < today_timestamp]

for code in eligible_codes:
    tmp_df = pandas.DataFrame([[today_timestamp, code]], columns=("timestamp", "code"))
    eligible_df = eligible_df.append(tmp_df, ignore_index=True)
for pair in eligible_pairs:
    tmp_df = pandas.DataFrame([[today_timestamp, pair[0] * 100000 + pair[1]]], columns=("timestamp", "code"))
    eligible_df = eligible_df.append(tmp_df, ignore_index=True)

hdf.put("eligible", eligible_df, format="table", data_columns=True)

hdf.close()

print("Daily Data added to %s." % os.path.join(options.directory, experiment_folder, "data.h5"), file=sys.stderr)

