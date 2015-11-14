#!/usr/bin/env python

import sys
import os
import operator
from datetime import datetime
from optparse import OptionParser
import configparser
import math
import numpy
import sqlite3

import batch
import records

import bars, xs

parser = OptionParser()
parser.add_option("--directory", dest="directory", help="Directory to Store Data", default="data")
parser.add_option("--date", dest="date", help="Date (YYYYMMDD)", default=datetime.strftime(datetime.today(), "%Y%m%d"))
parser.add_option("--config", dest="config", help="Name of Configuration File", default=None)
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

# Codes

data_whitelist = [ int(x) for x in configParser.get("Main", "data_whitelist").split(",") ]

exclude_list = [ int(x) for x in configParser.get("Scan", "exclude").split(",") ]
data_whitelist = [ x for x in data_whitelist if x not in exclude_list ]

eligible_codes = data_whitelist
all_prices = {}
all_issued_shares = {}
all_exchange_rates = {}
all_ratings = {}
all_fundamentals_dates = {}
all_fundamentals = {}
all_forecasts = {}

# Industry

industry_file = configParser.get("Main", "industry_file")

exclude_industries = configParser.get("Scan", "exclude_industries")
exclude_sectors = configParser.get("Scan", "exclude_sectors")

toDelete = []
for code in eligible_codes:
    industry = get_latest_industry(os.path.join(options.directory, industry_file), code)
    sector = get_latest_sector(os.path.join(options.directory, industry_file), code)
    if industry in exclude_industries or sector in exclude_sectors:
        toDelete += [code]

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Filtered by Industry -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Ratings

ratings_file = configParser.get("Main", "ratings_file")

min_analysts_count = int(configParser.get("Scan", "min_analysts_count"))

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

# Turnover

prices_folder = configParser.get("Main", "prices_folder")

dates = sorted(os.listdir(os.path.join(options.directory, prices_folder)))
dates = [d for d in dates if d <= options.date]

min_turnover_days = int(configParser.get("Scan", "min_turnover_days"))
min_turnover = float(configParser.get("Scan", "min_turnover"))

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

print("Filtered by Avarage Turnover -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Issued Shares

shares_file = configParser.get("Main", "shares_file")

min_market_cap = float(configParser.get("Scan", "min_market_cap"))

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

# Exchange Rates

exchange_rates_file = configParser.get("Main", "exchange_rates_file")

all_exchange_rates = get_latest_exchange_rates(os.path.join(options.directory, exchange_rates_file))

# Fundamentals

fundamentals_file = configParser.get("Main", "fundamentals_ltm_file")  #

toDelete = []
for code in eligible_codes:
    fundamentals = get_latest_fundamentals(os.path.join(options.directory, fundamentals_file), code)
    if len(fundamentals) > 0:
        all_fundamentals_dates[code] = sorted(fundamentals)[-1]
        all_fundamentals[code] = fundamentals[all_fundamentals_dates[code]]
    else:
        toDelete += [code]

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Securities with Fundamentals -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Forecasts

forecasts_file = configParser.get("Main", "forecasts_file")

toDelete = []
for code in eligible_codes:
    forecasts = get_latest_forecasts(os.path.join(options.directory, forecasts_file), code)
    tmp = []
    for key in sorted(forecasts, key=lambda x: x.financial_year):
        if code in all_fundamentals_dates:
            if key.financial_year * 4 + financial_period_map[key.financial_period] <= all_fundamentals_dates[code].financial_year * 4 + financial_period_map[all_fundamentals_dates[code].financial_period]:
                continue
        tmp += [forecasts[key]]
    if len(tmp) > 0:
        all_forecasts[code] = tmp
    else:
        toDelete += [code]

eligible_codes = [ c for c in eligible_codes if c not in toDelete ]

print("Securities with Forecasts -", file=sys.stderr)
print(eligible_codes, file=sys.stderr)

# Statistics

connection = sqlite3.connect(":memory:")
cursor = connection.cursor()

# Create raw table

cursor.execute("""
                  CREATE TABLE raw_data
                  (
                    code INT NOT NULL,
                    px_last FLOAT DEFAULT NULL,
                    issued_shares FLOAT DEFAULT NULL,
                    exchange_rate FLOAT DEFAULT NULL,
                    cash FLOAT DEFAULT NULL,
                    current_assets FLOAT DEFAULT NULL,
                    ppe FLOAT DEFAULT NULL,
                    bank_debt FLOAT DEFAULT NULL,
                    current_liabilities FLOAT DEFAULT NULL,
                    long_term_debt FLOAT DEFAULT NULL,
                    minority_equity FLOAT DEFAULT NULL,
                    ebit FLOAT DEFAULT NULL,
                    net_income FLOAT DEFAULT NULL,
                    capex FLOAT DEFAULT NULL,
                    eps_forecast_1 FLOAT DEFAULT NULL,
                    eps_forecast_2 FLOAT DEFAULT NULL,
                    PRIMARY KEY (code)
                  );
               """)

cursor.execute("""
                  CREATE VIEW calculations AS
                  SELECT code,
                         px_last,
                         (max(current_assets - cash - current_liabilities + bank_debt, 0) + ppe) * exchange_rate AS net_operating_assets,
                         (bank_debt + long_term_debt + minority_equity) * exchange_rate + px_last * issued_shares AS ev,
                         cash * exchange_rate AS cash,
                         ebit * exchange_rate AS ebit,
                         eps_forecast_1 * issued_shares * ebit / net_income * exchange_rate AS ebit_forecast_1,
                         eps_forecast_2 * issued_shares * ebit / net_income * exchange_rate AS ebit_forecast_2,
                         capex * exchange_rate AS capex,
                         min(capex * (eps_forecast_1 * issued_shares * ebit / net_income) / ebit, 0) * exchange_rate AS capex_forecast_1,
                         min(capex * (eps_forecast_2 * issued_shares * ebit / net_income) / ebit, 0) * exchange_rate AS capex_forecast_2
                  FROM raw_data;
               """)

cursor.execute("""
                  CREATE VIEW ratios AS
                  SELECT code,
                         px_last,
                         (ebit / (ev - cash) + ebit_forecast_1 / (ev - cash) + ebit_forecast_2 / (ev - cash)) / 3 AS earnings_yield,
                         (ebit / net_operating_assets + ebit_forecast_1 / (net_operating_assets - capex_forecast_1) + ebit_forecast_2 / (net_operating_assets - capex_forecast_1 - capex_forecast_2)) / 3 AS return_on_capital,
                         1 - (ev - cash) / ((net_operating_assets + (net_operating_assets - capex_forecast_1) + (net_operating_assets - capex_forecast_1 - capex_forecast_2)) / 3) AS book_discount
                  FROM calculations;
               """)

# Construct individual rankings

all_ratios = []

cursor.execute("PRAGMA table_info(ratios);")

for row in cursor.fetchall():
    (index, column_name, column_type, v4, v5, v6) = row
    if column_type == "":
        all_ratios += [column_name]

cursor.execute("""
                  CREATE TABLE ranks
                  (
                    code INT NOT NULL,
                    %s,
                    PRIMARY KEY (code)
                  );
               """ % ", ".join(["%s_rank INT" % m for m in all_ratios]))

# Add eligible codes

cursor.executemany("INSERT INTO raw_data (code) VALUES (?);", [ (code, ) for code in eligible_codes])

cursor.executemany("INSERT INTO ranks (code) VALUES (?);", [ (code, ) for code in eligible_codes])

# Update with existing values

cursor.executemany(
                   """
                      UPDATE raw_data
                      SET px_last = ?,
                          issued_shares = ?,
                          exchange_rate = ?,
                          cash = ?,
                          current_assets = ?,
                          ppe = ?,
                          bank_debt = ?,
                          current_liabilities = ?,
                          long_term_debt = ?,
                          minority_equity = ?,
                          ebit = ?,
                          net_income = ?,
                          capex = ?,
                          eps_forecast_1 = ?,
                          eps_forecast_2 = ?
                      WHERE code = ?;
                   """,
                   [
                    (
                     all_prices[code].px_last,
                     all_issued_shares[code],
                     all_exchange_rates[all_fundamentals[code].currency],
                     all_fundamentals[code].cash,
                     all_fundamentals[code].current_assets,
                     all_fundamentals[code].ppe,
                     all_fundamentals[code].bank_debt,
                     all_fundamentals[code].current_liabilities,
                     all_fundamentals[code].long_term_debt,
                     all_fundamentals[code].minority_equity,
                     all_fundamentals[code].ebit,
                     all_fundamentals[code].net_income,
                     all_fundamentals[code].capex,
                     all_forecasts[code][0].eps if len(all_forecasts[code]) >= 1 and not math.isnan(all_forecasts[code][0].eps) else 0,
                     all_forecasts[code][1].eps if len(all_forecasts[code]) >= 2 and not math.isnan(all_forecasts[code][1].eps) else 0,
                     code
                    ) for code in eligible_codes
                   ]
                  )

# Populate results

for ratio in all_ratios:
    cursor.execute("SELECT * FROM ratios ORDER BY %s DESC;" % ratio)
    res = cursor.fetchall()
    cursor.executemany("""
                          UPDATE ranks
                          SET %s_rank = ?
                          WHERE code = ?;
                       """ % ratio, [ (i + 1, res[i][0]) for i in range(0, len(res)) ])
#    cursor.execute("""
#                      UPDATE ranks
#                      SET %(ratio)s_rank = (
#                                            SELECT the_rank
#                                            FROM (
#                                                  SELECT r1.code AS the_code, count(r2.%(ratio)s) AS the_rank
#                                                  FROM ratios AS r1, ratios AS r2
#                                                  WHERE 
#                                                        r1.%(ratio)s < r2.%(ratio)s 
#                                                        OR
#                                                        (
#                                                         r1.%(ratio)s = r2.%(ratio)s
#                                                         AND
#                                                         r1.code = r2.code
#                                                        )
#                                                  GROUP BY r1.code, r1.%(ratio)s
#                                                 )
#                                            WHERE the_code = code
#                                           );
#                   """ % { "ratio" : ratio })

cursor.execute("""
                  SELECT ratios.code, %(all_ratios)s, %(all_ranks)s
                  FROM 
                       ratios
                       INNER JOIN
                       ranks
                       ON ratios.code = ranks.code
                  ORDER BY %(combined_rank)s;
               """ % { "all_ratios" : ", ".join(["ratios.%s" % ratio for ratio in all_ratios]), "all_ranks" : ", ".join(["ranks.%s_rank" % ratio for ratio in all_ratios]), "combined_rank" : " + ".join(["%s_rank" % ratio for ratio in all_ratios]) })

for row in cursor.fetchall():
    print(row, file=sys.stderr)

