#!/bin/env python3

# Note: Hardcoded information present

import sys
import os
from enum import Enum
import numpy
import scipy.stats
import theano.tensor as T
from theano import function, Param

import bars

# Shorthands

def get_volatility(bars):
    bar_os = numpy.log(numpy.array([bar.px_open for bar in bars]))
    bar_us = numpy.log(numpy.array([bar.px_high for bar in bars]))
    bar_ds = numpy.log(numpy.array([bar.px_low for bar in bars]))
    bar_cs = numpy.log(numpy.array([bar.px_last for bar in bars]))
    return get_volatility_ohlc(bar_os, bar_us, bar_ds, bar_cs)

def get_volatility_ohlc(bar_os, bar_us, bar_ds, bar_cs):
    os = bar_os - numpy.concatenate((bar_os[0:1], bar_cs[:-1]))
    us = bar_us - bar_os
    ds = bar_ds - bar_os
    cs = bar_cs - bar_os
    vo = numpy.var(os, ddof=1)
    vc = numpy.var(cs, ddof=1)
    vrs = numpy.mean(us * (us - cs) + ds * (ds - cs))
    k = 0.34 / (1.34 + float(len(os) + 1) / float(len(os) - 1))
    return vo + k * vc + (1 - k) * vrs

# Static matrices

def nmat(n):
    return numpy.cumsum(1 - numpy.tri(n, k=-1, dtype=bool), axis=1)

def n1mat(n):
    return numpy.cumsum(1 - numpy.tri(n, dtype=bool), axis=1)

def kmat(n):
    return 0.34 / (1.34 + numpy.cumsum(1 - numpy.tri(n, dtype=bool) + 2 * numpy.diag(numpy.ones(n)), axis=1) / numpy.cumsum(1 - numpy.tri(n, dtype=bool), axis=1))

# Utility Functions

def subsequence_maxs(x):
    return numpy.maximum.accumulate(numpy.where(numpy.tri(x.shape[0], k=-1, dtype=bool), -numpy.inf, x), axis=1)

def subsequence_mins(x):
    return numpy.minimum.accumulate(numpy.where(numpy.tri(x.shape[0], k=-1, dtype=bool), numpy.inf, x), axis=1)

def subsequence_sums(x):
    return numpy.cumsum(x * (1 - numpy.tri(x.shape[0], k=-1, dtype=bool)), axis=1)

def subsequence_vars(x, nmatrix=None, n1matrix=None):
    return (numpy.cumsum(x * x * (1 - numpy.tri(x.shape[0], k=-1, dtype=bool)), axis=1) - numpy.cumsum(x * (1 - numpy.tri(x.shape[0], k=-1, dtype=bool)), axis=1) ** 2 / (nmat(x.shape[0]) if nmatrix is None else nmatrix)) / (n1mat(x.shape[0]) if n1matrix is None else n1matrix)

def subsequence_ecdf(x, w):
    n = x.shape[0]
    return numpy.sum(numpy.where((1 - numpy.tri(n, k=-1, dtype=bool)).reshape((n, 1, n)).repeat(n, axis=1) & numpy.tri(n, dtype=bool).reshape((1, n, n)).repeat(n, axis=0) & (x.reshape((n, 1)).repeat(n, axis=1) >= x).reshape((1, n, n)).repeat(n, axis=0), w, 0), axis=2) / subsequence_sums(w)

# Component functions

def fill_data(foo, data):
    data["O"] = numpy.log(numpy.array([bar.px_open for bar in foo]))
    data["U"] = numpy.log(numpy.array([bar.px_high for bar in foo]))
    data["D"] = numpy.log(numpy.array([bar.px_low for bar in foo]))
    data["C"] = numpy.log(numpy.array([bar.px_last for bar in foo]))
    data["o"] = data["O"] - numpy.concatenate((data["O"][0:1], data["C"][:-1]))
    data["u"] = data["U"] - data["O"]
    data["d"] = data["D"] - data["O"]
    data["c"] = data["C"] - data["O"]
    data["maxU"] = subsequence_maxs(data["U"])
    data["minD"] = subsequence_mins(data["D"])
    # 
    data["volume"] = numpy.array([bar.px_volume for bar in foo])
    data["value"] = (data["O"] + data["U"] + data["D"] + data["C"]) / 4 * data["volume"]
    data["vwap"] = subsequence_sums(data["value"]) / subsequence_sums(data["volume"])
    #
    data["2O"] = numpy.concatenate((data["O"][:-1], numpy.array([numpy.nan])))
    data["2U"] = numpy.concatenate((numpy.maximum(data["U"][:-1], data["U"][1:]), numpy.array([numpy.nan])))
    data["2D"] = numpy.concatenate((numpy.minimum(data["D"][:-1], data["D"][1:]), numpy.array([numpy.nan])))
    data["2C"] = numpy.concatenate((data["C"][1:], numpy.array([numpy.nan])))
    data["2volume"] = numpy.concatenate((data["volume"][:-1] + data["volume"][1:], numpy.array([numpy.nan])))
    data["2value"] = (data["2O"] + data["2U"] + data["2D"] + data["2C"]) / 4 * data["2volume"]

# Price Analytics

def fill_vo(data, nmatrix=None, n1matrix=None):
    data["vo"] = subsequence_vars(data["o"], nmatrix=nmatrix, n1matrix=n1matrix)

def fill_vc(data, nmatrix=None, n1matrix=None):
    data["vc"] = subsequence_vars(data["c"], nmatrix=nmatrix, n1matrix=n1matrix)

def fill_vrs(data, nmatrix=None):
    data["vrs"] = subsequence_sums(data["u"] * (data["u"] - data["c"]) + data["d"] * (data["d"] - data["c"])) / (nmat(data["c"].shape[0]) if nmatrix is None else nmatrix)

def fill_vs(data, kmatrix=None):
    data["vs"] = data["vo"] + (kmat(data["c"].shape[0]) if kmatrix is None else kmatrix) * data["vc"] + (1 - (kmat(data["c"].shape[0]) if kmatrix is None else kmatrix)) * data["vrs"]

def fill_vl(data):
    data["vl"] = (data["maxU"] - data["minD"]) ** 2 / (4 * numpy.log(2))

def fill_vr(data, nmatrix=None):
    data["vr"] = data["vs"] * (nmat(data["c"].shape[0]) if nmatrix is None else nmatrix) / data["vl"]
    data["vr2"] = data["vrs"] * (nmat(data["c"].shape[0]) if nmatrix is None else nmatrix) / data["vl"]

def fill_p(data, nmatrix=None):
    data["p"] = scipy.stats.f.cdf(data["vr"], (nmat(data["c"].shape[0]) if nmatrix is None else nmatrix), 3)
    data["p2"] = scipy.stats.f.cdf(data["vr2"], (nmat(data["c"].shape[0]) if nmatrix is None else nmatrix), 3)

def fill_semivariance(data):
    data["buysemivarianceopen"] = (data["u"] + numpy.maximum(data["o"], 0)) ** 2
    data["buysemivarianceclose"] = (data["c"] - data["d"]) ** 2
    data["sellsemivarianceopen"] = (-data["d"] - numpy.minimum(data["o"], 0)) ** 2
    data["sellsemivarianceclose"] = (data["u"] - data["c"]) ** 2
    data["buysemivariance"] = data["buysemivarianceopen"] + data["buysemivarianceclose"]
    data["sellsemivariance"] = data["sellsemivarianceopen"] + data["sellsemivarianceclose"]
    data["semivariance"] = data["buysemivariance"] + data["sellsemivariance"]

def fill_semivariancer(data):
    data["semivariancer"] = subsequence_sums(data["buysemivariance"]) / subsequence_sums(data["sellsemivariance"])

def fill_semivariancep(data):
    data["semivariancep"] = scipy.stats.f.cdf(data["semivariancer"], (nmat(data["c"].shape[0]) / 2 if nmatrix is None else nmatrix), (nmat(data["c"].shape[0]) / 2 if nmatrix is None else nmatrix))

# TODO: Price Analytics - Reversals

# TODO: Volume Analytics

# For below, [i, j] as the channel (both ends inclusive), so the (i-1)-th tick is prior to entering the channel, and the (j+1)-th tick breaks

def fill_breakout(data):
    data["breakoutup"] = 1.0 * (numpy.concatenate((data["U"][1:], numpy.array([numpy.nan]))) > data["maxU"])
    data["breakoutdown"] = 1.0 * (numpy.concatenate((data["D"][1:], numpy.array([numpy.nan]))) < data["minD"])
    data["breakout"] = data["breakoutup"] - data["breakoutdown"]

# Pre-conditions

# priorp, priorvolumeratio, priorvolumep, (priorretracement)

def fill_priorp(data):
    pass

def fill_retracement(data):
    n = data["c"].shape[0]
    data["highindex"] = numpy.argmax(numpy.where(numpy.tri(n, k=-1, dtype=bool), -numpy.inf, data["U"]), axis=1)
    data["lowindex"] = numpy.argmin(numpy.where(numpy.tri(n, k=-1, dtype=bool), numpy.inf, data["D"]), axis=1)
    data["retracementlow"] = numpy.nanmin(numpy.concatenate((data["C"][data["highindex"]].reshape((n, 1)), numpy.concatenate((data["minD"][1:, -1], numpy.array([numpy.nan])))[data["highindex"]].reshape((n, 1))), axis=1), axis=1)
    data["retracementhigh"] = numpy.nanmax(numpy.concatenate((data["C"][data["lowindex"]].reshape((n, 1)), numpy.concatenate((data["maxU"][1:, -1], numpy.array([numpy.nan])))[data["lowindex"]].reshape((n, 1))), axis=1), axis=1)

# Post-conditions

def fill_volumeratio(data, nmatrix=None):
    data["averagevolume"] = subsequence_sums(data["volume"]) / (nmat(data["c"].shape[0]) if nmatrix is None else nmatrix)
    data["volumeratio"] = data["averagevolume"] / numpy.concatenate((data["averagevolume"][1:, -1], numpy.array([numpy.nan])))

def fill_volumep(data, nmatrix=None):
    n = data["c"].shape[0]
    data["volumep"] = scipy.stats.f.cdf(data["volumeratio"], (nmat(n) if nmatrix is None else nmatrix), numpy.tile(n - numpy.arange(n) - 1, n).reshape((n, n)))

#

#

#

#

#

#

#

#

#

def fill_time(data):
    n = data["c"].shape[0]
    data["timeup"] = numpy.clip((data["U"].reshape((1, 1, n)).repeat(n, axis=0).repeat(n, axis=1) - data["maxU"].reshape((n, n, 1)).repeat(n, axis=2)) / (data["U"] - data["D"]).reshape((1, 1, n)).repeat(n, axis=0).repeat(n, axis=1), 0.0, 1.0)
    data["timedown"] = numpy.clip((data["minD"].reshape((n, n, 1)).repeat(n, axis=2) - data["D"].reshape((1, 1, n)).repeat(n, axis=0).repeat(n, axis=1)) / (data["U"] - data["D"]).reshape((1, 1, n)).repeat(n, axis=0).repeat(n, axis=1), 0.0, 1.0)
    data["totaltimeup"] = numpy.where(numpy.tri(n, k=-1, dtype=bool), numpy.nan, numpy.sum(numpy.where(numpy.tri(n, dtype=bool).reshape((1, n, n)).repeat(n, axis=0), 0, data["timeup"]), axis=2))
    data["totaltimedown"] = numpy.where(numpy.tri(n, k=-1, dtype=bool), numpy.nan, numpy.sum(numpy.where(numpy.tri(n, dtype=bool).reshape((1, n, n)).repeat(n, axis=0), 0, data["timedown"]), axis=2))
    data["proportiontimeup"] = data["totaltimeup"] / (n - numpy.arange(n))
    data["proportiontimedown"] = data["totaltimedown"] / (n - numpy.arange(n))

