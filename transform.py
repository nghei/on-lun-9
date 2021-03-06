#!/bin/env python3

# Note: Hardcoded information present

import sys
import datetime as dtime
from datetime import datetime
from collections import namedtuple
import math
import statsmodels.robust
import numpy
import pandas
import pywt

# Data Structures

# Utility Functions

# Wavelets

def wavelet_denoise_single_pass(series, wavelet):
    n = series.shape[0]
    l = int(numpy.ceil(numpy.log2(n)))
    noisy_coefs = pywt.wavedec(series, wavelet, level=l, mode='per')
    sigma = statsmodels.robust.stand_mad(noisy_coefs[-1])
    uthresh = sigma * numpy.sqrt(2 * numpy.log(n))
    denoised_coefs = noisy_coefs[:]
    denoised_coefs[1:] = (pywt.thresholding.soft(x, value=uthresh) for x in denoised_coefs[1:])
    denoised_series = pywt.waverec(denoised_coefs, wavelet, mode='per')
    if denoised_series.shape[0] > n:
        denoised_series = denoised_series[0:n]
    return denoised_series

# Logarithm-Detrend-Denoise-Trend-Exponentiate
def wavelet_denoise(series, wavelet):
    n = series.shape[0]
    # Take logarithm and Detrend
    log_series = numpy.log(series)
    linear_trend = (log_series[n-1] - log_series[0]) / (n - 1)
    detrended_series = log_series - (numpy.arange(n) * linear_trend if linear_trend != 0 else 0)
    denoised_series = None 
    # Roll around to get average denoising
    # For roll periods +/- 1, 2, 4, 8, ...
    roll_periods = [2**i for i in range(0, int(numpy.log2(n)) + 1)]
    roll_periods = sorted([-x for x in roll_periods] + [0] + roll_periods)
    for i in roll_periods:
        tmp_series = numpy.roll(detrended_series, i)
        tmp_denoised_series = wavelet_denoise_single_pass(tmp_series, wavelet)
        tmp_unshifted_series = numpy.roll(tmp_denoised_series, -i)
        if denoised_series is None:
            denoised_series = tmp_unshifted_series
        else:
            denoised_series = denoised_series + tmp_unshifted_series
    denoised_series = denoised_series / len(roll_periods)
    # Add back trend and return exponentiated series
    denoised_series += (numpy.arange(n) * linear_trend if linear_trend != 0 else 0)
    return numpy.exp(denoised_series)

def wavelet_transform(series, wavelet):
    n = series.shape[0]
    l = int(numpy.ceil(numpy.log2(n)))
    denoised = wavelet_denoise(series, wavelet)
    coefs = pywt.wavedec(denoised, wavelet, level=l, mode='per')
    return coefs

