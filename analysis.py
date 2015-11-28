import os
from datetime import datetime
from optparse import OptionParser
import configparser
import numpy
import pandas
import matplotlib
import matplotlib.pyplot as plt

# Utility Functions

def load_equity_curve(experiment):
    return pandas.read_csv(...)

def plot_equity_curve(experiment, df):
    try:
        ys = df["equity"].values
        idx = numpy.where(ys != ys[0])[0][0] - 1
        xs = df["timestamp"].values
        x = numpy.array([datetime.fromtimestamp(t).date() for t in xs if t >= xs[idx]])
        y = ys[idx:]
        font = { 'family' : 'normal', 'weight' : 'normal', 'size' : 9 }
        matplotlib.rc('font', **font)
        plt.plot(x, y)
        plt.savefig(...)
        plt.close()
    except IndexError:
        pass

