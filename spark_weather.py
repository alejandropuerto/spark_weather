#!/usr/bin/env python3

import numpy as np
import time
import databricks.koalas as ks
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import functions as sf
from datetime import datetime

sc = SparkContext(master = 'local[2]')
sqlc = SQLContext(sc)

w1 = pd.read_csv('/Weather/weather01.txt', delim_whitespace = True)
w2 = pd.read_csv('/Weather/weather02.txt', delim_whitespace = True)
w3 = pd.read_csv('/Weather/weather03.txt', delim_whitespace = True)
w4 = pd.read_csv('/Weather/weather04.txt', delim_whitespace = True)
w5 = pd.read_csv('/Weather/weather05.txt', delim_whitespace = True)
w6 = pd.read_csv('/Weather/weather06.txt', delim_whitespace = True)
weather = pd.concat([w1, w2, w3, w4, w5, w6], axis=1)
weather = weather.reset_index(drop=True)
weather = weather.drop('X', axis=1)
weather = weather.reset_index(drop=True)
weather = weather.set_index(['year', 'month', 'measure'], drop=True)
weather = weather.rename(columns={x:y for x,y in zip(weather.columns,range(1,len(weather.columns)+1))}).astype(str)
weather = weather.T
weather = weather.reset_index()
weather = weather.rename(columns={"index": "day"})
weather= weather.set_index('day')
weather = weather.T
weather = weather.reset_index(level=2)
weather = weather.stack()
weather = weather.unstack(2)
weather.index = weather.index.get_level_values(0).astype(str) + '-' + weather.index.get_level_values(1).astype(str) + '-' + weather.index.get_level_values(2).astype(str)
weather.index.names = ['date']
weather = weather.replace(['<NA>', 'nan'], np.NaN)
weather = weather.dropna(how='all')
weather = weather.reset_index()
weather = weather.drop(31, axis=0)
weather = weather.reset_index(drop=True)
weather = weather.set_index('date', drop=True)
weather.index = pd.to_datetime(weather.index)
weather.index.names = ['date']
weather['Events'] = weather['Events'].fillna(method='ffill')
weather['Max.TemperatureF'] = weather['Max.TemperatureF'].astype(int)
weather['Mean.TemperatureF'] = weather['Mean.TemperatureF'].astype(int)
weather['Min.TemperatureF'] = weather['Min.TemperatureF'].astype(int)
weather_per_day = ks.from_pandas(weather)
weather_per_day.to_csv()

sc.stop()
