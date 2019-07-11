#!/usr/bin/env python3

import numpy as np
import time
from pyspark import SparkContext
from pyspark.sql import functions as sf
from datetime import datetime
import databricks.koalas as ks
import pandas as pd

sc = SparkContext(master = 'local[2]')
sqlc = SQLContext(sc)
w1 = pd.read_csv('/Weather/weather01.txt', delim_whitespace = True)
w2 = pd.read_csv('/Weather/weather02.txt', delim_whitespace = True)
w3 = pd.read_csv('/Weather/weather03.txt', delim_whitespace = True)
w4 = pd.read_csv('/Weather/weather04.txt', delim_whitespace = True)
w5 = pd.read_csv('/Weather/weather05.txt', delim_whitespace = True)
w6 = pd.read_csv('/Weather/weather06.txt', delim_whitespace = True)
weather1 = pd.concat([w1, w2, w3, w4, w5, w6], axis=1)
weather1 = weather1.reset_index(drop=True)
weather2 = weather1.drop('X', axis=1)
weather2 = weather2.reset_index(drop=True)
mami = weather2.set_index(['year', 'month', 'measure'], drop=True)
lol = mami.rename(columns={x:y for x,y in zip(mami.columns,range(1,len(mami.columns)+1))}).astype(str)
jaja = lol.T
jaja = jaja.reset_index()
jaja = jaja.rename(columns={"index": "day"})
popo= jaja.set_index('day')
yamama = popo.T
yaporfavor = yamama.reset_index(level=2)
lala = yamama.stack()
creoqueya = lala.unstack(2)
creoqueya.index = creoqueya.index.get_level_values(0).astype(str) + '-' + creoqueya.index.get_level_values(1).astype(str) + '-' + creoqueya.index.get_level_values(2).astype(str)
creoqueya.index.names = ['date']
yamerito = creoqueya.replace(['<NA>', 'nan'], np.NaN)
porfincreo = yamerito.dropna(how='all')
yacasiahorasi = porfincreo.reset_index()
averahora = yacasiahorasi.drop(31, axis=0)
unomas = averahora.reset_index(drop=True)
ahorasi = unomas.set_index('date', drop=True)
ahorasi.index = pd.to_datetime(ahorasi.index)
ahorasi.index.names = ['date']
ahorasi['Events'] = ahorasi['Events'].fillna(method='ffill')
ahorasi['Max.TemperatureF'] = ahorasi['Max.TemperatureF'].astype(int)
ahorasi['Mean.TemperatureF'] = ahorasi['Mean.TemperatureF'].astype(int)
ahorasi['Min.TemperatureF'] = ahorasi['Min.TemperatureF'].astype(int)
ya = ks.from_pandas(ahorasi)
ya.to_csv()
sc.stop()
