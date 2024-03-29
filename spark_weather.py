#!/usr/bin/env python3

import numpy as np
import time
import databricks.koalas as ks
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import functions as sf
from datetime import datetime
#libraries needed

sc = SparkContext(master = 'local[2]') #add these lines in order to work with spark and set the context where we will work

w1 = pd.read_csv('/home/alejandro/Documents/git-repositories/spark_weather/Weather/weather01.txt', delim_whitespace = True) #read the external .txt files using pandas library. These files have whitespaces as delimeter.
w2 = pd.read_csv('/home/alejandro/Documents/git-repositories/spark_weather/Weather/weather02.txt', delim_whitespace = True)
w3 = pd.read_csv('/home/alejandro/Documents/git-repositories/spark_weather/Weather/weather03.txt', delim_whitespace = True)
w4 = pd.read_csv('/home/alejandro/Documents/git-repositories/spark_weather/Weather/weather04.txt', delim_whitespace = True)
w5 = pd.read_csv('/home/alejandro/Documents/git-repositories/spark_weather/Weather/weather05.txt', delim_whitespace = True)
w6 = pd.read_csv('/home/alejandro/Documents/git-repositories/spark_weather/Weather/weather06.txt', delim_whitespace = True)
weather_concat = pd.concat([w1, w2, w3, w4, w5, w6], axis=1) #concatenate the files into one single dataframe and concatenates within columns
weather_concat = weather_concat.reset_index(drop=True) #reset the indexes by eliminating them
weather_no_x = weather_concat.drop('X', axis=1) #eliminates the Xs from the columns 
weather_no_x = weather_no_x.reset_index(drop=True)
weather_new_index = weather_no_x.set_index(['year', 'month', 'measure'], drop=True) #set the indexes by year, month, and measure
weather = weather_new_index.rename(columns={x:y for x,y in zip(weather_new_index.columns,range(1,len(weather_new_index.columns)+1))}).astype(str) #renames the columns where the Xs where eliminated and replaces with numbers from 1 to the limit of the number of columns left
weather_transpose = weather.T #transpose indexes and columns
weather_transpose = weather_transpose.reset_index() #resets the indexes
weather_transpose = weather_transpose.rename(columns={"index": "day"}) #renames the columns index as 'day' 
weather_day = weather_transpose.set_index('day') #set the day column as index
weather_day_transpose = weather_day.T 
weather_stacked = weather_day_transpose.stack() #allows having multi-level indexes by stacking columns to index
weather_unstacked = weather_stacked.unstack(2) #unstack the index and treat it as column. In this case, the second level.
weather_unstacked.index = weather_unstacked.index.get_level_values(0).astype(str) + '-' + weather_unstacked.index.get_level_values(1).astype(str) + '-' + weather_unstacked.index.get_level_values(2).astype(str) #gets the values of the indexes and treat them as string
weather_unstacked.index.names = ['date'] #set the name of the index as 'date'
weather = weather_unstacked.replace(['<NA>', 'nan'], np.NaN) #replaces the default cells with no values with a valid default null type to work with.
weather_no_null = weather.dropna(how='all') #eliminates all the null values
weather = weather_no_null.reset_index()
weather_no_31 = weather.drop(31, axis=0) #eliminates the 31 label of the index
weather_new_index = weather_no_31.reset_index(drop=True)
weather = weather_new_index.set_index('date', drop=True) #sets the index as 'date' by eliminating the previous one
weather.index = pd.to_datetime(weather.index) #turns the index to datetime format
weather.index.names = ['date'] #set the index name as date
weather['Events'] = weather['Events'].fillna(method='ffill') #fills the null values of the Events column with the last valid observation in order to not have null values
weather['Max.TemperatureF'] = weather['Max.TemperatureF'].astype(int) #setsthe integer format to Max.TemperatureF column
weather['Mean.TemperatureF'] = weather['Mean.TemperatureF'].astype(int) #setsthe integer format to Mean.TemperatureF column
weather['Min.TemperatureF'] = weather['Min.TemperatureF'].astype(int) #setsthe integer format to Min.TemperatureF column
weather_per_day = ks.from_pandas(weather) #creates a Koalas dataframe from Pandas dataframe
weather_per_day.to_csv('weather_per_day.csv') #write a comma-separated values (csv) file of the dataframe named 'weather_per_day.csv'

sc.stop() #stops the context of the spark environment
