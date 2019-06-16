# -*- coding: utf-8 -*-
"""
Created on Sun Jun 16 16:09:29 2019

@author: corra
"""

from pymongo import MongoClient
import csv
import pandas as pd
import json

#--------- call local host and create new data-----------------------

client = MongoClient('localhost', 27017)
db = client.new_data
poverty = db.poverty_collection

#--------- load csv ------------------------------------------------

df = pd.read_csv("PovStatsCountry.csv")

#--------- rename country -------------------------------------------

df.rename(columns={'Short Name':'Country'}, inplace = True)

#---------select columns --------------------------------------------
df.columns

df = df[['Country Code', 'Country', 'Currency Unit',  'Region', 'Income Group']]

#---------add year --------------------------------------------------

df['year'] = 2016


#--------- orient to records-----------------------------------------

records = df.to_dict(orient = 'records')

#---------- insert many: oriented data-------------------------------

poverty = db.poverty.insert_many(records)


