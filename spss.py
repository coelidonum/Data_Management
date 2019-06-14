# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 17:04:32 2019

@author: corra
"""
import pandas as pd
import pyreadstat
import pymongo
from pymongo import MongoClient
client = MongoClient('localhost', 27017)

#--------- Import db from sav ---------------------------------------

df_WV2, meta_WV2 = pyreadstat.read_sav("WV2.sav", apply_value_formats=True)
df_WV3, meta_WV3 = pyreadstat.read_sav("WV3.sav", apply_value_formats=True)
df_WV4, meta_WV4 = pyreadstat.read_sav("WV4.sav", apply_value_formats=True)
df_WV5, meta_WV5 = pyreadstat.read_sav("WV5.sav", apply_value_formats=True)
df_WV6, meta_WV6 = pyreadstat.read_sav("WV6.sav", apply_value_formats=True)

#--------- call local host and create new data-----------------------

db = client.new_data
spss = db.spss

#--------- labels -----------------------------------------------------
df_WV2.columns = meta_WV2.column_labels
df_WV3.columns = meta_WV3.column_labels
df_WV4.columns = meta_WV4.column_labels
df_WV5.columns = meta_WV5.column_labels
df_WV6.columns = meta_WV6.column_labels 

#--------------orient to records---------------------------------------

df_WV2_records_2 = df_WV2.to_dict(orient = 'records') 
spss = db.spss.insert_many(df_WV2_records_2)

df_WV3_records_3 = df_WV3.to_dict(orient = 'records')
spss = db.spss.insert_many(df_WV3_records_3)

df_WV4_records_4 = df_WV4.to_dict(orient = 'records') 
spss = db.spss.insert_many(df_WV4_records_4)

df_WV5_records_5 = df_WV5.to_dict(orient = 'records')
spss = db.spss.insert_many(df_WV5_records_5)

df_WV6_records_6 = df_WV6.to_dict(orient = 'records') 
spss = db.spss.insert_many(df_WV6_records_6)

#---------- aggregate ------------------------------------------------
spss = db.spss.aggregate([
 {
        '$group': {
            '_id': '$Country/region'
        }
    },   {
        '$unwind': {
            'path': '$Country/region'
        }
    }, {
        '$lookup': {
            'from': 'spss', 
            'localField': 'Country/field', 
            'foreignField': 'Country', 
            'as': 'CountryFeatures'
        }
    }
])


