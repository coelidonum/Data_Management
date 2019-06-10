import json
import csv
import pandas as pd
import sys
import re
import pymongo

from pymongo import MongoClient


#--------- call local host and create new data-----------------------

client = MongoClient('localhost', 27017)

db = client.new_data
happiness = db.data_management_collection
new = db.data_management_collection

#db2 = client.new_data
#happiness_2016 = db2.data_management_collection

happiness_2015 = pd.read_csv("happiness_2015.csv") #csv file which you want to import
happiness_2016 = pd.read_csv("happiness_2016.csv") #csv file which you want to import
happiness_2017 = pd.read_csv("happiness_2017.csv") #csv file which you want to import

#---------add year to dataframes--------------------------------------

happiness_2015['Year'] = "2015"
happiness_2016['Year'] = "2016"
happiness_2017['Year'] = "2017"


#---------one dataframe--------------------------------------

records = happiness_2015.append([happiness_2016, happiness_2017], ignore_index=True)

#--------- orient to records------------------------------------------

records = records.to_dict(orient = 'records')


#---------- insert many: oriented data---------------------------------

happiness = db.happiness.insert_many(records)

#-----------aggregate--------------------------------------------------



#happiness = db.happiness.aggregate([
#     {'$group': { '_id': "$Country"}},
#     {"$sort":  { "_id":1}
#     {'$lookup': {
#            'from': 'data_management_collection', 
#            'localField': 'Country', 
#            'foreignField': 'Country', 
#            'as': 'CountryFeatures'}}])

happiness = db.happiness.aggregate([
     {'$group': 
         { '_id': "$Country",
          "count" : 
                 { '$sum' :1 },
                 }},       
     {'$lookup': {
            'from': 'data_management_collection', 
            'localField': 'Country', 
            'foreignField': 'Country', 
            'as': 'CountryFeatures'}}])

