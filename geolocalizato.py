import json
from pymongo import MongoClient
import csv
import pandas as pd
import sys
import re
import itertools
from dbfread import DBF
from pandas import DataFrame
'''
dbf = DBF('ged181.dbf')
frame = DataFrame(iter(dbf))

print(frame)
'''
from dbfread import DBF
import collections
from collections import OrderedDict
import bson


#--------- call local host and create new data-----------------------
client = MongoClient('localhost', 27017)
db = client.new_data2
geolocalizato = db.geolocalizato


#laod the geolocalized file
filename = DBF('ged181.dbf', load=True, encoding='ANSI')

geolocalizato  = db.geolocalizato.insert_many(filename)



