import pandas as pd
import pymongo

from pymongo import MongoClient
client = MongoClient('localhost', 27017)

#--------- call local host and create new data-----------------------

db = client.new_data
GCI = db.GCI

#--------- upload Excel data----------------------
GCI_pandas = pd.read_excel("GCI.xlsx", sheet_name = "Foglio1")


#---------- subset df -------------------------------------------------

GCI_pandas = GCI_pandas.loc[(GCI_pandas['Attribute']=='Value')]
GCI_pandas = GCI_pandas.drop(columns= ['Placement', 'GLOBAL ID', 'Code GCR', 'Dataset', 'Series', 'Attribute'])

#--------- clean series unindented column ----------------------------

GCI_pandas['Series unindented'] = GCI_pandas['Series unindented'].str.replace(".", "")

GCI_pandas['Series unindented'] = GCI_pandas['Series unindented'].astype(str)

#--------- subest df according to year ----------------------------


GCI_pandas2007 = GCI_pandas.loc[GCI_pandas['Year'] == 2007]

GCI_pandas2008 = GCI_pandas.loc[GCI_pandas['Year'] == 2008]

GCI_pandas2009 = GCI_pandas.loc[GCI_pandas['Year'] == 2009]

GCI_pandas2010 = GCI_pandas.loc[GCI_pandas['Year'] == 2010]

GCI_pandas2011 = GCI_pandas.loc[GCI_pandas['Year'] == 2011]

GCI_pandas2012 = GCI_pandas.loc[GCI_pandas['Year'] == 2012]

GCI_pandas2013 = GCI_pandas.loc[GCI_pandas['Year'] == 2013]

GCI_pandas2014 = GCI_pandas.loc[GCI_pandas['Year'] == 2014]

GCI_pandas2015 = GCI_pandas.loc[GCI_pandas['Year'] == 2015]

GCI_pandas2016 = GCI_pandas.loc[GCI_pandas['Year'] == 2016]

GCI_pandas2017 = GCI_pandas.loc[GCI_pandas['Year'] == 2017]

#--------- transpose all df, clean, add column year-------------------
#2007
GCI_pandas2007_transposed = GCI_pandas2007.T 
GCI_pandas2007_transposed.columns = GCI_pandas2007_transposed.iloc[1]
GCI_pandas2007_transposed = GCI_pandas2007_transposed.reset_index()
GCI_pandas2007_transposed = GCI_pandas2007_transposed.rename(columns={"index":"Country"})
GCI_pandas2007_transposed = GCI_pandas2007_transposed.drop(GCI_pandas2007_transposed.index[[0, 1]])
GCI_pandas2007_transposed['Year'] = '2007'

#2008
GCI_pandas2008_transposed = GCI_pandas2008.T 
GCI_pandas2008_transposed.columns = GCI_pandas2008_transposed.iloc[1]
GCI_pandas2008_transposed = GCI_pandas2008_transposed.reset_index()
GCI_pandas2008_transposed = GCI_pandas2008_transposed.rename(columns={"index":"Country"})
GCI_pandas2008_transposed = GCI_pandas2008_transposed.drop(GCI_pandas2008_transposed.index[[0, 1]])
GCI_pandas2008_transposed['Year'] = '2008'


#2009
GCI_pandas2009_transposed = GCI_pandas2009.T 
GCI_pandas2009_transposed.columns = GCI_pandas2009_transposed.iloc[1]
GCI_pandas2009_transposed = GCI_pandas2009_transposed.reset_index()
GCI_pandas2009_transposed = GCI_pandas2009_transposed.rename(columns={"index":"Country"})
GCI_pandas2009_transposed = GCI_pandas2009_transposed.drop(GCI_pandas2009_transposed.index[[0, 1]])
GCI_pandas2009_transposed['Year'] = '2009'


#2010
GCI_pandas2010_transposed = GCI_pandas2010.T 
GCI_pandas2010_transposed.columns = GCI_pandas2010_transposed.iloc[1]
GCI_pandas2010_transposed = GCI_pandas2010_transposed.reset_index()
GCI_pandas2010_transposed = GCI_pandas2010_transposed.rename(columns={"index":"Country"})
GCI_pandas2010_transposed = GCI_pandas2010_transposed.drop(GCI_pandas2010_transposed.index[[0, 1]])
GCI_pandas2010_transposed['Year'] = '2010'


#2011
GCI_pandas2011_transposed = GCI_pandas2011.T 
GCI_pandas2011_transposed.columns = GCI_pandas2011_transposed.iloc[1]
GCI_pandas2011_transposed = GCI_pandas2011_transposed.reset_index()
GCI_pandas2011_transposed = GCI_pandas2011_transposed.rename(columns={"index":"Country"})
GCI_pandas2011_transposed = GCI_pandas2011_transposed.drop(GCI_pandas2011_transposed.index[[0, 1]])
GCI_pandas2011_transposed['Year'] = '2011'


#2012
GCI_pandas2012_transposed = GCI_pandas2012.T 
GCI_pandas2012_transposed.columns = GCI_pandas2012_transposed.iloc[1]
GCI_pandas2012_transposed = GCI_pandas2012_transposed.reset_index()
GCI_pandas2012_transposed = GCI_pandas2012_transposed.rename(columns={"index":"Country"})
GCI_pandas2012_transposed = GCI_pandas2012_transposed.drop(GCI_pandas2012_transposed.index[[0, 1]])
GCI_pandas2012_transposed['Year'] = '2012'

#2013
GCI_pandas2013_transposed = GCI_pandas2013.T 
GCI_pandas2013_transposed.columns = GCI_pandas2013_transposed.iloc[1]
GCI_pandas2013_transposed = GCI_pandas2013_transposed.reset_index()
GCI_pandas2013_transposed = GCI_pandas2013_transposed.rename(columns={"index":"Country"})
GCI_pandas2013_transposed = GCI_pandas2013_transposed.drop(GCI_pandas2013_transposed.index[[0, 1]])
GCI_pandas2013_transposed['Year'] = '2013'


#2014
GCI_pandas2014_transposed = GCI_pandas2014.T 
GCI_pandas2014_transposed.columns = GCI_pandas2014_transposed.iloc[1]
GCI_pandas2014_transposed = GCI_pandas2014_transposed.reset_index()
GCI_pandas2014_transposed = GCI_pandas2014_transposed.rename(columns={"index":"Country"})
GCI_pandas2014_transposed = GCI_pandas2014_transposed.drop(GCI_pandas2014_transposed.index[[0, 1]])
GCI_pandas2014_transposed['Year'] = '2014'


#2015
GCI_pandas2015_transposed = GCI_pandas2015.T 
GCI_pandas2015_transposed.columns = GCI_pandas2015_transposed.iloc[1]
GCI_pandas2015_transposed = GCI_pandas2015_transposed.reset_index()
GCI_pandas2015_transposed = GCI_pandas2015_transposed.rename(columns={"index":"Country"})
GCI_pandas2015_transposed = GCI_pandas2015_transposed.drop(GCI_pandas2015_transposed.index[[0, 1]])
GCI_pandas2015_transposed['Year'] = '2015'


#2016
GCI_pandas2016_transposed = GCI_pandas2016.T 
GCI_pandas2016_transposed.columns = GCI_pandas2016_transposed.iloc[1]
GCI_pandas2016_transposed = GCI_pandas2016_transposed.reset_index()
GCI_pandas2016_transposed = GCI_pandas2016_transposed.rename(columns={"index":"Country"})
GCI_pandas2016_transposed = GCI_pandas2016_transposed.drop(GCI_pandas2016_transposed.index[[0, 1]])
GCI_pandas2016_transposed['Year'] = '2016'


#2017
GCI_pandas2017_transposed = GCI_pandas2017.T 
GCI_pandas2017_transposed.columns = GCI_pandas2017_transposed.iloc[1]
GCI_pandas2017_transposed = GCI_pandas2017_transposed.reset_index()
GCI_pandas2017_transposed = GCI_pandas2017_transposed.rename(columns={"index":"Country"})
GCI_pandas2017_transposed = GCI_pandas2017_transposed.drop(GCI_pandas2017_transposed.index[[0, 1]])
GCI_pandas2017_transposed['Year'] = '2017'


#--------- orient to records------------------------------------------

#GCI_pandas_transposed_records = GCI_pandas_transposed.to_dict(orient = 'records')
GCI_pandas2007_transposed_records = GCI_pandas2007_transposed.to_dict(orient = 'records')
GCI_pandas2008_transposed_records = GCI_pandas2008_transposed.to_dict(orient = 'records')
GCI_pandas2009_transposed_records = GCI_pandas2009_transposed.to_dict(orient = 'records')
GCI_pandas2010_transposed_records = GCI_pandas2010_transposed.to_dict(orient = 'records')
GCI_pandas2011_transposed_records = GCI_pandas2011_transposed.to_dict(orient = 'records')
GCI_pandas2012_transposed_records = GCI_pandas2012_transposed.to_dict(orient = 'records')
GCI_pandas2013_transposed_records = GCI_pandas2013_transposed.to_dict(orient = 'records')
GCI_pandas2014_transposed_records = GCI_pandas2014_transposed.to_dict(orient = 'records')
GCI_pandas2015_transposed_records = GCI_pandas2015_transposed.to_dict(orient = 'records')
GCI_pandas2016_transposed_records = GCI_pandas2016_transposed.to_dict(orient = 'records')
GCI_pandas2017_transposed_records = GCI_pandas2017_transposed.to_dict(orient = 'records')
#---------- insert many: oriented data---------------------------------


records = db.GCI_pandas_transposed.insert_many(GCI_pandas2007_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2008_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2010_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2011_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2012_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2013_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2014_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2015_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2016_transposed_records)
records = db.GCI_pandas_transposed.insert_many(GCI_pandas2017_transposed_records)


#-----------aggregate--------------------------------------------------


GCI_pandas_transposed = db.GCI_pandas_transposed.aggregate([
     {'$group': 
         { '_id': "$Country",
          "count" : 
                 { '$sum' :1 },
                 }},       
     {'$lookup': {
            'from': 'GCI_pandas_transposed', 
            'localField': 'Country', 
            'foreignField': 'Country', 
            'as': 'CountryFeatures'}}])
