# -*- coding: utf-8 -*-
"""
Created on Sun Jun 16 16:09:29 2019

@author: corra
"""

from pymongo import MongoClient
import csv
import pandas as pd
import json

#----------call mongodb
client = MongoClient('localhost', 27017)
db = client.new_data
povstat = db.data_management_collection

#----------read files
povstat = pd.read_csv('PovStatsData.csv')
povstat.rename(columns={'Country Name':'Country'}, inplace = True)

#---------- eliminate nans

povstat = povstat.drop(labels='Unnamed: 48', axis=1)
povstat = povstat.dropna()

#----------rename according to year

povstat1974 = povstat.iloc[:,[0, 4]].reset_index(drop=True)
povstat1974.rename(columns={'1974':'Poverty'}, inplace = True)
povstat1974['Year'] = 1974

povstat1975 = povstat.iloc[:,[0, 5]].reset_index(drop=True)
povstat1975.rename(columns={'1975':'Poverty'}, inplace = True)
povstat1975['Year'] = 1975

povstat1976 = povstat.iloc[:,[0, 6]].reset_index(drop=True)
povstat1976.rename(columns={'1976':'Poverty'}, inplace = True)
povstat1976['Year'] = 1976

povstat1977 = povstat.iloc[:,[0, 7]].reset_index(drop=True)
povstat1977.rename(columns={'1977':'Poverty'}, inplace = True)
povstat1977['Year'] = 1977

povstat1978 = povstat.iloc[:,[0, 8]].reset_index(drop=True)
povstat1978.rename(columns={'1978':'Poverty'}, inplace = True)
povstat1978['Year'] = 1978

povstat1979 = povstat.iloc[:,[0, 9]].reset_index(drop=True)
povstat1979.rename(columns={'1979':'Poverty'}, inplace = True)
povstat1979['Year'] = 1979

povstat1980 = povstat.iloc[:,[0, 10]].reset_index(drop=True)
povstat1980.rename(columns={'1980':'Poverty'}, inplace = True)
povstat1980['Year'] = 1980

povstat1981 = povstat.iloc[:,[0, 11]].reset_index(drop=True)
povstat1981.rename(columns={'1981':'Poverty'}, inplace = True)
povstat1981['Year'] = 1981

povstat1982 = povstat.iloc[:,[0, 12]].reset_index(drop=True)
povstat1982.rename(columns={'1982':'Poverty'}, inplace = True)
povstat1982['Year'] = 1982

povstat1983 = povstat.iloc[:,[0, 13]].reset_index(drop=True)
povstat1983.rename(columns={'1983':'Poverty'}, inplace = True)
povstat1983['Year'] = 1983

povstat1984 = povstat.iloc[:,[0, 14]].reset_index(drop=True)
povstat1984.rename(columns={'1984':'Poverty'}, inplace = True)
povstat1984['Year'] = 1984

povstat1985 = povstat.iloc[:,[0, 15]].reset_index(drop=True)
povstat1985.rename(columns={'1985':'Poverty'}, inplace = True)
povstat1985['Year'] = 1985

povstat1986 = povstat.iloc[:,[0, 16]].reset_index(drop=True)
povstat1986.rename(columns={'1986':'Poverty'}, inplace = True)
povstat1986['Year'] = 1986

povstat1987 = povstat.iloc[:,[0, 17]].reset_index(drop=True)
povstat1987.rename(columns={'1987':'Poverty'}, inplace = True)
povstat1987['Year'] = 1987

povstat1988 = povstat.iloc[:,[0, 18]].reset_index(drop=True)
povstat1988.rename(columns={'1988':'Poverty'}, inplace = True)
povstat1988['Year'] = 1988

povstat1989 = povstat.iloc[:,[0, 19]].reset_index(drop=True)
povstat1989.rename(columns={'1989':'Poverty'}, inplace = True)
povstat1989['Year'] = 1989

povstat1990 = povstat.iloc[:,[0, 20]].reset_index(drop=True)
povstat1990.rename(columns={'1990':'Poverty'}, inplace = True)
povstat1990['Year'] = 1990

povstat1991 = povstat.iloc[:,[0, 21]].reset_index(drop=True)
povstat1991.rename(columns={'1991':'Poverty'}, inplace = True)
povstat1991['Year'] = 1991

povstat1992 = povstat.iloc[:,[0, 22]].reset_index(drop=True)
povstat1992.rename(columns={'1992':'Poverty'}, inplace = True)
povstat1992['Year'] = 1992

povstat1993 = povstat.iloc[:,[0, 23]].reset_index(drop=True)
povstat1993.rename(columns={'1993':'Poverty'}, inplace = True)
povstat1993['Year'] = 1993

povstat1994 = povstat.iloc[:,[0, 24]].reset_index(drop=True)
povstat1994.rename(columns={'1994':'Poverty'}, inplace = True)
povstat1994['Year'] = 1994

povstat1995 = povstat.iloc[:,[0, 25]].reset_index(drop=True)
povstat1995.rename(columns={'1995':'Poverty'}, inplace = True)
povstat1995['Year'] = 1995

povstat1996 = povstat.iloc[:,[0, 26]].reset_index(drop=True)
povstat1996.rename(columns={'1996':'Poverty'}, inplace = True)
povstat1996['Year'] = 1996

povstat1997 = povstat.iloc[:,[0, 27]].reset_index(drop=True)
povstat1997.rename(columns={'1997':'Poverty'}, inplace = True)
povstat1997['Year'] = 1997

povstat1998 = povstat.iloc[:,[0, 28]].reset_index(drop=True)
povstat1998.rename(columns={'1998':'Poverty'}, inplace = True)
povstat1998['Year'] = 1998

povstat1999 = povstat.iloc[:,[0, 29]].reset_index(drop=True)
povstat1999.rename(columns={'1999':'Poverty'}, inplace = True)
povstat1999['Year'] = 1999

povstat2000 = povstat.iloc[:,[0, 30]].reset_index(drop=True)
povstat2000.rename(columns={'2000':'Poverty'}, inplace = True)
povstat2000['Year'] = 2000

povstat2001 = povstat.iloc[:,[0, 31]].reset_index(drop=True)
povstat2001.rename(columns={'2001':'Poverty'}, inplace = True)
povstat2001['Year'] = 2001

povstat2002 = povstat.iloc[:,[0, 32]].reset_index(drop=True)
povstat2002.rename(columns={'2002':'Poverty'}, inplace = True)
povstat2002['Year'] = 2002

povstat2003 = povstat.iloc[:,[0, 33]].reset_index(drop=True)
povstat2003.rename(columns={'2003':'Poverty'}, inplace = True)
povstat2003['Year'] = 2003

povstat2004 = povstat.iloc[:,[0, 34]].reset_index(drop=True)
povstat2004.rename(columns={'2004':'Poverty'}, inplace = True)
povstat2004['Year'] = 2004

povstat2005 = povstat.iloc[:,[0, 35]].reset_index(drop=True)
povstat2005.rename(columns={'2005':'Poverty'}, inplace = True)
povstat2005['Year'] = 2005

povstat2006 = povstat.iloc[:,[0, 36]].reset_index(drop=True)
povstat2006.rename(columns={'2006':'Poverty'}, inplace = True)
povstat2006['Year'] = 2006

povstat2007 = povstat.iloc[:,[0, 37]].reset_index(drop=True)
povstat2007.rename(columns={'2007':'Poverty'}, inplace = True)
povstat2007['Year'] = 2007

povstat2008 = povstat.iloc[:,[0, 38]].reset_index(drop=True)
povstat2008.rename(columns={'2008':'Poverty'}, inplace = True)
povstat2008['Year'] = 2008

povstat2009 = povstat.iloc[:,[0, 39]].reset_index(drop=True)
povstat2009.rename(columns={'2009':'Poverty'}, inplace = True)
povstat2009['Year'] = 2009

povstat2010 = povstat.iloc[:,[0, 40]].reset_index(drop=True)
povstat2010.rename(columns={'2010':'Poverty'}, inplace = True)
povstat2010['Year'] = 2010

povstat2011 = povstat.iloc[:,[0, 41]].reset_index(drop=True)
povstat2011.rename(columns={'2011':'Poverty'}, inplace = True)
povstat2011['Year'] = 2011

povstat2012 = povstat.iloc[:,[0, 42]].reset_index(drop=True)
povstat2012.rename(columns={'2012':'Poverty'}, inplace = True)
povstat2012['Year'] = 2012

povstat2013 = povstat.iloc[:,[0, 43]].reset_index(drop=True)
povstat2013.rename(columns={'2013':'Poverty'}, inplace = True)
povstat2013['Year'] = 2013

povstat2014 = povstat.iloc[:,[0, 44]].reset_index(drop=True)
povstat2014.rename(columns={'2014':'Poverty'}, inplace = True)
povstat2014['Year'] = 2014

povstat2015 = povstat.iloc[:,[0, 45]].reset_index(drop=True)
povstat2015.rename(columns={'2015':'Poverty'}, inplace = True)
povstat2015['Year'] = 2015

povstat2016 = povstat.iloc[:,[0, 46]].reset_index(drop=True)
povstat2016.rename(columns={'2016':'Poverty'}, inplace = True)
povstat2016['Year'] = 2016

povstat2017 = povstat.iloc[:,[0, 47]].reset_index(drop=True)
povstat2017.rename(columns={'2017':'Poverty'}, inplace = True)
povstat2017['Year'] = 2017


#--------- orient to records------------------------------------------

records1 = povstat1974.to_dict(orient = 'records')
records2 = povstat1975.to_dict(orient = 'records')
records3 = povstat1976.to_dict(orient = 'records')
records4 = povstat1977.to_dict(orient = 'records')
records5 = povstat1978.to_dict(orient = 'records')
records6 = povstat1979.to_dict(orient = 'records')
records7 = povstat1980.to_dict(orient = 'records')
records8 = povstat1981.to_dict(orient = 'records')
records9 = povstat1982.to_dict(orient = 'records')
records10 = povstat1983.to_dict(orient = 'records')
records11 = povstat1984.to_dict(orient = 'records')
records12 = povstat1985.to_dict(orient = 'records')
records13 = povstat1986.to_dict(orient = 'records')
records14 = povstat1987.to_dict(orient = 'records')

records15 = povstat1988.to_dict(orient = 'records')
records16 = povstat1989.to_dict(orient = 'records')
records17 = povstat1990.to_dict(orient = 'records')
records18 = povstat1991.to_dict(orient = 'records')
records19 = povstat1992.to_dict(orient = 'records')
records20 = povstat1993.to_dict(orient = 'records')
records21 = povstat1994.to_dict(orient = 'records')
records22 = povstat1995.to_dict(orient = 'records')
records23 = povstat1996.to_dict(orient = 'records')
records24 = povstat1997.to_dict(orient = 'records')
records25 = povstat1998.to_dict(orient = 'records')
records26 = povstat1999.to_dict(orient = 'records')
records27 = povstat2000.to_dict(orient = 'records')
records28 = povstat2001.to_dict(orient = 'records')
records29 = povstat2002.to_dict(orient = 'records')
records30 = povstat2003.to_dict(orient = 'records')
records31 = povstat2004.to_dict(orient = 'records')
records32 = povstat2005.to_dict(orient = 'records')
records33 = povstat2006.to_dict(orient = 'records')
records34 = povstat2007.to_dict(orient = 'records')
records35 = povstat2008.to_dict(orient = 'records')
records35 = povstat2009.to_dict(orient = 'records')
records35 = povstat2010.to_dict(orient = 'records')
records35 = povstat2011.to_dict(orient = 'records')
records35 = povstat2012.to_dict(orient = 'records')
records35 = povstat2013.to_dict(orient = 'records')
records35 = povstat2014.to_dict(orient = 'records')
records35 = povstat2015.to_dict(orient = 'records')
records35 = povstat2016.to_dict(orient = 'records')
records35 = povstat2017.to_dict(orient = 'records')


#---------- insert many: oriented data---------------------------------

povstatdata = db.povstat.insert_many(records1)
povstatdata = db.povstat.insert_many(records2)
povstatdata = db.povstat.insert_many(records3)
povstatdata = db.povstat.insert_many(records4)
povstatdata = db.povstat.insert_many(records5)
povstatdata = db.povstat.insert_many(records6)
povstatdata = db.povstat.insert_many(records7)
povstatdata = db.povstat.insert_many(records8)
povstatdata = db.povstat.insert_many(records9)
povstatdata = db.povstat.insert_many(records10)
povstatdata = db.povstat.insert_many(records11)
povstatdata = db.povstat.insert_many(records12)
povstatdata = db.povstat.insert_many(records13)
povstatdata = db.povstat.insert_many(records14)
povstatdata = db.povstat.insert_many(records15)
povstatdata = db.povstat.insert_many(records16)
povstatdata = db.povstat.insert_many(records17)
povstatdata = db.povstat.insert_many(records18)
povstatdata = db.povstat.insert_many(records19)
povstatdata = db.povstat.insert_many(records20)
povstatdata = db.povstat.insert_many(records21)
povstatdata = db.povstat.insert_many(records22)
povstatdata = db.povstat.insert_many(records23)
povstatdata = db.povstat.insert_many(records24)
povstatdata = db.povstat.insert_many(records25)
povstatdata = db.povstat.insert_many(records26)
povstatdata = db.povstat.insert_many(records27)
povstatdata = db.povstat.insert_many(records28)
povstatdata = db.povstat.insert_many(records29)
povstatdata = db.povstat.insert_many(records30)
povstatdata = db.povstat.insert_many(records31)
povstatdata = db.povstat.insert_many(records32)
povstatdata = db.povstat.insert_many(records33)
povstatdata = db.povstat.insert_many(records34)
povstatdata = db.povstat.insert_many(records35)




povstat = db.povstat.aggregate([
     {'$group': 
         { '_id': "$Country",
          "count" : 
                 { '$sum' :1 },
                 }},       
     {'$lookup': {
            'from': 'povstat', 
            'localField': 'Country', 
            'foreignField': 'Country', 
            'as': 'CountryFeatures'}}])








