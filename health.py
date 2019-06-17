# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 01:34:13 2019

@author: corra
"""

from pymongo import MongoClient
import pandas as pd

#---------- call mongodb
client = MongoClient('localhost', 27017)
db = client.new_data
health_db = db.data_management_collection

#----------- upload csv and clean

health = pd.read_csv('Health Nutrition.csv')
health.rename(columns = {'Country Name': 'Country'}, inplace=True)
health = health.drop(['Indicator Code','Unnamed: 60'], axis=1)
health = health.dropna()
health = health.reset_index(drop=True)

#-----------divide by year
health1960 = health.iloc[:,[0, 3]].reset_index(drop=True)
health1960.rename(columns={'1960':'Poverty'}, inplace = True)
health1960['Year'] = 1960

health1961 = health.iloc[:,[0, 4]].reset_index(drop=True)
health1961.rename(columns={'1961':'Poverty'}, inplace = True)
health1961['Year'] = 1961

health1962 = health.iloc[:,[0, 5]].reset_index(drop=True)
health1962.rename(columns={'1962':'Poverty'}, inplace = True)
health1962['Year'] = 1962

health1963 = health.iloc[:,[0, 6]].reset_index(drop=True)
health1963.rename(columns={'1963':'Poverty'}, inplace = True)
health1963['Year'] = 1963

health1964 = health.iloc[:,[0, 7]].reset_index(drop=True)
health1964.rename(columns={'1964':'Poverty'}, inplace = True)
health1964['Year'] = 1964

health1965 = health.iloc[:,[0, 8]].reset_index(drop=True)
health1965.rename(columns={'1965':'Poverty'}, inplace = True)
health1965['Year'] = 1965

health1966 = health.iloc[:,[0, 9]].reset_index(drop=True)
health1966.rename(columns={'1966':'Poverty'}, inplace = True)
health1966['Year'] = 1966

health1967 = health.iloc[:,[0, 10]].reset_index(drop=True)
health1967.rename(columns={'1967':'Poverty'}, inplace = True)
health1967['Year'] = 1967

health1968 = health.iloc[:,[0, 11]].reset_index(drop=True)
health1968.rename(columns={'1968':'Poverty'}, inplace = True)
health1968['Year'] = 1968

health1969 = health.iloc[:,[0, 12]].reset_index(drop=True)
health1969.rename(columns={'1969':'Poverty'}, inplace = True)
health1969['Year'] = 1969

health1970 = health.iloc[:,[0, 13]].reset_index(drop=True)
health1970.rename(columns={'1970':'Poverty'}, inplace = True)
health1970['Year'] = 1970

health1971 = health.iloc[:,[0, 14]].reset_index(drop=True)
health1971.rename(columns={'1971':'Poverty'}, inplace = True)
health1971['Year'] = 1971

health1972 = health.iloc[:,[0, 15]].reset_index(drop=True)
health1972.rename(columns={'1972':'Poverty'}, inplace = True)
health1972['Year'] = 1972

health1973 = health.iloc[:,[0, 16]].reset_index(drop=True)
health1973.rename(columns={'1973':'Poverty'}, inplace = True)
health1973['Year'] = 1973

health1974 = health.iloc[:,[0, 17]].reset_index(drop=True)
health1974.rename(columns={'1974':'Poverty'}, inplace = True)
health1974['Year'] = 1974

health1975 = health.iloc[:,[0, 18]].reset_index(drop=True)
health1975.rename(columns={'1975':'Poverty'}, inplace = True)
health1975['Year'] = 1975

health1976 = health.iloc[:,[0, 19]].reset_index(drop=True)
health1976.rename(columns={'1976':'Poverty'}, inplace = True)
health1976['Year'] = 1976

health1977 = health.iloc[:,[0, 20]].reset_index(drop=True)
health1977.rename(columns={'1977':'Poverty'}, inplace = True)
health1977['Year'] = 1977

health1978 = health.iloc[:,[0, 21]].reset_index(drop=True)
health1978.rename(columns={'1978':'Poverty'}, inplace = True)
health1978['Year'] = 1978

health1979 = health.iloc[:,[0, 22]].reset_index(drop=True)
health1979.rename(columns={'1979':'Poverty'}, inplace = True)
health1979['Year'] = 1979

health1980 = health.iloc[:,[0, 23]].reset_index(drop=True)
health1980.rename(columns={'1980':'Poverty'}, inplace = True)
health1980['Year'] = 1980

health1981 = health.iloc[:,[0, 24]].reset_index(drop=True)
health1981.rename(columns={'1981':'Poverty'}, inplace = True)
health1981['Year'] = 1981

health1982 = health.iloc[:,[0, 25]].reset_index(drop=True)
health1982.rename(columns={'1982':'Poverty'}, inplace = True)
health1982['Year'] = 1982

health1983 = health.iloc[:,[0, 26]].reset_index(drop=True)
health1983.rename(columns={'1983':'Poverty'}, inplace = True)
health1983['Year'] = 1983

health1984 = health.iloc[:,[0, 27]].reset_index(drop=True)
health1984.rename(columns={'1984':'Poverty'}, inplace = True)
health1984['Year'] = 1984

health1985 = health.iloc[:,[0, 28]].reset_index(drop=True)
health1985.rename(columns={'1985':'Poverty'}, inplace = True)
health1985['Year'] = 1985

health1986 = health.iloc[:,[0, 29]].reset_index(drop=True)
health1986.rename(columns={'1986':'Poverty'}, inplace = True)
health1986['Year'] = 1986

health1987 = health.iloc[:,[0, 30]].reset_index(drop=True)
health1987.rename(columns={'1987':'Poverty'}, inplace = True)
health1987['Year'] = 1987

health1988 = health.iloc[:,[0, 31]].reset_index(drop=True)
health1988.rename(columns={'1988':'Poverty'}, inplace = True)
health1988['Year'] = 1988

health1989 = health.iloc[:,[0, 32]].reset_index(drop=True)
health1989.rename(columns={'1989':'Poverty'}, inplace = True)
health1989['Year'] = 1989

health1990 = health.iloc[:,[0, 33]].reset_index(drop=True)
health1990.rename(columns={'1990':'Poverty'}, inplace = True)
health1990['Year'] = 1990

health1991 = health.iloc[:,[0, 34]].reset_index(drop=True)
health1991.rename(columns={'1991':'Poverty'}, inplace = True)
health1991['Year'] = 1991

health1992 = health.iloc[:,[0, 35]].reset_index(drop=True)
health1992.rename(columns={'1992':'Poverty'}, inplace = True)
health1992['Year'] = 1992

health1993 = health.iloc[:,[0, 36]].reset_index(drop=True)
health1993.rename(columns={'1993':'Poverty'}, inplace = True)
health1993['Year'] = 1993

health1994 = health.iloc[:,[0, 37]].reset_index(drop=True)
health1994.rename(columns={'1994':'Poverty'}, inplace = True)
health1994['Year'] = 1994

health1995 = health.iloc[:,[0, 38]].reset_index(drop=True)
health1995.rename(columns={'1995':'Poverty'}, inplace = True)
health1995['Year'] = 1995

health1996 = health.iloc[:,[0, 39]].reset_index(drop=True)
health1996.rename(columns={'1996':'Poverty'}, inplace = True)
health1996['Year'] = 1996

health1997 = health.iloc[:,[0, 40]].reset_index(drop=True)
health1997.rename(columns={'1997':'Poverty'}, inplace = True)
health1997['Year'] = 1997

health1998 = health.iloc[:,[0, 41]].reset_index(drop=True)
health1998.rename(columns={'1998':'Poverty'}, inplace = True)
health1998['Year'] = 1998

health1999 = health.iloc[:,[0, 42]].reset_index(drop=True)
health1999.rename(columns={'1999':'Poverty'}, inplace = True)
health1999['Year'] = 1990

health2000 = health.iloc[:,[0, 43]].reset_index(drop=True)
health2000.rename(columns={'2000':'Poverty'}, inplace = True)
health2000['Year'] = 2000

health2001 = health.iloc[:,[0, 44]].reset_index(drop=True)
health2001.rename(columns={'2000':'Poverty'}, inplace = True)
health2001['Year'] = 2001

health2002 = health.iloc[:,[0, 45]].reset_index(drop=True)
health2002.rename(columns={'2002':'Poverty'}, inplace = True)
health2002['Year'] = 2002

health2003 = health.iloc[:,[0, 46]].reset_index(drop=True)
health2003.rename(columns={'2003':'Poverty'}, inplace = True)
health2003['Year'] = 2003

health2004 = health.iloc[:,[0, 47]].reset_index(drop=True)
health2004.rename(columns={'2004':'Poverty'}, inplace = True)
health2004['Year'] = 2004

health2005 = health.iloc[:,[0, 48]].reset_index(drop=True)
health2005.rename(columns={'2005':'Poverty'}, inplace = True)
health2005['Year'] = 2005

health2006 = health.iloc[:,[0, 49]].reset_index(drop=True)
health2006.rename(columns={'2006':'Poverty'}, inplace = True)
health2006['Year'] = 2006

health2007 = health.iloc[:,[0, 50]].reset_index(drop=True)
health2007.rename(columns={'2007':'Poverty'}, inplace = True)
health2007['Year'] = 2007

health2008 = health.iloc[:,[0, 51]].reset_index(drop=True)
health2008.rename(columns={'2008':'Poverty'}, inplace = True)
health2008['Year'] = 2008

health2009 = health.iloc[:,[0, 52]].reset_index(drop=True)
health2009.rename(columns={'2009':'Poverty'}, inplace = True)
health2009['Year'] = 2009

health2010 = health.iloc[:,[0, 53]].reset_index(drop=True)
health2010.rename(columns={'2010':'Poverty'}, inplace = True)
health2010['Year'] = 2010

health2011 = health.iloc[:,[0, 54]].reset_index(drop=True)
health2011.rename(columns={'2011':'Poverty'}, inplace = True)
health2011['Year'] = 2011

health2012 = health.iloc[:,[0, 55]].reset_index(drop=True)
health2012.rename(columns={'2012':'Poverty'}, inplace = True)
health2012['Year'] = 2012

health2013 = health.iloc[:,[0, 56]].reset_index(drop=True)
health2013.rename(columns={'2013':'Poverty'}, inplace = True)
health2013['Year'] = 2013

health2014 = health.iloc[:,[0, 57]].reset_index(drop=True)
health2014.rename(columns={'2014':'Poverty'}, inplace = True)
health2014['Year'] = 2014

health2015 = health.iloc[:,[0, 58]].reset_index(drop=True)
health2015.rename(columns={'2015':'Poverty'}, inplace = True)
health2015['Year'] = 2015

#--------- orient to records------------------------------------------

records1 = health1960.to_dict(orient = 'records')
records2 = health1961.to_dict(orient = 'records')
records3 = health1962.to_dict(orient = 'records')
records4 = health1963.to_dict(orient = 'records')
records5 = health1964.to_dict(orient = 'records')
records6 = health1965.to_dict(orient = 'records')
records7 = health1966.to_dict(orient = 'records')
records8 = health1967.to_dict(orient = 'records')
records9 = health1968.to_dict(orient = 'records')
records10 = health1969.to_dict(orient = 'records')
records11 = health1970.to_dict(orient = 'records')
records12 = health1971.to_dict(orient = 'records')
records13 = health1972.to_dict(orient = 'records')
records14 = health1973.to_dict(orient = 'records')
records15 = health1974.to_dict(orient = 'records')
records16 = health1975.to_dict(orient = 'records')
records17 = health1976.to_dict(orient = 'records')
records18 = health1977.to_dict(orient = 'records')
records19 = health1978.to_dict(orient = 'records')
records20 = health1979.to_dict(orient = 'records')

records21 = health1980.to_dict(orient = 'records')
records22 = health1981.to_dict(orient = 'records')
records23 = health1982.to_dict(orient = 'records')
records24 = health1983.to_dict(orient = 'records')
records25 = health1984.to_dict(orient = 'records')
records26 = health1985.to_dict(orient = 'records')
records27 = health1986.to_dict(orient = 'records')
records28 = health1987.to_dict(orient = 'records')
records29 = health1988.to_dict(orient = 'records')
records30 = health1989.to_dict(orient = 'records')
records31 = health1990.to_dict(orient = 'records')

records31 = health1990.to_dict(orient = 'records')
records32 = health1991.to_dict(orient = 'records')
records33 = health1992.to_dict(orient = 'records')
records34 = health1993.to_dict(orient = 'records')
records35 = health1994.to_dict(orient = 'records')
records36 = health1995.to_dict(orient = 'records')
records37 = health1996.to_dict(orient = 'records')
records38 = health1997.to_dict(orient = 'records')
records39 = health1998.to_dict(orient = 'records')
records40 = health1999.to_dict(orient = 'records')
records41 = health2000.to_dict(orient = 'records')

records41 = health2001.to_dict(orient = 'records')
records42 = health2002.to_dict(orient = 'records')
records43 = health2003.to_dict(orient = 'records')
records44 = health2004.to_dict(orient = 'records')
records45 = health2005.to_dict(orient = 'records')
records46 = health2006.to_dict(orient = 'records')
records47 = health2007.to_dict(orient = 'records')
records48 = health2008.to_dict(orient = 'records')
records49 = health2009.to_dict(orient = 'records')
records50 = health2010.to_dict(orient = 'records')
records51 = health2011.to_dict(orient = 'records')

records51 = health2012.to_dict(orient = 'records')
records52 = health2012.to_dict(orient = 'records')
records53 = health2013.to_dict(orient = 'records')
records54 = health2014.to_dict(orient = 'records')
records55 = health2015.to_dict(orient = 'records')

health_db = db.happiness.insert_many(records1)
health_db = db.happiness.insert_many(records2)
health_db = db.happiness.insert_many(records3)
health_db = db.happiness.insert_many(records4)
health_db = db.happiness.insert_many(records5)
health_db = db.happiness.insert_many(records6)
health_db = db.happiness.insert_many(records7)
health_db = db.happiness.insert_many(records8)
health_db = db.happiness.insert_many(records9)
health_db = db.happiness.insert_many(records10)

health_db = db.happiness.insert_many(records11)
health_db = db.happiness.insert_many(records12)
health_db = db.happiness.insert_many(records13)
health_db = db.happiness.insert_many(records14)
health_db = db.happiness.insert_many(records15)
health_db = db.happiness.insert_many(records16)
health_db = db.happiness.insert_many(records17)
health_db = db.happiness.insert_many(records18)
health_db = db.happiness.insert_many(records19)
health_db = db.happiness.insert_many(records20)
health_db = db.happiness.insert_many(records20)

health_db = db.happiness.insert_many(records21)
health_db = db.happiness.insert_many(records22)
health_db = db.happiness.insert_many(records23)
health_db = db.happiness.insert_many(records24)
health_db = db.happiness.insert_many(records25)
health_db = db.happiness.insert_many(records26)
health_db = db.happiness.insert_many(records27)
health_db = db.happiness.insert_many(records28)
health_db = db.happiness.insert_many(records29)

health_db = db.happiness.insert_many(records30)
health_db = db.happiness.insert_many(records31)
health_db = db.happiness.insert_many(records32)
health_db = db.happiness.insert_many(records33)
health_db = db.happiness.insert_many(records34)
health_db = db.happiness.insert_many(records35)
health_db = db.happiness.insert_many(records36)
health_db = db.happiness.insert_many(records37)
health_db = db.happiness.insert_many(records38)
health_db = db.happiness.insert_many(records39)
health_db = db.happiness.insert_many(records40)

health_db = db.happiness.insert_many(records41)
health_db = db.happiness.insert_many(records42)
health_db = db.happiness.insert_many(records43)
health_db = db.happiness.insert_many(records44)
health_db = db.happiness.insert_many(records45)
health_db = db.happiness.insert_many(records46)
health_db = db.happiness.insert_many(records47)
health_db = db.happiness.insert_many(records48)
health_db = db.happiness.insert_many(records49)
health_db = db.happiness.insert_many(records50)
health_db = db.happiness.insert_many(records51)
health_db = db.happiness.insert_many(records52)
health_db = db.happiness.insert_many(records53)
health_db = db.happiness.insert_many(records54)
health_db = db.happiness.insert_many(records55)

