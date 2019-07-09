#---------------------- geolocalized ---------------------------

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

client = MongoClient('localhost', 27017)
db = client.new_data
geolocalizato = db.geolocalizato


#laod the geolocalized file
filename = DBF('ged181.dbf', load=True, encoding='ANSI')

geolocalizato  = db.geolocalizato.insert_many(filename)

#---------------poverty---------------------------------------

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



#-------------- birth rate and others -------------------------

import json
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
db = client.new_data
birth_rate_and_others = db.birth_rate_and_others

# Open JSON
with open('birth_rate_and_others.json') as f:
    file_data = json.load(f)
birth_rate_and_others.insert(file_data)

#---------------- mortality codes -------------------------------

#for country names codes

# Obtained from this data
country_codes = open('Country_codes.txt', 'r')
newDataSet = open('New_morti_data.csv', 'w+')
oldDataSet = open('Morticd10_part2.csv', 'r')
causes_codes = open('morti tabulation.txt', 'r')


# Creating the dictionary where the data is saved
country_dic = {}

# Adding the data to the dictionary
for countries in country_codes:
    pais = countries.split()
    codigo = countries.split()[0]
    del pais[0]
    pais = " ".join(pais)
    country_dic[codigo] = pais

# --------------------------------------------------------

# The causes codes

# Creating the dictionary
causes_dic = {}

for line in causes_codes:
        line = line.replace("\n","")
        for i in range(len(line.split("\t"))):
                if i == 1:
                        causa = line.split("\t")[i]
                        causes_dic[temp_cod] = causa
                elif i==0:
                        temp_cod = line.split("\t")[i]
                elif line.split("\t")[i] == "":
                        break
                elif line.split("\t")[i].find("-") == -1:
                        causes_dic[line.split("\t")[i]] = causa # Para caundo solo es un valor, ej. A00
                else:
                        first_val = line.split("\t")[i][0: line.split("\t")[i].find("-")]
                        second_val = line.split("\t")[i][line.split("\t")[i].find("-")+1 : len(line.split("\t")[i]) ]
                        letera = first_val[0]
                        for numeros in range( int(first_val[1:3]) , int(second_val[1:3])+1  ):
                                if numeros<10:
                                       valor = str(letera)+ "0"+ str(numeros)
                                else:
                                       valor = str(letera)+ str(numeros)
                                causes_dic[ valor  ] = causa



# Searching and creating the new data
for line in oldDataSet:
        if line.split(",")[0] != "Country":
                if line.split(",")[5] in causes_dic:
                        line2 = line.replace(line.split(",")[5], causes_dic.get(line.split(",")[5]))
                else:
                        causes_dic[line.split(",")[5]] = "Other Causes"
                        line2 = line.replace(line.split(",")[5], causes_dic.get(line.split(",")[5]))
                line2 = line2.replace(line2.split(",")[0], country_dic.get(line2.split(",")[0]))
                newDataSet.write(line2)
        else:
                newDataSet.write(line)



# --------------------------------------------------
# finishing the jobs

newDataSet.close()
country_codes.close()



import json
from pymongo import MongoClient
import pandas as pd

client = MongoClient('localhost', 27017)
db = client.new_data
mortality = db.mortality

#laod the csv file
mortality_csv = pd.read_csv("New_morti_data.csv")


records_ = mortality_csv.to_dict(orient = 'records')
r = json.dumps(records_)
loaded_r = json.loads(r)

# print(loaded_r)
mortality.insert(loaded_r)


#------------- Share of people who say they're happy --------------------------------------

from pymongo import MongoClient
import csv
import pandas as pd
import json


client = MongoClient('localhost', 27017)
db = client.new_data
who_say = db.shared_people_who_say_they_are_happy

#Open CSV
df = pd.read_csv("share-of-people-who-say-they-are-happy (1).csv")
records_ = df.to_dict(orient = 'records')
r = json.dumps(records_)
loaded_r = json.loads(r)
who_say.insert_many(loaded_r)

#--------------World Opinion Survey-------------------------------------------------------

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
    


#-------------- Eurobarometer -------------------------------------------------------------

import pandas as pd
import pyreadstat
import pymongo
from pymongo import MongoClient
import csv
client = MongoClient('localhost', 27017)

countries = {'AL': 'Albania', 'AD':'Andorra', 'AM':'Armenia', 'AT':'Austria', 'BY':'Belarus', 'BE':'Belgium', 'BA':'Bosnia and Herzegovina', 'BG':'Bulgaria',
'CH':'Switzerland', 'CY':'Cyprus', 'CZ':'Czech Republic','DE':'Germany', 'DK':'Denmark', 'EE':'Estonia', 'ES':'Spain', 'FO':'Faeroe Islands','FI':'Finland','FR':'France', 'GB':'United Kingdom',
'GE':'Georgia', 'GI':'Gibraltar', 'GR':'Greece', 'HU':'Hungary', 'HR': 'Croatia', 'IE':'Ireland', 'IS':'Iceland', 'IT':'Italy', 'LT':'Lithuania', 'LU':'Luxembourg', 'LV':'Latvia', 'MC':'Monaco',
'MK': 'Macedonia', 'MT': 'Malta', 'NO':'Norway', 'NL':'Netherlands', 'PL': 'Poland', 'PT': 'Portugal', 'RO': 'Romania', 'RU':'Russian Federation', 'SE':'Sweden', 'SI':'Slovenia', 'SK':'Slovakia', 'SM':'San Marino',
'TR':'Turkey', 'UA':'Ukraine', 'VA':'Vatican City State', 'EL':'Greece', 'UK': 'United Kingdom', 'RS': 'Serbia' }

db = client.new_data2
eurobarometer = db.eurobarometer_collection


# areas_mortgage_rent_hire = pd.read_csv(tsvfile, "Arrears (mortgage or rent, utility bills or hire purchase) 2003 onwards.tsv",sep=r'\,|\t')

name = ["Arrears (mortgage or rent, utility bills or hire purchase) 2003 onwards.tsv", 
        "At-risk-of poverty rate for children by citizenship of their parents (population aged 0 to 17 years).tsv",
        "Contribution to the international 100bn USD commitment on climate related expending .tsv",
        "crime, violence or vandalism in the area.tsv",
        "Distribution of population by broad group of citizenship and tenure status.tsv",
        "Early leavers from education and training by sex and citizenship.tsv",
        "Early leavers from education and training by sex and country of birth.tsv",
        'ecommercesales.tsv',
        "Electricity production capacities for renewables and wastes.tsv",
        "Emissions of greenhouse gases and air pollutants from final use of CPA08 products - input-output analysis, ESA 2.tsv",
        "Employed persons discriminated at work during the last 12 months by sex and age (source Eurofound).tsv",
        "Energy efficiency.tsv",
        "Energy supply and use by NACE Rev. 2 activity.tsv",
        "environ prot expenditure by env domains.tsv",
        "Environmental taxes by economic activity.tsv",
        "Exposure to air pollution by particulate matter (source EEA).tsv",
        "Fatal Accidents at work by NACE.tsv",
        "Gender employment gap.tsv",
        "Gender pay gap in unadjusted form by NACE Rev. 2 activity - structure of earnings survey methodology.tsv",
        "generation of waste excluding major mieral per domestic .tsv",
        "Generation of waste excluding major mineral wastes per GDP unit.tsv",
        "Greenhouse gas emissions.tsv",
        "Housing cost overburden rate by age, sex and broad group of citizenship.tsv",
        "ilc_peesIntersections of Europe 2020 poverty target indicators by broad group of citizenship (population aged16).tsv",
        "In-work at-risk-of-poverty rate by broad group of citizenship (population aged 18 and over).tsv",
        "In-work at-risk-of-poverty rate by broad group of country of birth (population aged 18 and over).tsv",
        "Inability to face unexpected financial expenses.tsv",
        "Individuals - computer use.tsv",
        "Individuals - frequency of internet use.tsv",
        "Individuals who have never used a computer.tsv",
        "Individuals' level of digital skills.tsv",
        "job_hours_weekly.tsv",
        "Labour transitions by employment status.tsv",
        "Liquid biofuels production capacities.tsv",
        "Long-term residents among all non-EU citizens holding residence permits by citizenship on 31 December (%).tsv",
        "Low-wage earners as a proportion of all employees (excluding apprentices) by sex.tsv",
        "Management of waste excluding major mineral waste, by waste management operations.tsv",
        "Material and social deprivation rate by age, sex and broad group of country of birth.tsv",
        "Mean and median income by broad group of citizenship (population aged 18 and over).tsv",
        "Mean and median income by broad group of country of birth (population aged 18 and over).tsv",
        "municipalwastepercapita.tsv",
        "Noise from neighbours or from the street.tsv",
        "Non-fatal accidents at work by NACE.tsv",
        "ntersections of Europe 2020 poverty target indicators by broad group of country of birth (population aged 18 and.tsv",
        "Nuclear energy facilities.tsv",
        "Number of fatal accidents at work by Member State and age, excluding road traffic accidents and accidents on boa.tsv",
        "Overcrowding rate by age, sex and broad group of citizenship.tsv",
        "Participation in any cultural or sport activities in the last 12 months by income quintile, household type and.tsv",
        "Participation in any cultural or sport activities in the last 12 months by sex, age and educational attainment l.tsv",
        "People at risk of poverty or social exclusion by broad group of citizenship (population aged 18 and over).tsv",
        "People having a long-standing illness or health problem, by sex, age and groups of country of birth.tsv",
        "People living in households with very low work intensity by broad group of citizenship (population aged 18 to 59.tsv",
        "Persons reporting a chronic disease, by disease, sex, age and broad group of citizenship .tsv",
        "Persons who have someone to ask for help by sex, age and educational attainment level.tsv",
        "Persons who have someone to discuss personal matters by sex, age and educational attainment level.tsv",
        "Pollution, grime or other environmental problems.tsv",
        "Population by educational attainment level, sex and age (%).tsv",
        "Population by educational attainment level, sex, age and country of birth (%).tsv",
        "Population covered by the Covenant of Mayors for Climate & Energy signatories .tsv",
        "population density.tsv",
        "private investments.tsv",
        "Recorded offences by offence category - police data.tsv",
        "Residents who acquired citizenship as a share of resident non-citizens by former citizenship and sex.tsv",
        "Self-perceived health by sex, age and groups of country of birth.tsv",
        "Severe material deprivation rate by broad group of citizenship.tsv",
        "Severe material deprivation rate by broad group of country of birth.tsv",
        "Solar thermal collectors' surface.tsv",
        "Structure of earnings survey monthly earnings.tsv",
        "Supplies and raw materials.tsv",
        "Supply, transformation and consumption of renewables and wastes.tsv",
        "Time spent, participation time and participation rate in the main activity by sex and educational attainment lev.tsv"


        ]

variable_name = ['areas mortgage rent hire',
                'children poverty',
                'for clime expenditure',
                'crime',
                'tenure_status',
                'dropout',
                'dropout_country_birth',
                'e_commerce',
                'electricity_production',
                'greenhouse_gas_emissions',
                'greenhouse_employees',
                'energy_efficiency',
                'energy_supply',
                'environ_exp',
                'emviron_tax',
                'air_pollution_exposure',
                'fatal_accident_work',
                'gender_employment_gap',
                'gender_employment_gap_adj',
                'waste_including_mineral',
                'waste_excluding_mineral2',
                'greenhouse_gas_emissions',
                'hoverburden_housing_cost',
                'poverty_by_citizenship',
                'inwork_at_poverty_risk_citizenship',
                'inwork_at_poverty_risk_country_birth',
                'inability_unexpected_expenses',
                'computer_use',
                'internet_use',
                'never_computer',
                'digital_skills',
                'job_jours_weekly',
                'labor_transitions_by_employment',
                'liquid_biofuels_production',
                'residence_permits',
                'low_wage',
                'management_mineral_waste',
                'material_social_deprivation',
                'income_citizenship',
                'income_countrybirth',
                'municipal_waste',
                'noise_neighbours_street',
                'accident_work',
                'poverty_target_indicators',
                'nuclear_facilities',
                'fatal_accidents',
                'overcrowding_rate',
                'sport_activity_income',
                'sport_activity_sex',
                'social_exclusion',
                'long_standing_illness_birthcountry',
                'low_work_intensity_groupcitizenship',
                'chronic_disease',
                'ask_for_help',
                'can_discuss',
                'pollution_grime',
                'population_education_sex',
                'population_education_countrbirth',
                'covenant_mayors_climate',
                'population_density',
                'private_investments',
                'offence_category',
                'percentage_get_citizenship',
                'perceived_health',
                'material_deprivation_citizenship',
                'material_deprivation_countrygroup',
                'solar_thermal_collectors',
                'earnings',
                'supply_raw_materials',
                'renewable',
                'time_spent_main_activity'
                ]


# db.eurobarometer.update_one({'Country': countries['AL'], 'Year':2009 }, { '$set': {"value": {'hola2':1}}  })

temp_data = {}


for num_data in range(len(name)):
    temp_data['Type'] = variable_name[num_data]

    with open(name[num_data]) as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        first = True
        for row_document in reader:
            # print(row_document)
            if first:
                first = False
                Titles = row_document[0].split(",")
                years = row_document[1:]
            else:
                values = []
                for num in range(len(Titles)): #obtain information of the files
                    values.append(row_document[0].split(",")[num])
                    # print(Titles[num] + ": " + row_document[0].split(",")[num] )
                try:        
                    temp_data['Country'] = countries[values[len(values)-1]]

                    for num in range(len(years)): # obtaining the years...
                        try:
                            del temp_data['_id']
                        except:
                            pass
                        temp_data['Year'] = int(years[num])
                        for num2 in range(len(Titles)-1):
                            temp_data[Titles[num2]] = values[num2]    

                        # try:
                        #     # db.eurobarometer.update_one({'Country': countries[values[len(values)-1]], 'Year':int(years[num]) }, { '$set': {variable_name +"."+"value" : row_document[num+1]}}  )
                        # except:
                        #     print("no se encontro este pais: "+ values[len(values)-1] )
                        # print( years[num] + ': ' + row_document[num+1] )
                        temp_data['Value'] = row_document[num+1]
                        db.eurobarometer.insert_one(temp_data)
                except:
                    print("no se encontro este pais: "+ values[len(values)-1] )

                    
#-------------- world economic forum --------------------------------------------------------------
                    
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
    
#-------------- health            -----------------------------------------------------------------

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

#-------------- descriptive statistics -------------------------------------------------------------

from pymongo import MongoClient
client = MongoClient('localhost', 27017)

db = client.new_data

eurobarometer = db.eurobarometer_collection
mortality = db.mortality
birth_rate_and_others = db.birth_rate_and_others
geolocalizato = db.geolocalizato
poverty = db.poverty_collection
who_say = db.shared_people_who_say_they_are_happy
happiness = db.Happiness



country_desv_birth_rate_and_others = db.birth_rate_and_others.aggregate([
    { '$group': { "_id": '$Country Name', 'stdDev_birth_rate_and_others': { '$stdDevPop': '$Value' } } }
])

country_desv_birth_rate_and_others = db.birth_rate_and_others.aggregate([
    { '$group': { "_id": '$Country Name', 'stdDev_birth_rate_and_others': { '$avg': '$Value' } } }
])

general_desv_birth_rate_and_others = db.birth_rate_and_others.aggregate([
    { '$group': { "_id": 'Country Name', 'stdDev_birth_rate_and_others': { '$stdDevPop': '$Value' } } }
])

general_avg_birth_rate_and_others = db.birth_rate_and_others.aggregate([
    { '$group': { "_id": 'Country Name', 'stdDev_birth_rate_and_others': { '$avg': '$Value' } } }
])



country_desv_mortality = db.mortality.aggregate([
    { '$group': { "_id": '$Country', 'std_Deaths1': { '$stdDevPop': '$Deaths1' },
    'std_Deaths2': { '$stdDevPop': '$Deaths2' },
    'std_Deaths3': { '$stdDevPop': '$Deaths3' },
    'std_Deaths4': { '$stdDevPop': '$Deaths4' },
    'std_Deaths5': { '$stdDevPop': '$Deaths5' },
    'std_Deaths6': { '$stdDevPop': '$Deaths6' },
    'std_Deaths7': { '$stdDevPop': '$Deaths7' },
    'std_Deaths8': { '$stdDevPop': '$Deaths8' },
    'std_Deaths9': { '$stdDevPop': '$Deaths9' },
    'std_Deaths10': { '$stdDevPop': '$Deaths10' },
    'std_Deaths11': { '$stdDevPop': '$Deaths11' },
    'std_Deaths12': { '$stdDevPop': '$Deaths12' },
    'std_Deaths13': { '$stdDevPop': '$Deaths13' },
    'std_Deaths14': { '$stdDevPop': '$Deaths14' },
    'std_Deaths15': { '$stdDevPop': '$Deaths15' },
    'media_Deaths1': { '$avg': '$Deaths1' },
    'media_Deaths2': { '$avg': '$Deaths2' },
    'media_Deaths3': { '$avg': '$Deaths3' },
    'media_Deaths4': { '$avg': '$Deaths4' },
    'media_Deaths5': { '$avg': '$Deaths5' },
    'media_Deaths6': { '$avg': '$Deaths6' },
    'media_Deaths7': { '$avg': '$Deaths7' },
    'media_Deaths8': { '$avg': '$Deaths8' },
    'media_Deaths9': { '$avg': '$Deaths9' },
    'media_Deaths10': { '$avg': '$Deaths10' },
    'media_Deaths11': { '$avg': '$Deaths11' },
    'media_Deaths12': { '$avg': '$Deaths12' },
    'media_Deaths13': { '$avg': '$Deaths13' },
    'media_Deaths14': { '$avg': '$Deaths14' },
    'media_Deaths15': { '$avg': '$Deaths15' }
    } }
])





who_say = db.shared_people_who_say_they_are_happy.aggregate([
    { '$group': { "_id": '$Entity', 'stdDev_shared_people_who_say_they_are_happy': { '$stdDevPop': '(%)' },
    'media_shared_people_who_say_they_are_happy': { '$avg': '$Year' }  } }
])


poverty = db.poverty_collection.aggregate([
    { '$group': { "_id": "$Income Group",  'num of countries': { "$sum": 1 }
    } }
])



happy = db.Happiness.aggregate([
    {
        '$group': {
            '_id': 'Family Data', 
            'Desv estandard Family': {
                '$stdDevPop': '$Family'
            }, 
            'media Family': {
                '$avg': '$Family'
            }, 
            'desv estandard Freedom': {
                '$stdDevPop': '$Freedom'
            }, 
            'media Freedom': {
                '$avg': '$Freedom'
            }, 
            'media happiness score': {
                '$avg': '$Happiness Score'
            }, 
            'Desv estandard happiness score': {
                '$stdDevPop': '$Happiness Score'
            }, 
            'Desv estandard Generosity': {
                '$stdDevPop': '$Generosity'
            }, 
            'media Generosity': {
                '$avg': '$Generosity'
            }
        }
    }
])
