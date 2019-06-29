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


#-----------------------------------------first is created the data blank data for all countries and years

# for i in countries:
#     for year in range(2003,2019):
#         db.eurobarometer.insert_one({ 'Country': countries[i], 'Year': year  })

#-----------------------------------------Now we add information from other countries...


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


temp_data['Type'] = variable_name[2]

with open(name[2]) as tsvfile:
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

                    


