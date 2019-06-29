


#for country names codes

# Obtained from this data
country_codes = open('Country_codes.txt', 'r')
newDataSet = open('New_morti_data.csv', 'w+')
oldDataSet = open('Morticd10_part1.csv', 'r')
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
'''
# Searching and creating the new data
for line in oldDataSet:
        if line.split(",")[0] != "Country":
                line2 = line.replace(line.split(",")[0], country_dic.get(line.split(",")[0]))
                newDataSet.write(line2)
'''
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
db = client.new_data2
mortality = db.mortality

#laod the csv file
mortality_csv = pd.read_csv("New_morti_data.csv")


records_ = mortality_csv.to_dict(orient = 'records')
r = json.dumps(records_)
loaded_r = json.loads(r)

# print(loaded_r)
mortality.insert(loaded_r)