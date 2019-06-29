from pymongo import MongoClient

client = MongoClient('localhost', 27017)

db = client.new_data2
eurobarometer = db.eurobarometer_collection

data = db.eurobarometer.find({'Country': 'Belgium'})
print (data)

for i in data:
    print(i)