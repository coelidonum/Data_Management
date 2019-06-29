import json
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
db = client.new_data2
birth_rate_and_others = db.birth_rate_and_others

# Open JSON
with open('birth_rate_and_others.json') as f:
    file_data = json.load(f)
birth_rate_and_others.insert(file_data)

