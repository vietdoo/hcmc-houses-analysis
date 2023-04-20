import requests
import pymongo
import sys
import json
from bson.json_util import dumps
from bson.json_util import loads
import urllib.request

urllib.request.urlretrieve("https://tigerlake.s3.ap-southeast-1.amazonaws.com/clean_houses.json", filename = "full.json")
print('Fetch new Json: OK')

try:
    input_data = json.load(open('full.json'))
except Exception as e:
    print(e)


input_data = []
for line in open('full.json', 'r'):
    input_data.append(json.loads(line))

myclient = pymongo.MongoClient("mongodb+srv://vietdoo:viet29Viet@mongoboy.nvgcddc.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["nhatot"]
chotot_lite = mydb["full"]
print('Connect to mongodb: OK')

for coll in mydb.list_collection_names():
    print(coll)
    
print('Delete:', chotot_lite.delete_many({}).deleted_count)
print(chotot_lite.insert_many(input_data))