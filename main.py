import pymongo
from pymongo import MongoClient
import datetime

current_date_time = datetime.datetime.now() # it will give current system date and time
print (current_date_time)

# pass url for your mongo here bydefault will be taking localhost:27017 default port for mongoDB
myClient = MongoClient()

# pyDatabase is your database name you can access it like myClient["pyDatabase"] this also.
db = myClient.pyDatabase

users = db.users # users is collection name where your want to insert data.

# indexing
db.users.create_index([("name", pymongo.ASCENDING)],unique = True)

# single Insert
user1 = {"name": "vivek", "password": "123", "likes": ["cricket", "games", "music"], "createdDate": current_date_time}
user_id = users.insert_one(user1).inserted_id
print (user_id)


# multiple insertion
UsersObj = db.users
users = [{"name": "Test", "password": "123","createdDate": current_date_time}, {"name": "PQR", "password": "345","createdDate": current_date_time}]
inserted_Ids = UsersObj.insert_many(users)
print (inserted_Ids.inserted_ids)


# Find Objects

print (UsersObj.find().count())
print (UsersObj.find({"name":"vivek"}).count())
print (UsersObj.find())

old_date = datetime.datetime(2018, 3 , 28)

# $gt - greaterThan $gte - greaterthanequalto $lt - lessthan etc. for details comparision for data
# $ne - not equals

print (UsersObj.find({"createdDate": {"$gte": old_date}}).count())

# $exists used when you want to check if collection data has this columns or not as its nosql
# so it might possible that column should not be there for some data
print (UsersObj.find({"likes": {"$exists": True}}).count())


