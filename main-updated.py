from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure
from pymongo.collation import Collation
import datetime
from bson.objectid import ObjectId
import time


def connectMongoDB(host = "localhost", port = 27017):
    connection = None
    try:
        print("Going For Connection")
        connection = MongoClient(host, port)
        print(connection)
        return connection
    except ConnectionFailure:
        print("Error in Database Connection retry After 5 seconds")
        print(ConnectionFailure)
        time.sleep(1)
        print("Host "+host)
        connectMongoDB(host, port)
    

def getCollectionList(system_collection_flag = False):
    return database.collection_names(include_system_collections=system_collection_flag)

def insertUser(user_object):
    #usersCollection.insert_one(userData).inserted_id 
    return usersCollection.insert(user_object)

def getUserById(user_id):
    return usersCollection.find_one({"_id": ObjectId(user_id)})

def getUserByName(user_name):
    return usersCollection.find_one({"name": user_name})

def getAllUsers():
    return usersCollection.find()

def insertBulkUsers(user_collection):
    return usersCollection.insert_many(user_collection)

def getAllUsersCount():
    return usersCollection.count_documents({})

def getUserCountByName(user_name):
    return usersCollection.count_documents({"name": user_name})

def createIndexOnUser(column_name, sort = 1, unique = False):
    usersCollection.create_index([(column_name, ASCENDING if sort == 1 else DESCENDING)], unique = unique)
    return "Index SuccessFully Created on "+ column_name

def dropUserCollection():
    usersCollection.drop()
    return "User Collection Drop SuccessFully"

def getUserByCollationLocale(locale):
    return usersCollection.find().sort('name').collation(Collation(locale = locale))

def copyDatabase(source_db, destination_db, host = "localhost"):
    client.admin.command('copydb', fromdb = source_db, todb = destination_db, from_host = host)

def updateUserById(user_id, key_name, value):
    updateJson = {}
    updateJson[key_name] = value 
    usersCollection.update_one({"_id": ObjectId(user_id)}, { "$set" : updateJson })

client = connectMongoDB("192.168.2.84")
    
# DataBase Selection
database = client['test_db']

userData = {"id": 1, "name": 'Vivek', "password": 123, "date": datetime.datetime.utcnow()}
userData_collection = [{"id": 1, "name": 'PQR', "password": 123, "date": datetime.datetime.utcnow()}, {"id": 2, "name": 'Xyz', "password": 123, "date": datetime.datetime.utcnow()}
,{"id": 3, "name": 'Test', "password": 123, "date": datetime.datetime.utcnow()}]
usersCollection = database.users
print(getCollectionList(True))
#print(dropUserCollection())
#print(insertUser(userData))
#print(insertBulkUsers(userData_collection))
for user in getAllUsers():
    print(user)
#print(createIndexOnUser('name', 2))
print(getAllUsersCount())
print(getUserCountByName('Test'))
print(updateUserById('5c237daa38c113380200c5dd', 'name', '123'))
for user in getAllUsers():
    print(user)

#print(copyDatabase("test_db", "test_db_dummy"))
