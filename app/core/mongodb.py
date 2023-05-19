import motor.motor_asyncio
import os
import pymongo
# client = motor.motor_asyncio.AsyncIOMotorClient(os.environ.get('MONGODB_URI'))
client = pymongo.MongoClient(os.environ.get('MONGODB_URI'))
db = client["caris_databridge"]