import os
import pymongo
from .settings import settings
# client = motor.motor_asyncio.AsyncIOMotorClient(os.environ.get('MONGODB_URI'))
#client = pymongo.MongoClient(os.environ.get('MONGODB_URI'))
client = pymongo.MongoClient(settings.mongodb_uri)
db = client["caris_databridge"]