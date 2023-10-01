from ...core import (
    settings
)
from pymongo import MongoClient


client = MongoClient(settings.mongodb_uri)
# client = MongoClient("mongodb+srv://jhonandre:Pass2Pass@cluster0.5p8etij.mongodb.net/caris_databridge")

db = client["caris_databridge"]
collection = db["schooling_enfant_positif"]
schooling_oev_collection = db["schooling_oev"]
schooling_siblings_collection = db["schooling_siblings"]
schooling_cwv_collection = db["schooling_cwv"]
schooling_dreams_collection = db["schooling_dreams"]
