from ..db import db
from ..models.form_data import FormData
from bson import ObjectId
import numpy as np
import math


data_collection = db["data"]

def format_data(data:dict)->dict:
    if '_id' in data:
        _id = data['_id']
        del data['_id']
    if 'form_id' in data:
        form_id = data['form_id']
        del data['form_id']
    new_data = {}
    for key, value in data.items():
        if isinstance(value, float) :
            if math.isnan(value):
                pass
            else:
                new_data[key] = value
        elif isinstance(value, str):
            if value == "":
                pass
            else:
                new_data[key] = value
        elif value is None:
            pass
        else:
            new_data[key] = value

    return {"_id":_id,"form_id":form_id,"data_in":new_data}
async def createData(data:list[dict]):
    data = map(format_data, data)
    result = await data_collection.insert_many(data)
    if result:
        return {"number inserted": len(result.inserted_ids)}

async def retrieveData(form_id:ObjectId, limit:int=50, skip:int=0)->list[dict]:
    data = await data_collection.find({"form_id":form_id},{"form_id":0}).limit(limit).skip(skip).to_list(length=100)
    return data
async def retrieveDataById(id:ObjectId)->dict:
    data = await data_collection.find_one({"_id":id},{"form_id":0,"_id":0})
    return data
async def retrieveDataByFormId(form_id:ObjectId, limit=100000)->list[dict]:
    data = await data_collection.find({"form_id":form_id},{"form_id":0}).to_list(length=limit)
    return data
### Retrieve data from a form data without success response or without response
async def retrieveDataByFormId_not_sent(form_id:ObjectId, limit=100000)->list[dict]:
    data = await data_collection.find({"$and":[{"form_id":form_id},{"data_out":{"$exists":True}},{"$or":[{"response.succes":False},{"response":{"$exists":False}}]}]},{"form_id":0}).to_list(length=limit)
    return data
async def retrieveDataByFormId_not_sent_regardless_data_out(form_id:ObjectId, limit=100000)->list[dict]:
    data = await data_collection.find({"$and":[{"form_id":form_id},{"$or":[{"response.succes":False},{"response":{"$exists":False}}]}]},{"form_id":0}).to_list(length=limit)
    return data

async def update_data(id:ObjectId, data:dict)->dict:
    result = await data_collection.update_one({"_id":id},{"$set":data})
    return result