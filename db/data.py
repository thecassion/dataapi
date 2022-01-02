from db import db
from models.form_data import FormData
from bson import ObjectId


data_collection = db["data"]

def format_data(data:dict)->dict:
    if '_id' in data:
        _id = data['_id']
        del data['_id']
    if 'form_id' in data:
        form_id = data['form_id']
        del data['form_id']
    return {"_id":_id,"form_id":form_id,"data_in":data}
async def createData(data:list[dict]):
    data = map(format_data, data)
    result = await data_collection.insert_many(data)
    if result:
        return {"number inserted": len(result.inserted_ids)}