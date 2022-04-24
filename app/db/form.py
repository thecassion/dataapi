
from typing import List
from ..db import db
from ..models.form import Form
from bson.objectid import ObjectId
form_collection = db.forms
import pymongo as pm


async def add_form_indexes():
    __list_indexes = [ index async for index in form_collection.list_indexes()]
    if "form_index" not in __list_indexes:
        await form_collection.create_index([("type",pm.ASCENDING),("name",pm.ASCENDING)], unique=True,name="form_index")

async def retrieveForm(name:str,type:str):
    __form = await form_collection.find_one({"$and":[{"name":name},{"type":type}]})
    if __form:
        return __form
async def get_form_by_id(id:str):
    if (form := await form_collection.find_one({"_id":ObjectId(id)})) is not None:
        return form
    else:
        raise Exception("Form not found")
async def retrieveForms()->List[Form]:
    __forms = await form_collection.find().to_list(100000)
    # __forms = [Form(f) for f in __forms]
    return __forms

async def createForm(form:Form)->Form:
    await add_form_indexes()
    result = await form_collection.insert_one(form.dict())
    if result:
        return result

async def createForms(forms:List[Form])->List[Form]:
    await add_form_indexes()
    __forms = [form.dict() for form in forms]
    result = await form_collection.insert_many(__forms)
    if result:
        return {"numer_forms_saved":len(result.inserted_ids)}

async def update_form(id:str,form:Form):
    await  add_form_indexes()
    __form = {k:v for k,v in form.dict().items() if v is not None}
    if len(__form)>0:
        update_result = await form_collection.update_one({"_id":ObjectId(id)},{"$set":__form})
        if update_result.modified_count == 1:
            if(
                update_form := await form_collection.find_one({"_id":ObjectId(id)})
            ) is not None:
                return update_form
    if (existing_form := await form_collection.find_one({"_id":ObjectId(id)})) is not None:
        return existing_form
    else:
        raise Exception("Form not found")

async def updateForms(forms:List[Form])->List[Form]:
    await add_form_indexes()
    result = await form_collection.insert_many(forms)
    if result:
        return Form(result)

async def deleteForm(name:str,type:str)->Form:
    result = await form_collection.delete_one({"name":name,"type":type})
    if result:
        return {"deleted":True}

async def updateForm(id:str,form:Form)->Form:
    await add_form_indexes()
    result = await form_collection.update_one({"name":form.name,"type":form.type},{"$set":form.dict()})
    if result:
        return form