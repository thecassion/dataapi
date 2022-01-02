
from typing import List
from db import db
from models.form import Form

form_collection = db.forms
async def retrieveForm(name:str,type:str)->Form:
    __form = await form_collection.find_one({"$and":[{"name":name},{"type":type}]})
    if __form:
        return __form
async def retrieveForms()->List[Form]:
    __forms = await form_collection.find().to_list(100000)
    # __forms = [Form(f) for f in __forms]
    return __forms

async def createForm(form:Form)->Form:
    result = await form_collection.insert_one(form.dict())
    if result:
        return result

async def createForms(forms:List[Form])->List[Form]:
    __forms = [form.dict() for form in forms]
    result = await form_collection.insert_many(__forms)
    if result:
        return {"numer_forms_saved":len(result.inserted_ids)}

async def updateForm(form:Form)->Form:
    result = await form_collection.update_one({"name":form.name,"type":form.type},{"$set":form.dict()})
    if result:
        return form

async def updateForms(forms:List[Form])->List[Form]:
    result = await form_collection.insert_many(forms)
    if result:
        return forms

async def deleteForm(name:str,type:str)->Form:
    result = await form_collection.delete_one({"name":name,"type":type})
    if result:
        return {"deleted":True}
