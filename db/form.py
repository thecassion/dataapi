
from db import db
from models.form import Form

form_collection = db.forms
async def retrieveForm(name:str,type:str)->Form:
    return await Form(form_collection.find_one({"name":name,"type":type}))