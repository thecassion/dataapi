import os
from fastapi import FastAPI, Body, HTTPException, status,File, UploadFile, Form
from fastapi.responses import JSONResponse, FileResponse
import pandas as pd
import json

from starlette.responses import StreamingResponse
from models.question import UpdateQuestions, Questions
from models.form import Form
from typing import  List
import pymongo as pm
from db import db
from db.form import createForms, retrieveForm, updateForm,createForm, retrieveForms
from db.question import create_question, get_questions_by_form
from utils.data import dataInToDataOut
import io

app = FastAPI(title="UNOPS DATA INTEGRATION", description="A data integration system that helps UNOPS send their data to USI system", version="0.1")


_forms = db["forms"]
_forms.create_index([("type",pm.ASCENDING),("name",pm.ASCENDING)], unique=True,name="form_index")
_form_data_in = db["form_data_in"]
_form_data_out = db["form_data_out"]

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/bulksuploadformdata", response_model=List[dict],response_description="Create form data_in in the server using an xlsx, json , csv or xml format", status_code=201, summary="Create form data_in in the server using an xlsx, json , csv or xml format")
async def form(file: UploadFile = File(...)):
    df = pd.read_excel(file.file.read(), header=1)
    df["_id"] = df.site_identifier_11
    df["_id_site_identifier_12"]=df.site_identifier_12
    df.site_identifier_12 = df.site_identifier_12.astype(str)
    _json = json.loads(df.to_json(orient='records'))
    await _forms.insert_many(_json)
    return JSONResponse(content=_json)

@app.post("/questions",summary="create a list of questions on our server and the output server",response_description="Create a list of questions on our server and the output server", status_code=201)
async def questions(questions: Questions):
    result = await create_question(questions)
    return result

@app.put("/questions")
async def update_questions(questions: UpdateQuestions):

    return {"questions": "questions"}

@app.post("/form")
async def create_form(form_data: Form):
    result = await createForm(form_data)
    return JSONResponse(content=result.dict())
@app.put("/form")
async def update_form(form_data: Form):
    result = await updateForm(form_data)
    return JSONResponse(content=result.dict())

@app.post("/forms")
async def create_forms(forms: List[Form]):
    result = await createForms(forms)
    return result
@app.get("/forms", response_model=List[Form],response_description="Create a list of forms on our server and the output server", status_code=201, summary="Create a list of forms on our server and the output server")
async def retrieve_forms():
    result = await retrieveForms()
    return result

@app.put("/tsform")
async def transform_data_in_to_data_out(name:str,type:str):
    result = await retrieveForm(name,type)
    result_1 = dataInToDataOut(result.get("data_in"),result.get('format_in'),result.get("format_out"))
    return JSONResponse(content=result_1)

@app.post("/checkform")
async def check_form(name:str,type:str, file : UploadFile = File(...)):
    result = await retrieveForm(name,type)
    df = pd.read_excel(file.file.read())
    df = df[:1]
    __df_col = df.transpose()
    __df_col["description"] = __df_col.index
    __df_col.columns = ["code","description"]
    old_questions = await get_questions_by_form(name,type)
    __df_old_questions = pd.DataFrame(old_questions)
    __df_col = pd.merge(__df_col,__df_old_questions,on="description",how="left")
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        __df_col.to_excel(writer, sheet_name="new", index=False)
        __df_old_questions.to_excel(writer, sheet_name="old", index=False)
    buffer.seek(0)
    # __my_json = json.loads(__df_col.to_json(orient='records'))
    headers = {"Content-Disposition": "attachment; filename="+type+"_"+name+".xlsx"}
    return StreamingResponse(buffer,headers=headers)
    return FileResponse(type+"_"+name+".xlsx",media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.post("/insert_columndata_from_xlsx")
async def column_data_from_xlsx(name:str,type:str, file : UploadFile = File(...)):
    result = await retrieveForm(name,type)
    df = pd.read_excel(file.file.read())
    df = df[:1]
    __df_col = df.transpose()
    __df_col["description"] = __df_col.index
    __df_col.columns = ["code","description"]
    old_questions = await get_questions_by_form(name,type)
    __df_old_questions = pd.DataFrame(old_questions)
    __df_col = pd.merge(__df_col,__df_old_questions,on="description",how="left")
    __df_col = __df_col.to_excel(type+"_"+name+".xlsx")
    __df_old_questions.to_excel(type+"_"+name+".xlsx")

    __my_json = json.loads(__df_col.to_json(orient='records'))
    return __my_json

