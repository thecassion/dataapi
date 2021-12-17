import os
from fastapi import FastAPI, Body, HTTPException, status,File, UploadFile, Form
from fastapi.responses import JSONResponse
import pandas as pd
import json
from models.question import UpdateQuestions, Questions
from models.form import Form
from typing import  List
import pymongo as pm
from db import db
from db.form import createForms, retrieveForm, updateForm,createForm
from db.question import create_question


app = FastAPI(title="UNOPS DATA INTEGRATION", description="A data integration system that helps UNOPS send their data to USI system", version="0.1")


_forms = db["forms"]
_forms.create_index([("type",pm.ASCENDING),("name",pm.ASCENDING)], unique=True)

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

@app.post("/questions",summary="create a list of questions on our server and the output server", response_model=List[dict],response_description="Create a list of questions on our server and the output server", status_code=201)
async def questions(questions: Questions):
    result = await create_question(questions)
    return result

@app.put("/questions")
async def update_questions(questions: UpdateQuestions):

    return {"questions": "questions"}

@app.post("/form")
async def create_form(form_data: Form):
    result = await createForm(form_data)
    return JSONResponse(content=result.to_dict())
@app.put("/form")
async def update_form(form_data: Form):
    result = await updateForm(form_data)
    return JSONResponse(content=result.to_dict())

@app.post("/forms")
async def create_forms(forms: List[Form]):
    result = await createForms(forms)
    return JSONResponse(content=result.to_dict())
