import os
from fastapi import (
    FastAPI,
    Body,
    HTTPException,
    status,
    File,
    UploadFile,
    Form,
    Depends
)
from fastapi.responses import JSONResponse, FileResponse
import pandas as pd
import json
from pydantic import EmailStr
from typing import Optional

from models.form import Form
from models.user import User, RegisterUser, RegisterAdmin
from models.token import Token, TokenData
from typing import  List
import pymongo as pm
from db import db
from db.form import createForms, retrieveForm, updateForm,createForm, retrieveForms
from db.user import (
    createUser,
    getUsers,
    getUserByEmail,
    getUserByUsername,
    updateUser,
    updateUser_byEmail,
    delUserByEmail,
    delUserByUsername,
    registerUser,
    registerAdmin
)
from utils.data import dataInToDataOut
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from core.config import settings
from core.hashing import Hasher
from routers.form_data import router as data_router
from routers.questions import router as questions_router

app = FastAPI(title=settings.PROJECT_TITLE, description=settings.PORJECT_DESCRIPTION, version=settings.PROJECT_VERSION, docs_url=settings.DOCS_URL)

app.include_router(data_router)
app.include_router(questions_router)
_forms = db["forms"]
_forms.create_index([("type",pm.ASCENDING),("name",pm.ASCENDING)], unique=True,name="form_index")

from dependencies import oauth2_scheme, get_current_user_from_token, authenticate_user,create_access_token
# @app.get("/", tags=['Home'])
# async def root():
#     return {"message": "Please write /docs into the browser path to enter to openapi"}




################ Admin


@app.get("/admin/user/{username}", response_model=User,response_description=settings.GET_USER_DESCRIPTION, summary=settings.GET_USER_SUMMARY, status_code=status.HTTP_201_CREATED, tags=['USER'])
async def get_userByUsername(username:str)->User:
    _user = await getUserByUsername(username)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong wwith {username}")





@app.post("/admin/createUser", response_model=User ,response_description=settings.CREATE_USER_DESCRIPTION, summary=settings.CREATE_USER_SUMMARY, status_code=status.HTTP_201_CREATED, tags=['USER'])
async def post_user(user:User)->User:
    _user = user.dict()
    _resUser = await createUser(_user)
    if _resUser:
        return _resUser
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")



@app.get("/admin/user/{email}/", response_model=User, response_description=settings.GET_USER_DESCRIPTION, summary=settings.GET_USER_SUMMARY,status_code=status.HTTP_201_CREATED, tags=['USER'])
async def get_userByEmail(email: EmailStr)->User:
    _user = await getUserByEmail(email)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong wwith {email}")



@app.get('/admin/users', response_model=List[User], response_description=settings.GET_USERS_DESCRIPTION, summary=settings.GET_USERS_SUMMARY,status_code=status.HTTP_201_CREATED, tags=['USER'])
async def get_users()->List[User]:
    _users = await getUsers()
    if _users:
        return _users
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")



@app.put("/admin/update_user/{username}", response_model=User,response_description=settings.UPDATE_USER_DESCRIPTION, summary=settings.UPDATE_USER_SUMMARY, status_code=status.HTTP_201_CREATED,  tags=['USER'])
async def put_user(username:str,email:EmailStr,password:str, is_active:Optional[bool], is_superUser:Optional[bool])->User:
    _user = await updateUser(username,email,password,is_active,is_superUser)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {username}")


@app.put("/admin/update_user/{email}/", response_model=User,response_description=settings.UPDATE_USER_DESCRIPTION, summary=settings.UPDATE_USER_SUMMARY, status_code=status.HTTP_201_CREATED,  tags=['USER'])
async def update_user(username:str,email:EmailStr,password:str, is_active:Optional[bool], is_superUser:Optional[bool])->User:
    _user = await updateUser_byEmail(username,email,password,is_active,is_superUser)
    if _user:
        return _user
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {email}")



@app.delete("/admin/delete_user/{email}/", response_description=settings.DELETE_USER_DESCRIPTION, summary=settings.DELETE_USER_SUMMARY, status_code=status.HTTP_201_CREATED, tags=['USER'])
async def delete_userByEmail(email:EmailStr)-> dict:
    _userdeleted = await delUserByEmail(email)
    if _userdeleted:
        return {"Message": f"the user with the {email} has been deleted" }
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {email}")



@app.delete("/admin/delete_user/{username}", response_description=settings.DELETE_USER_DESCRIPTION, summary=settings.DELETE_USER_SUMMARY, status_code=status.HTTP_201_CREATED, tags=['USER'])
async def delete_userByUsername(username:str)->dict:
    _userdeleted = await delUserByUsername(username)
    if _userdeleted:
        return {"Message": f"the user with the {username} has been deleted" }
    raise HTTPException(status.HTTP_404_NOT_FOUND, f"Something went wrong with {username}")



############### Registration


@app.post('/register', response_model=RegisterUser, response_description=settings.REGISTRATION_DESCRIPTION, summary=settings.REGISTRATION_SUMMARY, status_code=status.HTTP_201_CREATED, tags=['Register'])
async def register(user: RegisterUser)->RegisterUser:
    _user = user.dict()
    _resUser = await registerUser(_user)
    if _resUser:
        return _resUser
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")



@app.post('/admin/register', response_model=RegisterAdmin, response_description=settings.REGISTER_ADMIN_DESCRIPTION, summary=settings.REGISTER_ADMIN_SUMMARY, status_code=status.HTTP_201_CREATED, tags=['Register'])
async def register_admin(admin: RegisterAdmin)->RegisterAdmin:
    _user = admin.dict()
    _resUser = await registerAdmin(_user)
    if _resUser:
        return _resUser
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Something went wrong")


##################### helper


################# /login/token

@app.post("/token" , response_description=settings.LOGIN_DESCRIPTION, summary=settings.LOGIN_SUMMARY, response_model=Token, status_code=status.HTTP_201_CREATED, tags=['LOGIN'])
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    username = form_data.username
    password = form_data.password
    _user = await authenticate_user(username, password)
    if not _user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": username}, expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"access_token": access_token, "token_type": "bearer"}

















################


@app.post("/form", tags=['Form'])
async def create_form(form_data: Form,current_user: User=Depends(get_current_user_from_token)):
    result = await createForm(form_data)
    return JSONResponse(content=result.dict())

@app.put("/form", tags=['Form'])
async def update_form(form_data: Form,current_user: User=Depends(get_current_user_from_token)):
    result = await updateForm(form_data)
    return JSONResponse(content=result.dict())

@app.post("/forms",tags=['Form'])
async def create_forms(forms: List[Form],current_user: User=Depends(get_current_user_from_token)):
    result = await createForms(forms)
    return result

@app.get("/forms", response_model=List[Form],response_description=settings.FORMS_DESCRIPTION, status_code=201, summary=settings.FORMS_SUMMARY, tags=['Form'])
async def retrieve_forms(current_user: User=Depends(get_current_user_from_token)):
    result = await retrieveForms()
    return result

@app.put("/tsform", tags=['Form'])
async def transform_data_in_to_data_out(name:str,type:str,current_user: User=Depends(get_current_user_from_token)):
    result = await retrieveForm(name,type)
    result_1 = dataInToDataOut(result.get("data_in"),result.get('format_in'),result.get("format_out"))
    return JSONResponse(content=result_1)


