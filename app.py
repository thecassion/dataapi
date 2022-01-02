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

from starlette.responses import StreamingResponse
from models.question import UpdateQuestions, Questions
from models.form import Form
from models.user import User, RegisterUser, RegisterAdmin
from models.token import Token, TokenData
from typing import  List
import pymongo as pm
from db import db
from db.form import createForms, retrieveForm, updateForm,createForm, retrieveForms
from db.question import create_question, get_questions_by_form
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
import io
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from core.config import settings
from core.hashing import Hasher
from routers.form_data import router as data_router

app = FastAPI(title=settings.PROJECT_TITLE, description=settings.PORJECT_DESCRIPTION, version=settings.PROJECT_VERSION, docs_url=settings.DOCS_URL)

app.include_router(data_router)
_forms = db["forms"]
_forms.create_index([("type",pm.ASCENDING),("name",pm.ASCENDING)], unique=True,name="form_index")

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

async def authenticate_user(username:str,password:str)->User:
    _user =  await getUserByUsername(username)
    print(_user)
    if not _user:
        return False
    if not Hasher.verify_password(password,_user.password):
        return False
    return _user

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


async def get_current_user_from_token(token:str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials")
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = await getUserByUsername(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user




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

@app.post("/bulksuploadformdata", response_model=List[dict],response_description=settings.BULKUPLOADFORMDATA_DESCRIPTION, status_code=201, summary=settings.BULKUPLOADFORMDATA_SUMMARY,tags=['Data Processing'])
async def form(file: UploadFile = File(...),current_user: User=Depends(get_current_user_from_token)):
    df = pd.read_excel(file.file.read(), header=1)
    df["_id"] = df.site_identifier_11
    df["_id_site_identifier_12"]=df.site_identifier_12
    df.site_identifier_12 = df.site_identifier_12.astype(str)
    _json = json.loads(df.to_json(orient='records'))
    await _forms.insert_many(_json)
    return JSONResponse(content=_json)

@app.post("/questions",summary=settings.QUESTIONS_SUMMARY,response_description=settings.QUESTIONS_DESCRIPTION, status_code=201, tags=['Question'])
async def questions(questions: Questions,current_user: User=Depends(get_current_user_from_token)):
    result = await create_question(questions)
    return result

@app.put("/questions",summary=settings.QUESTION_UPDATE_SUMMARY, response_description=settings.QUESTION_UPDATE_DESCRIPTION, tags=['Question'])
async def update_questions(questions: UpdateQuestions,current_user: User=Depends(get_current_user_from_token)):
    return {"questions": "questions"}

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

@app.post("/checkform/xlsx", tags=['Form'])
async def check_form(name:str,type:str, file : UploadFile = File(...),current_user: User=Depends(get_current_user_from_token)):
    df = pd.read_excel(file.file.read())
    df = df[:1]
    __df_col = df.transpose()
    __df_col["description"] = __df_col.index
    __df_col.columns = ["code","description"]
    old_questions = await get_questions_by_form(name,type)
    if isinstance(old_questions,dict):
        if old_questions["message"] == "Form not found":
            return {"message":"Form not found"}
    if len(old_questions) > 0:
        __df_old_questions = pd.DataFrame(old_questions)
        __old_join_new_by_description = pd.merge(__df_col,__df_old_questions,on="description",how="left")
        __new_join_old_by_description = pd.merge(__df_old_questions,__df_col,on="description",how="left")
        __new_join_old_by_code = pd.merge(__df_col,__df_old_questions,on="code",how="left")
        __old_join_new_by_code = pd.merge(__df_old_questions,__df_col,on="code",how="left")
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        __df_col.to_excel(writer, sheet_name="new", index=False)
        if len(old_questions) > 0:
            __df_old_questions.to_excel(writer, sheet_name="old", index=False)
            __old_join_new_by_description.to_excel(writer, sheet_name="old_join_new_by_description", index=False)
            __new_join_old_by_description.to_excel(writer, sheet_name="new_join_old_by_description", index=False)
            __new_join_old_by_code.to_excel(writer, sheet_name="new_join_old_by_code", index=False)
            __old_join_new_by_code.to_excel(writer, sheet_name="old_join_new_by_code", index=False)
    buffer.seek(0)
    # Download the file
    headers = {"Content-Disposition": "attachment; filename="+type+"_"+name+"_"+str(datetime.now())+".xlsx"}
    return StreamingResponse(buffer,headers=headers)

@app.post("/form/questions/xlsx", response_description=settings.FORM_QUESTIONS_DESCRIPTION, status_code=201, summary=settings.FORM_QUESTIONS_SUMMARY, tags=['Form'])
async def column_data_from_xlsx(name:str,type:str, file : UploadFile = File(...),current_user: User=Depends(get_current_user_from_token)):
    result = await retrieveForm(name,type)
    df = pd.read_excel(file.file.read())
    __my_json = json.loads(df.to_json(orient='records'))
    return __my_json

