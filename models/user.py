from typing import Optional
from pydantic import BaseModel,EmailStr, Field 


class User(BaseModel):
    username: str = Field(...,title='username of the USER')
    email: EmailStr = Field(...,title='email of the USER')
    password: str = Field(...,title=' hashed password of the USER')
    is_active: bool = Field(True,title='is the USER active', hidden_from_schema=True,hidden=True)
    is_superUser: bool = Field(False,title='does the USER have admin credentials')


class UserCreate(User):
    username : str = Field(...,title='username of the USER')
    email : EmailStr = Field(...,title='email of the USER')
    password : str = Field(...,title=' hashed password of the USER')
    is_superUser: bool = Field(False,title='does the USER have admin credentials')



class ShowUser(User):
    username : str = Field(...,title='username of the USER')
    email : EmailStr = Field(...,title='email of the USER')
    is_active: bool  = Field(True,title='is the USER active')
    is_superUser: bool = Field(False,title='does the USER have admin credentials')
