from typing import Optional, ClassVar
from pydantic import BaseModel,EmailStr, Field 


class User(BaseModel):
    username: str = Field(...,title='username of the USER')
    email: EmailStr = Field(...,title='email of the USER')
    password: str = Field(...,title=' hashed password of the USER')
    is_active: Optional[bool] = Field(True,title='is the USER active')
    is_superUser: Optional[bool] = Field(False,title='does the USER have admin credentials')



class RegisterUser(BaseModel):
    username: str 
    email: EmailStr 
    password: str 
    is_active: ClassVar[bool] = True
    is_superUser: ClassVar[bool] = False



class RegisterAdmin(BaseModel):
    username: str 
    email: EmailStr 
    password: str 
    is_active: ClassVar[bool] = True
    is_superUser: ClassVar[bool] = True
    


