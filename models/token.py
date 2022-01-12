from pydantic import BaseModel, EmailStr
from typing import Optional


class Token(BaseModel):
    access_token: str
    token_type: str
    
class TokenData(BaseModel):
    username: Optional[str]
    #email: EmailStr
    #is_active: Optional[bool]
    #is_superUser: Optional[bool]
    
    
#class TokenAdminData(BaseModel):
#    username: Optional[str]
#    is_superUser: Optional[bool]
