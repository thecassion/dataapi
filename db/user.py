from typing import List
from db import db
from core.hashing import Hasher
from models.user import User, ShowUser, UserCreate

user_collection = db.users

async def createUser(user:UserCreate)->UserCreate:
    __user = user.dict()
    __user['password'] = Hasher.get_password_hash(__user.password)
    __usercreate = await user_collection.insert_one(__user.dict())
    if __usercreate:
        return __usercreate
    
    
async def retrieveUser(username: str,email:str)->ShowUser:
    __user_FindOne = await user_collection.find_one({"username":username},{"email":email})
    if __user_FindOne:
        return __user_FindOne

async def retrieveUsers()-> List[ShowUser]:
    __users = await user_collection.find().to_list(100000)
    if __users:
        return __users

