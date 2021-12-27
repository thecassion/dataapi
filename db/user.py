from typing import List
from db import db
from core.hashing import Hasher
from models.user import User, ShowUser, UserCreate

user_collection = db.users

async def createUser(user: dict)-> User:
    _user = dict(user)
    _hashed_password = Hasher().get_password_hash(_user.get('password'))
    _user['password'] = _hashed_password
    _usercreate = await user_collection.insert_one(_user)
    _new_user = await user_collection.find_one({"_id": _usercreate.inserted_id})
    if _new_user:
        return User(**_new_user)
    
    
async def retrieveUser(username: str,email:str)->ShowUser:
    __user_FindOne = await user_collection.find_one({"username":username},{"email":email})
    if __user_FindOne:
        return __user_FindOne

async def retrieveUsers()-> List[ShowUser]:
    __users = await user_collection.find().to_list(100000)
    if __users:
        return __users

