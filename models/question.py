from pydantic import BaseModel, Field, EmailStr
from typing import Collection, Optional, List
from bson.objectid import ObjectId as BsonObjectId


class UpadateQuestion(BaseModel):
    code: Optional[str] = Field(..., title="name of the question")
    uid: str=Field(..., title="guuid of the question")
    description: Optional[str]=Field(None, title="label of the question")
    path: Optional[str]=Field(None, title="path of the question")

class Question(BaseModel):
    code: str=Field(..., title="name of the question")
    description: str=Field(None, title="label of the question")
    path: Optional[str]=Field(None, title="path of the question")
    uid: Optional[str]=Field(None, title="guuid of the question")
class UpdateQuestions(BaseModel):
    questions: List[UpadateQuestion] = Field(..., title="list of questions to be updated")
    form_name: str = Field(..., title="name of the form")
    form_type: str = Field(..., title="type of the form")
class Questions(BaseModel):
    questions: List[Question] = Field(..., title="list of questions")
    form_name: str = Field(..., title="name of the form")
    form_type: str = Field(..., title="type of the form")