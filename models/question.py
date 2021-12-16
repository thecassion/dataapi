from pydantic import BaseModel, Field, EmailStr
from typing import Collection, Optional, List


class UpadateQuestion(BaseModel):
    name: Optional[str] = Field(None, title="name of the question")
    guuid: str=Field(None, title="guuid of the question")
    label: Optional[str]=Field(None, title="label of the question")
    path: Optional[str]=Field(None, title="path of the question")

class Question(BaseModel):
    name: str=Field(None, title="name of the question")
    label: str=Field(None, title="label of the question")
    path: str=Field(None, title="path of the question")

class UpdateQuestions(BaseModel):
    questions: List[UpadateQuestion] = Field(..., title="list of questions to be updated")
    form_name: str = Field(..., title="name of the form")
    form_type: str = Field(..., title="type of the form")
class Questions(BaseModel):
    questions: List[Question] = Field(..., title="list of questions")
    form_name: str = Field(..., title="name of the form")
    form_type: str = Field(..., title="type of the form")