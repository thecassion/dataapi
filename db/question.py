from db import db
from models.question import Questions, UpdateQuestions, Question, UpadateQuestion
from db.form import retrieveForm
import pymongo as pm

__questions = db["questions"]
__questions.create_index([("uid",pm.ASCENDING)], unique=True,name="question_uid_index")
__questions.create_index([("code",pm.ASCENDING),("form",pm.ASCENDING)], unique=True,name="form_question_code_index")
async def create_question(questions: Questions):
    """
    Create a new question
    """
    __form = await retrieveForm(name=questions.form_name,type=questions.form_type)
    if __form:
        # Insert  questions into the questions_collection
        __list_questions = [{**q.dict(), "form":__form.get("_id")} for q in questions.questions]
        result = await __questions.insert_many(__list_questions)
        return {"number_questions_saved":len(result.inserted_ids)}
    else:
        return {"message": "Form not found"}