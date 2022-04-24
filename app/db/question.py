from functools import partial
from bson import objectid
from ..db import db
from ..models.question import Questions, UpdateQuestions, Question, UpadateQuestion
from ..db.form import retrieveForm
import pymongo as pm


__questions = db.questions


async def add_question_indexes():
    """
    Add index to the question collection
    """
    try:
        __indexes_info =  await __questions.index_information()
        if __indexes_info:
            if "form_question_code_index" not in __indexes_info:
                await __questions.create_index([("code",1),("form",1)], unique=True,name="form_question_code_index")
            if "question_uid_index" not in __indexes_info:
               await  __questions.create_index([("uid",pm.ASCENDING)], unique=True,name="question_uid_index", partialFilterExpression={"uid": {"$exists": True}})
        return {"message":"Indexes added"}
    except Exception as e:
        return {"message": str(e)}

async def create_question(questions: Questions):
    """
    Create a new question
    """
    await add_question_indexes()
    __form = await retrieveForm(name=questions.form_name,type=questions.form_type)
    if __form:
        # Insert  questions into the questions_collection
        __list_questions = [{**q.dict(), "form":__form.get("_id")} for q in questions.questions]
        result = await __questions.insert_many(__list_questions)
        return {"number_questions_saved":len(result.inserted_ids)}
    else:
        return {"message": "Form not found"}

async def get_questions_by_form(form_name:str,form_type:str):
    """
    Get all questions by form
    """
    try:
        __form = await retrieveForm(name=form_name,type=form_type)
        if __form:
            result = await __questions.find({"form":__form.get("_id")},{"form":0,"_id":0.}).to_list(100000)
            return result
        else:
            return {"message": "Form not found"}
    except Exception as e:
        return {"message": str(e)}

async def get_questions_by_form_id(form_id:objectid):
    """
    Get all questions by form id
    """
    try:
        result = await __questions.find({"form":form_id},{"form":0,"_id":0}).to_list(100000)
        return result
    except Exception as e:
        return {"message": str(e)}

async def get_questions_without_uid_by_form_id(form_id:objectid):
    """
        Get all questions by form id that is not sync with server

    """
    await add_question_indexes()
    try:
        result = await __questions.find({"$and":[{"form":form_id},{"uid":{ "$exists":False}}]},{"form":0,"path":0}).to_list(100000)
        return result
    except Exception as e:
        return {"message": str(e)}

async def update_question_uid(ques):
    """
    Update question uid
    """
    try:
        result = await __questions.update_one({"_id":ques["_id"]},{"$set":{"uid":ques["uid"]}})
        return result
    except Exception as e:
        raise e
