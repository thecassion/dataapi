from db import db
from models.question import Questions, UpdateQuestions, Question, UpadateQuestion
from db.form import retrieveForm
import pymongo as pm

async def create_question(questions: Questions):
    """
    Create a new question
    """
    __form_obj = await retrieveForm(questions.form_name,questions.form_type)
    __form = __form_obj.to_dict()
    if __form:
        questions_collection =__form["questions"]
        # Create unique index for the questions_collection
        questions_collection.create_index([("guuid",pm.ASCENDING)], unique=True)
        questions_collection.create_index([("name",pm.ASCENDING)], unique=True)
        # Insert  questions into the questions_collection
        result = await questions_collection.insert_many(questions.questions)
        return result
    else:
        return {"message": "Form not found"}