from ast import List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from db.question import create_question, get_questions_by_form, get_questions_without_uid_by_form_id
from db.form import retrieveForm
import pandas as pd
from starlette.responses import StreamingResponse
from models.question import UpdateQuestions, Questions
from core.config import settings
import io
import requests
from dependencies import get_current_user_from_token

router = APIRouter(
    prefix="/form/questions",
    tags=["Question"],
    dependencies=[Depends(get_current_user_from_token)]
)

@router.post("/xlsx")
async def create_form_question_xlsx(form_type: str, form_name: str, file: UploadFile = File(...)):
    """
    Create new form question.
    """
    try:
        df = pd.read_excel(file.file.read())
        __dict = df.to_dict(orient='records')
        questions ={"questions":__dict,"form_name":form_name,"form_type":form_type}
        questions = Questions.parse_obj(questions)
        result = await create_question(questions)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"number of questions":len(result.inserted_ids)}
@router.get("/xlsx")
async def get_form_question_xlsx(form_name: str, form_type: str):
    """
    Get all questions.
    """
    try:
        __questions = await get_questions_by_form(form_name, form_type)
        if len(__questions) > 0:
            df = pd.DataFrame(__questions)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer) as writer:
                df.to_excel(writer, sheet_name='questions', index=False)
                writer.save()
            buffer.seek(0)
            headers = {"Content-Disposition": "attachment; filename="+form_type+"_"+form_name+"_questions.xlsx"}
            return StreamingResponse(buffer, headers=headers)
        else:
            raise HTTPException(status_code=404, detail="No questions found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
@router.get("/")
async def get_form_question(form_name: str, form_type: str):
    """
    Get all questions.
    """
    try:
        __questions = await get_questions_by_form(form_name, form_type)
        if len(__questions) > 0:
            return __questions
        else:
            raise HTTPException(status_code=404, detail="No questions found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/",summary=settings.QUESTIONS_SUMMARY,response_description=settings.QUESTIONS_DESCRIPTION, status_code=201)
async def questions(questions: Questions):
    result = await create_question(questions)
    return result

@router.put("/",summary=settings.QUESTION_UPDATE_SUMMARY, response_description=settings.QUESTION_UPDATE_DESCRIPTION)
async def update_questions(questions: UpdateQuestions):
    return {"questions": "questions"}

@router.post("/check/xlsx")
async def check_form(name:str,type:str, file : UploadFile = File(...)):
    df = pd.read_excel(file.file.read())
    df = df[:1]
    __df_col = df.transpose()
    __df_col["description"] = __df_col.index
    __df_col.columns = ["code","description"]
    old_questions = await get_questions_by_form(name,type)
    if isinstance(old_questions,dict):
        if old_questions["message"] == "Form not found":
            return {"message":"Form not found"}
    if len(old_questions) > 0:
        __df_old_questions = pd.DataFrame(old_questions)
        __old_join_new_by_description = pd.merge(__df_col,__df_old_questions,on="description",how="left")
        __new_join_old_by_description = pd.merge(__df_old_questions,__df_col,on="description",how="left")
        __new_join_old_by_code = pd.merge(__df_col,__df_old_questions,on="code",how="left")
        __old_join_new_by_code = pd.merge(__df_old_questions,__df_col,on="code",how="left")
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        __df_col.to_excel(writer, sheet_name="new", index=False)
        if len(old_questions) > 0:
            __df_old_questions.to_excel(writer, sheet_name="old", index=False)
            __old_join_new_by_description.to_excel(writer, sheet_name="old_join_new_by_description", index=False)
            __new_join_old_by_description.to_excel(writer, sheet_name="new_join_old_by_description", index=False)
            __new_join_old_by_code.to_excel(writer, sheet_name="new_join_old_by_code", index=False)
            __old_join_new_by_code.to_excel(writer, sheet_name="old_join_new_by_code", index=False)
    buffer.seek(0)
    # Download the file
    headers = {"Content-Disposition": "attachment; filename="+type+"_"+name+"_"+str(datetime.now())+".xlsx"}
    return StreamingResponse(buffer,headers=headers)

# @router.put("/form/{form_type}/{form_name}")

# async def update_form_question(form_type: str, form_name: str):
#     try:
#         __questions = await get_questions_by_form(form_name, form_type)
#         if len(__questions) > 0:
#             return {"questions": __questions}
#         else:
#             raise HTTPException(status_code=404, detail="No questions found")
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

@router.post("/form/{form_type}/{form_name}")
async def sync_form_questions(form_type: str, form_name: str):
    headers={'Content-type':'application/json', 'Accept':'application/json'}
    def reformat_question(question):
        question["code"] = form_type+"_"+form_name+"_"+question["code"]
        return question
    try:
        __form = await retrieveForm(form_name, form_type)
        if __form:
            __questions = await get_questions_without_uid_by_form_id(__form["_id"])
            if len(__questions) > 0:
                __questions = list(map(reformat_question,__questions))
                # res = requests.post(__form["questions_url_out"], json=__questions,headers=headers)
                # return res.json()
                return __questions
            else:
                raise HTTPException(status_code=404, detail="No questions found")
        else:
            raise HTTPException(status_code=404, detail="Form not found")

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))