from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson import json_util
from fastapi import APIRouter
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from datetime import datetime
from typing import Optional
from ...core import (
    settings
)

router = APIRouter(
    prefix="/schooling",
    tags=["SCHOOLING"]
)
class SchoolingPositif(BaseModel):
    case_id: str
    date_modified: str
    schooling_year: str
    school_commune_1:str
    patient_code:str
    infant_commune:str
    parent_patient_code:str
    dat_peyman_fet:Optional[str]
    gender:str
    agent_name:str
    infant_dob:str
    age:int
    sexe:str
    category:str
    quarter: str
    case_type:str
    closed:str
    
    #Method calculate the quarter based on dat_peyman_fet
    @property
    def calculate_quarter(self):
        date_peye = datetime.strptime(self.dat_peyman_fet, "%Y-%m-%dT%H:%M:%S.%fZ")
        quarter = (date_peye.month - 1) // 3 + 1
        return f"Q{quarter}"

client = MongoClient("mongodb+srv://jhonandre:Pass2Pass@cluster0.5p8etij.mongodb.net/caris_databridge")
db = client["caris_databridge"]
collection = db["schooling_enfant_positif"]
schooling_oev_collection = db["schooling_oev"]
schooling_siblings_collection = db["schooling_siblings"]
schooling_cwv_collection=db["schooling_cwv"]
#client = MongoClient(settings.mongo_uri)
#db = client[settings.mongodb_database]
#collection = db[settings.mongodb_collection]


@router.get("/schooling_analysis")
def read_schooling():
    try:
        
        positive_filter_condition = {"properties.schooling_year": "2022-2023",
                                      "closed":False,
                                      "properties.dat_peyman_fet": {"$exists": True}}
        positif_info = {
            "_id": 0,
            "case_id": 1,
            "date_modified": 1,
            "properties.schooling_year": 1,
            "properties.school_commune_1": 1,
            "properties.patient_code": 1,
            "properties.infant_commune": 1,
            "properties.gender": 1,
            "properties.infant_dob": 1,
            "properties.dat_peyman_fet":1,
            "properties.age": 1,
            "sexe": 1,
            "category": 1,
            "quarter": 1,
            "properties.case_type":1,
            "closed":1,
        }
        
        positive_data = list(collection.find(positive_filter_condition, positif_info))
        if not positive_data:
            raise HTTPException(status_code=404, detail="No data found")
        
        oev_filter_condition = {"properties.schooling_year": "2022-2023",
                                "closed":False,
                                "properties.dat_peyman_fet": {"$exists": True}}
        oev_info = {
            "_id": 0,
            "case_id": 1,
            "date_modified": 1,
            "properties.schooling_year": 1,
            "properties.school_commune_1": 1,
            "properties.patient_code": 1,
            "properties.infant_commune": 1,
            "properties.gender": 1,
            "properties.infant_dob": 1,
            "properties.dat_peyman_fet": 1,
            "properties.age": 1,
            "sexe": 1,
            "category": 1,
            "quarter": 1,
            "properties.case_type":1,
            "properties.parent_patient_code":1,
            "closed":1,
        }

        oev_data = list(schooling_oev_collection.find(oev_filter_condition, oev_info))
        if not oev_data:
            raise HTTPException(status_code=404, detail="No data found in the second collection")
        
        siblings_filter_condition = {"properties.schooling_year": "2022-2023",
                                     "closed":False,
                                     "properties.dat_peyman_fet": {"$exists": True}
                                     }
        siblings_info = {
            "_id": 0,
            "case_id": 1,
            "date_modified": 1,
            "properties.schooling_year": 1,
            "properties.school_commune": 1,
            "properties.patient_code": 1,
            "properties.infant_commune": 1,
            "properties.gender": 1,
            "properties.infant_dob": 1,
            "properties.dat_peyman_fet": 1,
            "properties.age": 1,
            "sexe": 1,
            "category": 1,
            "quarter": 1,
            "properties.case_type":1,
            "properties.parent_patient_code":1,
            "closed":1,
        }

        siblings_data = list(schooling_siblings_collection.find(siblings_filter_condition, siblings_info))
        if not siblings_data:
            raise HTTPException(status_code=404, detail="No data found in the second collection")
        #================================================
        
        cwv_filter_condition = {"properties.schooling_year": "2022-2023",
                                "closed":False,
                                "properties.dat_peyman_fet": {"$exists": True}}
        cwv_info = {
            "_id": 0,
            "case_id": 1,
            "date_modified": 1,
            "properties.schooling_year": 1,
            "properties.school_commune_1": 1,
            "properties.gender_sex": 1,
            "properties.dob": 1,
            "properties.dat_peyman_fet": 1,
            "properties.eske_peye": 1,
            "age": 1,
            "sexe": 1,
            "category": 1,
            "quarter": 1,
            "properties.case_type":1,
            "closed":1,
           
        }

        cwv_data = list(schooling_cwv_collection.find(cwv_filter_condition, cwv_info))
        if not cwv_data:
            raise HTTPException(status_code=404, detail="No data found in the second collection")  
        #================================================
        #Merge collections regarding schooling for analysis
        merged_data = {
            "schooling_positive": positive_data,
            "schooling_oev": oev_data,
            "schooling_siblings":siblings_data,
            "schooling_cwv":cwv_data,
        }

        #Retrieve data from schooling positive collection using a for loop

        for item in positive_data:
        # Skip this iteration and move to the next item for some documents without dat_peyman_fet    
            if "dat_peyman_fet" not in item["properties"] or item["properties"]["dat_peyman_fet"] is None:
                continue  # Skip this iteration and move to the next item

            dob = datetime.strptime(item["properties"]["infant_dob"], "%Y-%m-%d")
            age = (datetime.now() - dob).days // 365
            item["age"] = age
            item["sexe"] = "female" if item["properties"]["gender"] == "2" else ("male" if item["properties"]["gender"] == "1" else "")

            #Calculate different category of age
            if age < 1:
                item["category"] = "<1"
            elif 1 <= age < 5:
                item["category"] = "1-4"
            elif 5 <= age < 10:
                item["category"] = "5-9"
            elif 10 <= age < 15:
                item["category"] = "10-14"
            elif 15 <= age < 18:
                item["category"] = "15-17"
            elif age>=18:
                item["category"] = "18+"
            item["site"] = item["properties"]["patient_code"][:8]
            item["commune"] = item["properties"]["school_commune_1"]
            date_peye = datetime.strptime(item["properties"]["dat_peyman_fet"], "%Y-%m-%d")
            quarter = (date_peye.month - 1) // 3 + 1
            item["quarter"] = f"Q{quarter}"
            item["case_type"] = item["properties"]["case_type"]
            
            
        # Retrieve data from schooling oev collection using a for loop  
        for item in oev_data:
            if "parent_patient_code" in item["properties"]:
                item["mother_patient_code"] = item["properties"]["parent_patient_code"]
                dob = datetime.strptime(item["properties"]["infant_dob"], "%Y-%m-%d")
                age = (datetime.now() - dob).days // 365
                item["age"] = age
                item["sexe"] = "female" if item["properties"]["gender"] == "2" else ("male" if item["properties"]["gender"] == "1" else "")
            
        #Calculate different category of age
            if age < 1:
                item["category"] = "<1"
            elif 1 <= age < 5:
                item["category"] = "1-4"
            elif 5 <= age < 10:
                item["category"] = "5-9"
            elif 10 <= age < 15:
                item["category"] = "10-14"
            elif 15 <= age < 18:
                item["category"] = "15-17"
            elif age>=18:
                item["category"] = "18+"
            item["site"] = item["properties"]["parent_patient_code"][:8]
            item["commune"] = item["properties"]["school_commune_1"]
            item["case_type"] = item["properties"]["case_type"] 
            quarter = (date_peye.month - 1) // 3 + 1
            item["quarter"] = f"Q{quarter}"
              
        
        # Retrieve data from schooling siblings collection using a for loop  
        for item in siblings_data:
            if "parent_patient_code" in item["properties"]:
                item["positive_patient_code"] = item["properties"]["parent_patient_code"]
                dob = datetime.strptime(item["properties"]["infant_dob"], "%Y-%m-%d")
                age = (datetime.now() - dob).days // 365
                item["age"] = age
                item["sexe"] = "female" if item["properties"]["gender"] == "2" else ("male" if item["properties"]["gender"] == "1" else "")
            
        #Calculate different category of age for siblings
            if age < 1:
                item["category"] = "<1"
            elif 1 <= age < 5:
                item["category"] = "1-4"
            elif 5 <= age < 10:
                item["category"] = "5-9"
            elif 10 <= age < 15:
                item["category"] = "10-14"
            elif 15 <= age < 18:
                item["category"] = "15-17"
            elif age>=18:
                item["category"] = "18+"
            item["site"] = item["properties"]["parent_patient_code"][:8]
            item["commune"] = item["properties"]["school_commune"]
            item["case_type"] = item["properties"]["case_type"] 
            quarter = (date_peye.month - 1) // 3 + 1
            item["quarter"] = f"Q{quarter}"
            

        #=============================================
        # Retrieve data from schooling cwv collection using a for loop  
        for item in cwv_data:
                #"eske_peye" in item["properties"]:
                #item["eske_peye"] = item["properties"]["eske_peye"]
                dob_cwv = datetime.strptime(item["properties"]["dob"], "%Y-%m-%d")
                age = (datetime.now() - dob_cwv).days // 365
                item["age"] = age
                item["sexe"] = "female" if item["properties"]["gender_sex"] == "F" else ("male" if item["properties"]["gender_sex"] == "M" else "")
            
        #Calculate different category of age for cwv
                if age < 1:
                   item["category"] = "<1"
                elif 1 <= age < 5:
                  item["category"] = "1-4"
                elif 5 <= age < 10:
                  item["category"] = "5-9"
                elif 10 <= age < 15:
                  item["category"] = "10-14"
                elif 15 <= age < 18:
                  item["category"] = "15-17"
                elif age>=18:
                  item["category"] = "18+"
                item["commune"] = item["properties"]["school_commune_1"]
                item["case_type"] = item["properties"]["case_type"] 
                quarter = (date_peye.month - 1) // 3 + 1
                item["quarter"] = f"Q{quarter}"
            
        #=============================================    
        return JSONResponse(content=merged_data, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

            
    
        