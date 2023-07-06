from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, List


class SchoolingPositif(BaseModel):
    case_id: str
    date_modified: str
    schooling_year: str
    school_commune_1: str
    patient_code: str
    infant_commune: str
    parent_patient_code: str
    dat_peyman_fet: Optional[str]
    eskew_peye: Optional[str]
    gender: str
    agent_name: str
    infant_dob: str
    age: int
    sexe: str
    commune: str
    category: str
    quarter: str
    case_type: str
    closed: str

    # Method calculate the quarter based on dat_peyman_fet
    @property
    def calculate_quarter(dat_peyman_fet):
        date_peye = datetime.strptime(dat_peyman_fet, "%Y-%m-%d")
        quarter = (date_peye.month - 1) // 3 + 1
        year = date_peye.year
        return f"Q{quarter}{year}"

    # Function calculate the the category of age
    @property
    def calculate_age_category(age: int) -> str:
        if age < 1:
            return "<1"
        elif 1 <= age < 5:
            return "1-4"
        elif 5 <= age < 10:
            return "5-9"
        elif 10 <= age < 15:
            return "10-14"
        elif 15 <= age < 18:
            return "15-17"
        elif 18 <= age < 20:
            return "18-20"
        else:
            return '20+'
