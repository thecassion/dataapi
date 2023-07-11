from fastapi import HTTPException
from datetime import datetime
from .db import (
    collection,
    schooling_oev_collection,
    schooling_siblings_collection,
    schooling_cwv_collection,
    schooling_dreams_collection
)
from .schema import SchoolingPositif
from itertools import groupby
from operator import itemgetter


positive_filter_condition = {
    "properties.schooling_year": {"$in": ["2022-2023", "2023-2024"]},
    "closed": False,
    "properties.eskew_peye": {"$in": ["wi", "1"]},
    "properties.dat_peyman_fet": {"$exists": True}
}
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
    "properties.dat_peyman_fet": 1,
    "properties.eskew_peye": 1,
    "properties.age": 1,
    "sexe": 1,
    "category": 1,
    "quarter": 1,
    "properties.case_type": 1,
    "closed": 1,
}

positive_data = list(collection.find(
    positive_filter_condition, positif_info))
if not positive_data:
    raise HTTPException(status_code=404, detail="No data found")

oev_filter_condition = {"properties.schooling_year": {"$in": ["2022-2023", "2023-2024"]},
                        "closed": False,
                        "properties.eskew_peye": {"$in": ["wi", "1"]},
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
    "properties.eskew_peye": 1,
    "properties.age": 1,
    "sexe": 1,
    "category": 1,
    "quarter": 1,
    "properties.case_type": 1,
    "properties.parent_patient_code": 1,
    "closed": 1,
}

oev_data = list(schooling_oev_collection.find(
    oev_filter_condition, oev_info))
if not oev_data:
    raise HTTPException(
        status_code=404, detail="No data found in the second collection")

siblings_filter_condition = {"properties.schooling_year": {"$in": ["2022-2023", "2023-2024"]},
                             "closed": False,
                             "properties.eskew_peye": {"$in": ["wi", "1"]},
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
    "properties.eskew_peye": 1,
    "properties.age": 1,
    "sexe": 1,
    "category": 1,
    "quarter": 1,
    "properties.case_type": 1,
    "properties.parent_patient_code": 1,
    "closed": 1,
}

siblings_data = list(schooling_siblings_collection.find(
    siblings_filter_condition, siblings_info))
if not siblings_data:
    raise HTTPException(
        status_code=404, detail="No data found in the second collection")

cwv_filter_condition = {"properties.schooling_year": {"$in": ["2022-2023", "2023-2024"]},
                        "closed": False,
                        "properties.eskew_peye": {"$in": ["wi", "1"]},
                        "properties.dat_peyman_fet": {"$exists": True}
                        }
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
    "properties.case_type": 1,
    "closed": 1,

}

cwv_data = list(schooling_cwv_collection.find(
    cwv_filter_condition, cwv_info))
if not cwv_data:
    raise HTTPException(
        status_code=404, detail="No data found in the second collection")
# ==============================================================================================
dreams_filter_condition = {"properties.schooling_year": {"$in": ["2022-2023", "2023-2024"]},
                           "closed": False,
                           "properties.eskew_peye": {"$in": ["wi", "1"]},
                           "properties.dat_peyman_fet": {"$exists": True}
                           }
dreams_info = {
    "_id": 0,
    "case_id": 1,
    "date_modified": 1,
    "properties.schooling_year": 1,
    "properties.dreams_code": 1,
    "properties.school_commune_1": 1,
    "properties.gender": 1,
    "properties.infant_dob": 1,
    "properties.dat_peyman_fet": 1,
    "properties.eske_peye": 1,
    "age": 1,
    "sexe": 1,
    "category": 1,
    "quarter": 1,
    "properties.case_type": 1,
    "closed": 1,

}

dreams_data = list(schooling_dreams_collection.find(
    dreams_filter_condition, dreams_info))
if not dreams_data:
    raise HTTPException(
        status_code=404, detail="No data found in the second collection")

# ==============================================================================================

for item in positive_data:
    # Skip this iteration and move to the next item for some documents without dat_peyman_fet
    if "dat_peyman_fet" not in item["properties"] or item["properties"]["dat_peyman_fet"] is None:
        continue  # Skip this iteration and move to the next item

    dob = datetime.strptime(
        item["properties"]["infant_dob"], "%Y-%m-%d")
    age = (datetime.now() - dob).days // 365
    item["age"] = age
    item["sexe"] = "female" if item["properties"]["gender"] == "2" else (
        "male" if item["properties"]["gender"] == "1" else "")

    # Calculate different category of age
    item["category"] = SchoolingPositif.calculate_age_category.fget(
        age)

    item["site"] = item["properties"]["patient_code"][:8]
    item["commune"] = item["properties"]["school_commune_1"]
    date_peye = datetime.strptime(
        item["properties"]["dat_peyman_fet"], "%Y-%m-%d")
    quarter = (date_peye.month - 1) // 3 + 1
    item["quarter"] = f"Q{quarter}"
    item["case_type"] = item["properties"]["case_type"]

# Retrieve data from schooling oev collection using a for loop
for item in oev_data:
    if "parent_patient_code" in item["properties"]:
        item["mother_patient_code"] = item["properties"]["parent_patient_code"]
        dob = datetime.strptime(
            item["properties"]["infant_dob"], "%Y-%m-%d")
        age = (datetime.now() - dob).days // 365
        item["age"] = age
        item["sexe"] = "female" if item["properties"]["gender"] == "2" else (
            "male" if item["properties"]["gender"] == "1" else "")

# Calculate different category of age
    item["category"] = SchoolingPositif.calculate_age_category.fget(
        age)
    item["site"] = item["properties"]["parent_patient_code"][:8]
    item["commune"] = item["properties"]["school_commune_1"]
    item["case_type"] = item["properties"]["case_type"]
    quarter = (date_peye.month - 1) // 3 + 1
    item["quarter"] = f"Q{quarter}"

# Retrieve data from schooling siblings collection using a for loop
for item in siblings_data:
    if "parent_patient_code" in item["properties"]:
        item["positive_patient_code"] = item["properties"]["parent_patient_code"]
        dob = datetime.strptime(
            item["properties"]["infant_dob"], "%Y-%m-%d")
        age = (datetime.now() - dob).days // 365
        item["age"] = age
        item["sexe"] = "female" if item["properties"]["gender"] == "2" else (
            "male" if item["properties"]["gender"] == "1" else "")

# Calculate different category of age for siblings
    item["category"] = SchoolingPositif.calculate_age_category.fget(
        age)
    item["site"] = item["properties"]["parent_patient_code"][:8]
    item["commune"] = item["properties"]["school_commune"]
    item["case_type"] = item["properties"]["case_type"]
    quarter = (date_peye.month - 1) // 3 + 1
    item["quarter"] = f"Q{quarter}"

# =============================================
# Retrieve data from schooling cwv collection using a for loop
for item in cwv_data:
    dob_cwv = datetime.strptime(item["properties"]["dob"], "%Y-%m-%d")
    age = (datetime.now() - dob_cwv).days // 365
    item["age"] = age
    item["sexe"] = "female" if item["properties"]["gender_sex"] == "F" else (
        "male" if item["properties"]["gender_sex"] == "M" else "")

# Calculate different category of age for cwv
    item["category"] = SchoolingPositif.calculate_age_category.fget(
        age)
    item["commune"] = item["properties"]["school_commune_1"]
    item["case_type"] = item["properties"]["case_type"]
    quarter = (date_peye.month - 1) // 3 + 1
    item["quarter"] = f"Q{quarter}"

# Retrieve data from schooling dreams collection using a for loop
for item in dreams_data:
    if "dreams_code" in item["properties"]:
        item["dreams_code"] = item["properties"]["dreams_code"]
        dob = datetime.strptime(
            item["properties"]["infant_dob"], "%Y-%m-%d")
        age = (datetime.now() - dob).days // 365
        item["age"] = age
        item["sexe"] = "female" if item["properties"]["gender"] == "F" else (
            "male" if item["properties"]["gender"] == "M" else "")

# Calculate different category of age for dreams
    item["category"] = SchoolingPositif.calculate_age_category.fget(
        age)
    item["commune"] = item["properties"]["school_commune_1"]
    item["case_type"] = item["properties"]["case_type"]
    # quarter = SchoolingPositif.calculate_quarter.fget(date_peye)
    quarter = (date_peye.month - 1) // 3 + 1
    item["quarter"] = f"Q{quarter}"

# =============================================#
result_count = len(positive_data) + len(oev_data) + \
    len(cwv_data) + len(siblings_data) + len(dreams_data)
merged_data = {
    "glob": {'total_all_cases': result_count},
    "data": [
        # {"total_global": result_count},
        *positive_data,
        *oev_data,
        *siblings_data,
        *cwv_data,
        *dreams_data
    ],
}


def aggregate_cases_by_commune(cases):
    sorted_cases = sorted(cases, key=itemgetter('commune'))
    grouped_cases = groupby(sorted_cases, key=itemgetter('commune'))
    aggregated_results = []
    for commune, group in grouped_cases:
        commune_cases = list(group)

        # Count the number of cases in the commune
        case_count = len(commune_cases)

        # Aggregate other keys as required
        # Example: Aggregate gender counts
        gender_counts = {'female': 0, 'male': 0}
        unknown_gender_counts = {'unknown_gender': 0}
        female_age_category = {'f_under_1': 0, 'f_1_4': 0, 'f_5_9': 0,
                               'f_10_14': 0, 'f_15_17': 0, 'f_18_20': 0, 'f_over_20': 0}
        male_age_category = {'m_under_1': 0, 'm_1_4': 0, 'm_5_9': 0,
                             'm_10_14': 0, 'm_15_17': 0, 'm_18_20': 0, 'm_over_20': 0}

        for case in commune_cases:
            if case['sexe'] == 'female':
                gender_counts['female'] += 1
            elif case['sexe'] == 'male':
                gender_counts['male'] += 1
            else:
                unknown_gender_counts['unknown_gender'] += 1

        for case in commune_cases:
            if case['category'] == "<1" and case['sexe'] == 'female':
                female_age_category['f_under_1'] += 1
            elif case['category'] == "1-4" and case['sexe'] == 'female':
                female_age_category['f_1_4'] += 1
            if case['category'] == "5-9" and case['sexe'] == 'female':
                female_age_category["f_5_9"] += 1
            elif case['category'] == "10-14" and case['sexe'] == 'female':
                female_age_category['f_10_14'] += 1
            elif case['category'] == "15-17" and case['sexe'] == 'female':
                female_age_category['f_15_17'] += 1
            if case['category'] == "18-20" and case['sexe'] == 'female':
                female_age_category["f_18_20"] += 1
            elif case['category'] == "20+" and case['sexe'] == 'female':
                female_age_category['f_over_20'] += 1
            else:
                continue

        for case in commune_cases:
            if case['category'] == "<1" and case['sexe'] == 'male':
                male_age_category['m_under_1'] += 1
            elif case['category'] == "1-4" and case['sexe'] == 'male':
                male_age_category['m_1_4'] += 1
            if case['category'] == "5-9" and case['sexe'] == 'male':
                male_age_category["m_5_9"] += 1
            elif case['category'] == "10-14" and case['sexe'] == 'male':
                male_age_category['m_10_14'] += 1
            elif case['category'] == "15-17" and case['sexe'] == 'male':
                male_age_category['m_15_17'] += 1
            if case['category'] == "18-20" and case['sexe'] == 'male':
                male_age_category["m_18_20"] += 1
            elif case['category'] == "20+" and case['sexe'] == 'male':
                male_age_category['m_over_20'] += 1
            else:
                continue

        # Create the aggregated result for the commune
        aggregated_result = {
            'commune': commune,
            'total': case_count,
            'male': gender_counts['male'],
            'female': gender_counts['female'],
            'unknown_gender': unknown_gender_counts['unknown_gender'],
            "f_under_1": female_age_category['f_under_1'],
            "f_1_4": female_age_category['f_1_4'],
            "f_5_9": female_age_category['f_5_9'],
            "f_10_14": female_age_category['f_10_14'],
            "f_15_17": female_age_category['f_15_17'],
            "f_18_20": female_age_category['f_18_20'],
            "f_over_20": female_age_category['f_over_20'],
            "m_under_1": male_age_category['m_under_1'],
            "m_1_4": male_age_category['m_1_4'],
            "m_5_9": male_age_category['m_5_9'],
            "m_10_14": male_age_category['m_10_14'],
            "m_15_17": male_age_category['m_15_17'],
            "m_18_20": male_age_category['m_18_20'],
            "m_over_20": male_age_category['m_over_20']
        }
        aggregated_results.append(aggregated_result)
    return aggregated_results
