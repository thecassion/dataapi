from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, List
from itertools import groupby
from operator import itemgetter


class SchoolingPositif(BaseModel):

    commune: str
    total: int
    male: int
    female: int
    unknown_gender: int
    f_under_1: int
    f_1_4: int
    f_5_9: int
    f_10_14: int
    f_15_17: int
    f_18_20: int
    f_over_20: int
    m_under_1: int
    m_1_4: int
    m_5_9: int
    m_10_14: int
    m_15_17: int
    m_18_20: int
    m_over_20: int

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

    @property
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
