
def pipeline_handler_positive(year, start_date, end_date):
    return [
        {
            '$match': {
                'properties.schooling_year': {
                    '$in': [
                        year, '2023-2024'
                    ]
                },
                'closed': False,
                'properties.eskew_peye': {
                    '$in': [
                        'wi', '1'
                    ]
                },
                'properties.dat_peyman_fet': {
                    '$exists': True
                },
                'properties.dat_peyman_fet': {
                    '$lte': end_date,
                    '$gte': start_date
                }
            }
        }, {
            '$project': {
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
                "properties.case_type": 1,
                "closed": 1,
            }
        }
    ]


def pipeline_handler_oevsiblings(year, start_date, end_date):
    return [
        {
            '$match': {
                'properties.schooling_year': {
                    '$in': [
                        year, '2023-2024'
                    ]
                },
                'closed': False,
                'properties.eskew_peye': {
                    '$in': [
                        'wi', '1'
                    ]
                },
                'properties.dat_peyman_fet': {
                    '$exists': True
                },
                'properties.dat_peyman_fet': {
                    '$lte': end_date,
                    '$gte': start_date
                }
            }
        }, {
            '$project': {
                "_id": 0,
                "case_id": 1,
                "date_modified": 1,
                "properties.schooling_year": 1,
                "properties.school_commune_1": 1,
                "properties.school_commune": 1,
                "properties.patient_code": 1,
                "properties.infant_commune": 1,
                "properties.gender": 1,
                "properties.infant_dob": 1,
                "properties.dat_peyman_fet": 1,
                "properties.eskew_peye": 1,
                "properties.case_type": 1,
                "properties.patient_code": "$properties.parent_patient_code",
                "closed": 1,
            }
        }
    ]


def pipeline_handler_cwv(year, start_date, end_date):
    return [
        {
            '$match': {
                'properties.schooling_year': {
                    '$in': [
                        year, '2023-2024'
                    ]
                },
                'closed': False,
                'properties.eskew_peye': {
                    '$in': [
                        'wi', '1'
                    ]
                },
                'properties.dat_peyman_fet': {
                    '$exists': True
                },
                'properties.dat_peyman_fet': {
                    '$lte': end_date,
                    '$gte': start_date
                }
            }
        }, {
            '$project': {
                "_id": 0,
                "case_id": 1,
                "date_modified": 1,
                "properties.schooling_year": 1,
                "properties.school_commune_1": 1,
                "properties.gender_sex": 1,
                "properties.dob": 1,
                "properties.dat_peyman_fet": 1,
                "properties.eskew_peye": 1,
                "properties.case_type": 1,
                "closed": 1,
            }
        }
    ]


def pipeline_handler_dreams(year, start_date, end_date):
    return [
        {
            '$match': {
                'properties.schooling_year': {
                    '$in': [
                        year, '2023-2024'
                    ]
                },
                'closed': False,
                'properties.eskew_peye': {
                    '$in': [
                        'wi', '1'
                    ]
                },
                'properties.dat_peyman_fet': {
                    '$exists': True
                },
                'properties.dat_peyman_fet': {
                    '$lte': end_date,
                    '$gte': start_date
                }
            }
        }, {
            '$project': {
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
                "properties.case_type": 1,
                "closed": 1,
            }
        }
    ]
