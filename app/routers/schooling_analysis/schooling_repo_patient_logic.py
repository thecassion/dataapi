from ...core import engine, mongo_database


class PtmeOev:

    @classmethod
    def get_ptme_call_or_oev_call_from_mongo(cls, start_date: str = '2022-10-01', end_date: str = '2023-09-30', type_appel: str = 'APPELS_PTME'):
        type_collection = {
            "APPELS_PTME": 'femme_allaitante', "appels_oev": 'appel_oev'}
        pipeline = [
            {
                '$addFields': {
                    'call_date': {
                        '$toDate': f"$form.{type_appel}.date_appel",
                    }
                }
            }, {
                '$addFields': {
                    'period_appel': {
                        '$dateToString': {
                            "format": "%Y-%m-%d",
                            "date": "$call_date",
                            "onNull": "0000-00-00"
                        }
                    }
                }
            }, {
                '$match': {
                    'period_appel': {
                        '$gte': f"{start_date}",
                        '$lte': f"{end_date}"
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'patient_code': f"$form.{type_appel}.patient_code"
                }
            }
        ]
        collection = mongo_database[type_collection[type_appel]]
        cursor = collection.aggregate(pipeline)
        list__ = list(cursor)
        return list__

    @classmethod
    def get_ovc_query_by_period(cls, start_date: str = '2022-10-01', end_date: str = '2023-09-30'):
        list_ptme_appel = cls.get_ptme_call_or_oev_call_from_mongo(
            start_date, end_date)
        list_oev_appel = cls.get_ptme_call_or_oev_call_from_mongo(
            start_date, end_date, type_appel='appels_oev')

        union_ptme_appel = " UNION ALL ".join(
            [f"SELECT '{item['patient_code']}' as patient_code" for item in list_ptme_appel])
        union_oev_appel = " UNION ALL ".join(
            [f"SELECT '{item['patient_code']}' as patient_code" for item in list_oev_appel])

        result_ovc = f"""select * from (SELECT id_patient FROM testing_mereenfant WHERE date BETWEEN '{start_date}' AND '{end_date}'
               UNION (SELECT ss.id_patient FROM session ss 
                      LEFT JOIN club_session cs ON cs.id = ss.id_club_session 
                      LEFT JOIN tracking_infant ti ON ss.id_patient = ti.id_patient 
                      WHERE cs.date BETWEEN '{start_date}' AND '{end_date}' AND ss.is_present = 1)
               UNION (SELECT ss.id_patient FROM session ss 
                      LEFT JOIN club_session cs ON cs.id = ss.id_club_session 
                      LEFT JOIN tracking_motherbasicinfo ti ON ss.id_patient = ti.id_patient 
                      WHERE cs.date BETWEEN '{start_date}' AND '{end_date}' AND ss.is_present = 1)
               UNION (SELECT tf.id_patient FROM tracking_followup tf 
                      LEFT JOIN tracking_infant ti ON ti.id_patient = tf.id_patient 
                      WHERE tf.date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT ts.id_patient FROM testing_specimen ts 
                      LEFT JOIN testing_mereenfant tm ON tm.id_patient=ts.id_patient 
                      WHERE ts.date_blood_taken BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT id_patient FROM tracking_motherfollowup tmf 
                      WHERE tmf.date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT p.id as id_patient FROM tracking_odk_phone_followup topf 
                      LEFT JOIN patient p ON p.patient_code=UPPER(topf.patient_code) 
                      WHERE topf.eccm_date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT p.id as id_patient FROM tracking_odk_phone_followup topf 
                      LEFT JOIN odk_hivhaiti_backward_compatibility obc ON obc.odk_case_id=topf.case_id 
                      LEFT JOIN patient p ON p.patient_code=UPPER(obc.patient_code) 
                      WHERE topf.eccm_date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT p.id as id_patient FROM odk_tracking_other_visit_ptme topf 
                      LEFT JOIN patient p ON p.patient_code=UPPER(topf.patient_code) 
                      WHERE topf.date_of_visit BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT p.id as id_patient FROM odk_tracking_other_visit_ptme topf 
                      LEFT JOIN odk_hivhaiti_backward_compatibility obc ON obc.odk_case_id=topf.case_id 
                      LEFT JOIN patient p ON p.patient_code=UPPER(obc.patient_code) 
                      WHERE topf.date_of_visit BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT p.id as id_patient FROM tracking_ptme_visit topf 
                      LEFT JOIN patient p ON p.patient_code=UPPER(topf.patient_code) 
                      WHERE (topf.date_of_visit BETWEEN '{start_date}' AND '{end_date}'))
               UNION (SELECT p.id as id_patient FROM tracking_ptme_visit topf 
                      LEFT JOIN odk_hivhaiti_backward_compatibility obc ON obc.odk_case_id=topf.case_id 
                      LEFT JOIN patient p ON p.patient_code=UPPER(obc.patient_code) 
                      WHERE (topf.date_of_visit BETWEEN '{start_date}' AND '{end_date}'))
               UNION (SELECT id_patient FROM tracking_regime tr 
                      WHERE (tr.start_date BETWEEN '{start_date}' AND '{end_date}') OR (tr.end_date BETWEEN '{start_date}' AND '{end_date}'))
               UNION (SELECT qmhk.id_patient FROM questionnaire_motherhivknowledge qmhk 
                      WHERE qmhk.date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT qms.id_patient FROM questionnaire_mothersurvey qms 
                      WHERE qms.date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT qnhk.id_patient FROM questionnaire_newmotherhivknowledge qnhk 
                      WHERE qnhk.date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT tp.id_patient_mother as id_patient FROM tracking_pregnancy tp 
                      WHERE (tp.ptme_enrollment_date BETWEEN '{start_date}' AND '{end_date}') OR (tp.actual_delivery_date BETWEEN '{start_date}' AND '{end_date}'))
               UNION (SELECT tmb.id_patient FROM tracking_motherbasicinfo tmb 
                      WHERE tmb.PTME_date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT tracking_motherbasicinfo.id_patient FROM tracking_motherbasicinfo 
                      LEFT JOIN patient ON patient.id = tracking_motherbasicinfo.id_patient 
                      LEFT JOIN testing_mereenfant ON CONCAT(testing_mereenfant.mother_city_code, '/', testing_mereenfant.mother_hospital_code, '/', testing_mereenfant.mother_code) = patient_code 
                      WHERE testing_mereenfant.date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT DISTINCT q.id_patient FROM questionnaire_child q 
                      WHERE q.date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT DISTINCT id_patient FROM questionnaire_proftraining18 
                      WHERE date BETWEEN '{start_date}' AND '{end_date}')
               UNION (SELECT DISTINCT id_patient FROM tracking_infant ti 
                      WHERE (ti.positive_pcr_1 BETWEEN '{start_date}' AND '{end_date}') OR (ti.positive_pcr_2 BETWEEN '{start_date}' AND '{end_date}'))
               UNION (SELECT DISTINCT id_patient FROM testing_result tr 
                      WHERE (tr.blood_draw_date BETWEEN '{start_date}' AND '{end_date}'))
               UNION (SELECT DISTINCT patient.id as id_patient FROM caris_db.odk_child_visit 
                      LEFT JOIN patient ON patient.patient_code = odk_child_visit.patient_code 
                      WHERE (odk_child_visit.date_of_visit BETWEEN '{start_date}' AND '{end_date}') AND is_available_at_time_visit = 1)
               UNION (SELECT p.id as id_patient from ({union_ptme_appel})a 
                      LEFT JOIN patient p ON p.patient_code=a.patient_code)
               UNION (SELECT p.id as id_patient from ({union_oev_appel})a 
                      LEFT JOIN patient p ON p.patient_code=a.patient_code))z
                      where z.id_patient is not null
                      """

        # with open('query.sql','w') as ree:
        #   ree.write(result_ovc)

        return result_ovc

    @classmethod
    def get_patient_info(cls):
        result_patient_info = f"""SELECT id as id_patient, patient_code FROM patient where id is not null"""
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(result_patient_info)
                results = cursor.fetchall()
                return results
            except Exception as e:
                return repr(e)
        return result_patient_info

    @classmethod
    def remove_newlines_from_sql_query(cls, sql_query):
        return sql_query.replace('\n', '')

    # def remove_backslash_from_sql_query(self, sql_query):
    #    return sql_query.replace('\\', '')

    @classmethod
    def get_ovc_by_period(cls, start_date: str = '2022-10-01', end_date: str = '2023-09-30'):
        query = cls.get_ovc_query_by_period(start_date, end_date)
        query = cls.remove_newlines_from_sql_query(query)
        # query = self.remove_backslash_from_sql_query(query)
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query.encode("utf-8"))
                results = cursor.fetchall()
                return results
            except Exception as e:
                return repr(e)
        return query.encode("utf-8")

    @staticmethod
    def compare_results(start_date: str = '2022-10-01', end_date: str = '2023-09-30'):
        # a= ovcresult
        # b=patient_info_result
        result_dict = {}
        result_list = []
        ovc_result = PtmeOev.get_ovc_by_period(start_date, end_date)
        patient_info_result = PtmeOev.get_patient_info()
        patient_dict = {item.get('id_patient'): item for item in patient_info_result if isinstance(
            item, dict) and 'id_patient' in item}
        # patient_dict = {item['id_patient']: item.values for item in patient_info_result}
        for item in ovc_result:
            id_patient = item['id_patient']
            if id_patient in patient_dict:
                # If there's a match, append the corresponding dictionary from list b to the result_list
                result_dict = patient_dict[id_patient]
                # Remove the 'id_patient' key from the dictionary
                result_dict.pop('id_patient')
                result_list.append(result_dict)

        return result_list
