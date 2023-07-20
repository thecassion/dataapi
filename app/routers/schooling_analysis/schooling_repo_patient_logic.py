from ...core import engine, mongo_database


class PtmeOev:
    def __init__(self) -> None:
        pass

    def get_ptme_call_or_oev_call_from_mongo(self, start_date: str = '2022-10-01', end_date: str = '2023-09-30', type_appel: str = "APPELS_PTME"):
        pipeline = [
            {
                '$addFields': {
                    'call_date': {
                        '$toDate': '$form.'+type_appel+'.date_appel'
                    }
                },
                '$addFields': {
                    'period_appel': {
                        '$dateToString': {
                            "format": "%Y-%m-%d",
                            "date": "$call_date",
                            "onNull": "0000-00-00"
                        }
                    }
                },

            }, {
                '$match': {
                    'period_appel': {
                        '$gte': start_date,
                        '$lte': end_date
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'patient_code': '$form.'+type_appel+'.patient_code'
                }
            }
        ]
        type_collection = {
            "APPELS_PTME": "femme_allaitante", "appels_oev": "appel_oev"}
        collection = mongo_database[type_collection[type_appel]]
        cursor = collection.aggregate(pipeline)
        list__ = list(cursor)
        return list__

    def get_ovc_query_by_period(self, start_date: str = '2022-10-01', end_date: str = '2023-09-30'):
        list_ptme_appel = self.get_ptme_call_or_oev_call_from_mongo(
            start_date, end_date)
        list_oev_appel = self.get_ptme_call_or_oev_call_from_mongo(
            start_date, end_date, type_appel="appels_oev")
        union_ptme_appel = " UNION ALL ".join(
            [f"SELECT '{item['patient_code']}'as patient_code" for item in list_ptme_appel])
        union_oev_appel = " UNION ALL ".join(
            [f"SELECT '{item['patient_code']}'as patient_code" for item in list_oev_appel])
        query = r"""select z.id_patient as id_patient, px.patient_code as patient_code from (SELECT id_patient FROM testing_mereenfant WHERE """ + \
                r"""date between '"""+start_date+r"""' and '"""+end_date+r"""') UNION (SELECT ss.id_patient FROM session ss """ +\
                r"""LEFT JOIN club_session cs ON cs.id = ss.id_club_session """ +\
                r"""LEFT JOIN tracking_infant ti ON ss.id_patient = ti.id_patient """ +\
                r"""WHERE cs.date between '"""+start_date+r"""' and '"""+end_date+r"""' AND ss.is_present = 1) UNION (SELECT ss.id_patient """ +\
                r"""FROM session ss LEFT JOIN club_session cs ON cs.id = ss.id_club_session LEFT JOIN """ +\
                r"""tracking_motherbasicinfo ti ON ss.id_patient = ti.id_patient WHERE cs.date between  '"""+start_date+r"""' and '"""+end_date+r"""' AND ss.is_present = 1) UNION (SELECT """ +\
                r"""tf.id_patient FROM tracking_followup tf LEFT JOIN tracking_infant ti ON ti.id_patient = tf.id_patient WHERE """ +\
                r"""tf.date between '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (select ts.id_patient from testing_specimen ts left join testing_mereenfant tm on tm.id_patient=ts.id_patient WHERE """ +\
                r"""ts.date_blood_taken between '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (select id_patient from tracking_motherfollowup tmf where tmf.date between '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""Union (select p.id as id_patient from tracking_odk_phone_followup topf left join patient p on p.patient_code=UPPER(topf.patient_code) """ +\
                r"""where topf.eccm_date between '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""union (select p.id as id_patient from tracking_odk_phone_followup topf left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id """ +\
                r"""left join patient p on p.patient_code=UPPER(obc.patient_code) """ +\
                r"""where topf.eccm_date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""union ( select p.id as id_patient from odk_tracking_other_visit_ptme topf left join patient p on p.patient_code=UPPER(topf.patient_code) """ +\
                r""" where topf.date_of_visit between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""union (select p.id as id_patient from odk_tracking_other_visit_ptme topf left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id """ +\
                r"""left join patient p on p.patient_code=UPPER(obc.patient_code) where topf.date_of_visit between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""union (select p.id as id_patient from tracking_ptme_visit topf left join patient p on p.patient_code=UPPER(topf.patient_code) """ +\
                r"""where (topf.date_of_visit between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""union (select p.id as id_patient from tracking_ptme_visit topf left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id """ +\
                r"""left join patient p on p.patient_code=UPPER(obc.patient_code) where ( topf.date_of_visit between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""union (select id_patient from tracking_regime tr where (tr.start_date between  '"""+start_date+r"""' and '"""+end_date+r"""') or (tr.end_date between  '"""+start_date+r"""' and '"""+end_date+r"""')) """ +\
                r"""union (SELECT qmhk.id_patient FROM questionnaire_motherhivknowledge qmhk where qmhk.date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (SELECT qms.id_patient FROM questionnaire_mothersurvey qms where qms.date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (SELECT qnhk.id_patient FROM questionnaire_newmotherhivknowledge qnhk where qnhk.date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (SELECT tp.id_patient_mother as id_patient from tracking_pregnancy tp where (tp.ptme_enrollment_date between  '"""+start_date+r"""' and '"""+end_date+r"""') or (tp.actual_delivery_date between  '"""+start_date+r"""' and '"""+end_date+r"""')) """ +\
                r"""UNION (SELECT tmb.id_patient from tracking_motherbasicinfo tmb where tmb.PTME_date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (SELECT tracking_motherbasicinfo.id_patient FROM tracking_motherbasicinfo LEFT JOIN patient ON patient.id = tracking_motherbasicinfo.id_patient """ +\
                r"""LEFT JOIN testing_mereenfant ON CONCAT(testing_mereenfant.mother_city_code, '/', testing_mereenfant.mother_hospital_code, '/', testing_mereenfant.mother_code) = patient_code """ +\
                r"""WHERE testing_mereenfant.date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (SELECT DISTINCT q.id_patient FROM questionnaire_child q WHERE q.date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION (SELECT DISTINCT id_patient FROM questionnaire_proftraining18 WHERE date between  '"""+start_date+r"""' and '"""+end_date+r"""') """ +\
                r"""UNION ( SELECT DISTINCT id_patient FROM tracking_infant ti WHERE (ti.positive_pcr_1 between  '"""+start_date+r"""' and '"""+end_date+r"""')) OR (ti.positive_pcr_2 between  '"""+start_date+r"""' and '"""+end_date+r"""')) """ +\
                r"""UNION (SELECT DISTINCT id_patient FROM testing_result tr WHERE (tr.blood_draw_date between  '"""+start_date+r"""' and '"""+end_date+r"""')) """ +\
                r"""UNION (SELECT DISTINCT patient.id as id_patient FROM caris_db.odk_child_visit LEFT JOIN patient ON patient.patient_code = odk_child_visit.patient_code WHERE (odk_child_visit.date_of_visit between  '"""+start_date+r"""' and '"""+end_date+r"""') AND is_available_at_time_visit = 1) """ +\
                r"""UNION (SELECT p.id as id_patient from ("""+union_ptme_appel+r""") a left join patient p on p.patient_code=a.patient_code) """ +\
                r"""UNION (SELECT p.id as id_patient from ("""+union_oev_appel+r""") a left join patient p on p.patient_code=a.patient_code)) z """ +\
                r"""left join patient px on z.id_patient= px.id where (z.id_patient is not null and z.id_patient!=0) and (px.patient_code is not null) group by z.id_patient"""
        return query

    def remove_newlines_from_sql_query(self, sql_query):
        return sql_query.replace('\n', '')

    def get_ovc_by_period(self, start_date: str = '2022-10-01', end_date: str = '2023-09-30'):
        query = self.get_ovc_query_by_period(start_date, end_date)
        query = self.remove_newlines_from_sql_query(query)
        e = engine()
        """ with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query.encode("utf-8"))
                results = cursor.fetchall()
                return results
            except Exception as e:
                return repr(e) """
        return query.encode("utf-8")
