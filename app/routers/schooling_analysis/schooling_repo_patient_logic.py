from ...core import settings, sql_achemy_engine, engine, mongo_database, mongoclient


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
        query = f"""(SELECT
                            id_patient
                        FROM
                            testing_mereenfant
                        WHERE
                            date between {start_date} and {end_date} ) UNION (SELECT
                            ss.id_patient
                        FROM
                            session ss
                                LEFT JOIN
                            club_session cs ON cs.id = ss.id_club_session
                                LEFT JOIN
                            tracking_infant ti ON ss.id_patient = ti.id_patient
                        WHERE
                            cs.date between  {start_date} and {end_date} AND ss.is_present = 1) UNION (SELECT
                            ss.id_patient
                        FROM
                            session ss
                                LEFT JOIN
                            club_session cs ON cs.id = ss.id_club_session
                                LEFT JOIN
                            tracking_motherbasicinfo ti ON ss.id_patient = ti.id_patient
                        WHERE
                            cs.date between  {start_date} and {end_date}
                                AND ss.is_present = 1) UNION (SELECT
                            tf.id_patient
                        FROM
                            tracking_followup tf
                                LEFT JOIN
                            tracking_infant ti ON ti.id_patient = tf.id_patient
                        WHERE
                            tf.date between  {start_date} and {end_date})
                        UNION (
                        select ts.id_patient from testing_specimen ts
                        left join testing_mereenfant tm on tm.id_patient=ts.id_patient
                        WHERE
                                ts.date_blood_taken between  {start_date} and {end_date}
                        )
                        UNION (
                        select id_patient from tracking_motherfollowup tmf where 
                                tmf.date between  {start_date} and {end_date}
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where 
                            topf.eccm_date between  {start_date} and {end_date}
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where 
                            topf.eccm_date between  {start_date} and {end_date}
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where 
                            topf.date_of_visit between  {start_date} and {end_date}
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where 
                            topf.date_of_visit between  {start_date} and {end_date}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(
                            topf.date_of_visit between  {start_date} and {end_date}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(
                            topf.date_of_visit between  {start_date} and {end_date}
                        )
                        union (
                        select id_patient from tracking_regime tr
                        where (
                                tr.start_date between  {start_date} and {end_date}
                            )
                                or (
                                    tr.end_date between  {start_date} and {end_date}
                            )
                        )
                        union (
                        SELECT
                            qmhk.id_patient
                        FROM
                            questionnaire_motherhivknowledge qmhk
                                where qmhk.date between  {start_date} and {end_date}
                            )
                               UNION
                                ( SELECT
                            qms.id_patient
                        FROM
                            questionnaire_mothersurvey qms
                                where QUARTER(qms.date) = {report_quarter}
                                AND YEAR(qms.date) = {report_year} )
                            UNION (SELECT
                            qnhk.id_patient
                        FROM
                            questionnaire_newmotherhivknowledge qnhk
                            where QUARTER(qnhk.date) = {report_quarter}
                                    AND YEAR(qnhk.date) = {report_year}
                        )
                        UNION (SELECT
                            tp.id_patient_mother as id_patient
                            from tracking_pregnancy tp
                            where (QUARTER(tp.ptme_enrollment_date) = {report_quarter}
                                    AND YEAR(tp.ptme_enrollment_date) = {report_year}) or
                                    (QUARTER(tp.actual_delivery_date ) = {report_quarter}
                                    AND YEAR(tp.actual_delivery_date ) = {report_year})
                        )
                        UNION (SELECT
                            tmb.id_patient
                            from tracking_motherbasicinfo tmb
                            where QUARTER(tmb.PTME_date) = {report_quarter}
                                    AND YEAR(tmb.PTME_date) = {report_year}
                        )
                        UNION (SELECT
                        tracking_motherbasicinfo.id_patient
                            FROM
        tracking_motherbasicinfo
    LEFT JOIN patient ON patient.id = tracking_motherbasicinfo.id_patient
    LEFT JOIN testing_mereenfant ON CONCAT(testing_mereenfant.mother_city_code, '/', testing_mereenfant.mother_hospital_code, '/', testing_mereenfant.mother_code) = patient_code
    WHERE QUARTER(testing_mereenfant.date) = {report_quarter}
                                    AND YEAR(testing_mereenfant.date) = {report_year}
                        )
                        UNION (SELECT DISTINCT q.id_patient
                            FROM questionnaire_child q
                            WHERE QUARTER(q.date) = {report_quarter}
                                    AND YEAR(q.date) = {report_year}
                        )
                        UNION (SELECT DISTINCT id_patient FROM questionnaire_proftraining18 WHERE QUARTER(date) = {report_quarter}
                                    AND YEAR(date) = {report_year})
                        UNION ( SELECT DISTINCT id_patient FROM tracking_infant ti 
                        WHERE (QUARTER(ti.positive_pcr_1) = {report_quarter}
                                    AND YEAR(ti.positive_pcr_1) = {report_year})
                                    OR (QUARTER(ti.positive_pcr_2) = {report_quarter}
                                    AND YEAR(ti.positive_pcr_2) = {report_year})

                        )
                        UNION (SELECT DISTINCT id_patient FROM testing_result tr
                        WHERE (QUARTER(tr.blood_draw_date) = {report_quarter}
                                    AND YEAR(tr.blood_draw_date) = {report_year})
                                )
                        UNION (SELECT DISTINCT
                                patient.id as id_patient
                            FROM
                                caris_db.odk_child_visit
                                LEFT JOIN patient ON patient.patient_code = odk_child_visit.patient_code
                            WHERE
                                (QUARTER(odk_child_visit.date_of_visit) = {report_quarter}
                                    AND YEAR(odk_child_visit.date_of_visit) = {report_year})
                                     AND is_available_at_time_visit = 1
                        )
                        UNION
                        (
                            SELECT p.id as id_patient from (
                            {union_ptme_appel}) a
                            left join patient p on p.patient_code=a.patient_code
                       )
                         UNION
                        (
                            SELECT p.id as id_patient from (
                            {union_oev_appel}) a
                            left join patient p on p.patient_code=a.patient_code
                       )
                        """
        return query
