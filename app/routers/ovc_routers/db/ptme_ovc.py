from ....core import settings, sql_achemy_engine, engine, mongo_database, mongoclient

import pandas as pd

class PtmeOvc:
    def __init__(self) -> None:
        pass

    def get_appel_ptme_from_mongo(self, report_year, report_quarter,type_appel="APPELS_PTME"):
        pipeline = [
                    {
                        '$addFields': {
                            'year_appel_ptme': {
                                '$year': {
                                    '$toDate': '$form.'+type_appel+'.date_appel'
                                }
                            }, 
                            'quarter_appel_ptme': {
                                '$substr': [
                                    {
                                        '$add': [
                                            {
                                                '$divide': [
                                                    {
                                                        '$subtract': [
                                                            {
                                                                '$month': {
                                                                    '$toDate': '$form.'+type_appel+'.date_appel'
                                                                }
                                                            }, 1
                                                        ]
                                                    }, 3
                                                ]
                                            }, 1
                                        ]
                                    }, 0, 1
                                ]
                            }
                        }
                    }, {
                        '$match': {
                            '$or': [
                                {
                                    'year_appel_ptme': int(report_year), 
                                    'quarter_appel_ptme': str(report_quarter)
                                }
                            ]
                        }
                    }, {
                        '$project': {
                            '_id': 0, 
                            'patient_code': '$form.'+type_appel+'.patient_code'
                        }
                    }
                ]
        type_collection = {"APPELS_PTME":"femme_allaitante","appels_oev":"appel_oev"}
        collection = mongo_database[type_collection[type_appel]]
        cursor = collection.aggregate(pipeline)

        list__ = list(cursor)

        return list__

    def get_ovc_query_by_year_quarter(self, report_year , report_quarter):
                list_ptme_appel = self.get_appel_ptme_from_mongo(report_year,report_quarter)
                list_oev_appel = self.get_appel_ptme_from_mongo(report_year,report_quarter,type_appel="appels_oev")
                union_ptme_appel = " UNION ALL ".join([f"SELECT '{item['patient_code']}'as patient_code" for item in list_ptme_appel])
                union_oev_appel = " UNION ALL ".join([f"SELECT '{item['patient_code']}'as patient_code" for item in list_oev_appel])
                query = f"""(SELECT
                            id_patient
                        FROM
                            testing_mereenfant
                        WHERE
                            QUARTER(date) = {report_quarter}
                                AND YEAR(date) = {report_year} ) UNION (SELECT
                            ss.id_patient
                        FROM
                            session ss
                                LEFT JOIN
                            club_session cs ON cs.id = ss.id_club_session
                                LEFT JOIN
                            tracking_infant ti ON ss.id_patient = ti.id_patient
                        WHERE
                            QUARTER(cs.date) = {report_quarter}
                                AND YEAR(cs.date) = {report_year}
                                AND ss.is_present = 1) UNION (SELECT
                            ss.id_patient
                        FROM
                            session ss
                                LEFT JOIN
                            club_session cs ON cs.id = ss.id_club_session
                                LEFT JOIN
                            tracking_motherbasicinfo ti ON ss.id_patient = ti.id_patient
                        WHERE
                            QUARTER(cs.date) = {report_quarter}
                                AND YEAR(cs.date) = {report_year}
                                AND ss.is_present = 1) UNION (SELECT
                            tf.id_patient
                        FROM
                            tracking_followup tf
                                LEFT JOIN
                            tracking_infant ti ON ti.id_patient = tf.id_patient
                        WHERE
                            QUARTER(tf.date) = {report_quarter}
                                AND YEAR(tf.date) = {report_year})
                        UNION (
                        select ts.id_patient from testing_specimen ts
                        left join testing_mereenfant tm on tm.id_patient=ts.id_patient
                        WHERE
                            QUARTER(ts.date_blood_taken) = {report_quarter}
                                AND YEAR(ts.date_blood_taken) = {report_year}
                                )
                        UNION (
                        select id_patient from tracking_motherfollowup tmf where QUARTER(tmf.date) = {report_quarter}
                                AND YEAR(tmf.date) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.eccm_date) = {report_quarter}
                                AND YEAR(topf.eccm_date) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.eccm_date) = {report_quarter}
                                AND YEAR(topf.eccm_date) = {report_year}
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select id_patient from tracking_regime tr
                        where (QUARTER(tr.start_date) = {report_quarter}
                                AND YEAR(tr.start_date) = {report_year})
                                or (QUARTER(tr.end_date) = {report_quarter}
                                AND YEAR(tr.end_date) = {report_year})
                        )
                        union (
                        SELECT
                            qmhk.id_patient
                        FROM
                            questionnaire_motherhivknowledge qmhk
                                where QUARTER(qmhk.date) = {report_quarter}
                                AND YEAR(qmhk.date) = {report_year}
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
                return query;

    def get_ovc_serv_semester_query(self, report_year_1 , report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation=None):
        query_1= self.get_ovc_query_by_year_quarter(report_year_1 , report_quarter_1)
        query_2= self.get_ovc_query_by_year_quarter(report_year_2 , report_qyarter_2)
        query = f"""SELECT
                        a.id_patient as id_patient from ({query_1}) a
                        left join
                        ({query_2}) b on a.id_patient=b.id_patient
                        where b.id_patient is not null"""
        
        aggregations = [
            {"departement": "d.name"},
            {"commune": "c.name"},
            {"site_code": "concat(p.city_code,'/',p.hospital_code)"}

        ]
        select = "a.id_patient as id_patient"
        group_by = ""
        order_by = ""
        aggregation_keys = [key for aggregation in aggregations for key in aggregation.keys()]
        if type_of_aggregation in aggregation_keys:
            select = ""
            for aggregation in aggregations:
                for key, value in aggregation.items():
                    select +=f"{value} as {key} ,"
                    group_by += f"{value} ,"
                    order_by = f"{value} ,"
                if type_of_aggregation in aggregation.keys():
                    break
            group_by = " group by "+group_by[:-1]
            print(group_by)
            select = select +" count(*) as quantity"
            order_by = " order by "+order_by[:-1]

        query_final = f"""SELECT 
                         {select} from ({query}) a
                        left join patient p on p.id=a.id_patient
                        left join lookup_hospital h on h.city_code=p.city_code and h.hospital_code=p.hospital_code
                        left join lookup_commune c on h.commune=c.id
                        left join lookup_departement d on d.id=c.departement
                        left join lookup_office o on o.id=h.office
                        where p.linked_to_id_patient=0 and h.network !=6
                        {group_by}



                        """
        return query_final

    def get_ovc_serv_semester(self, report_year_1 , report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation=None):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = self.get_ovc_serv_semester_query(report_year_1 , report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation)
                cursor.execute(query)
                return cursor.fetchall()
                # return pd.read_sql(query, conn)
            except Exception as e:
                print(e)
                return []