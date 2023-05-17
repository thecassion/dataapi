from ....core import settings, sql_achemy_engine, engine
import pandas as pd

class PtmeOvc:
    def __init__(self) -> None:
        pass

    def get_ovc_query_by_year_quarter(self, report_year , report_quarter):
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
                        where QUARTER(tr.start_date) = {report_quarter}
                                AND YEAR(tr.start_date) = {report_year}
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
                        """
                return query;

    def get_ovc_serv_semester_query(self, report_year_1 , report_quarter_1, report_year_2, report_qyarter_2):
        query_1= self.get_ovc_query_by_year_quarter(report_year_1 , report_quarter_1)
        query_2= self.get_ovc_query_by_year_quarter(report_year_2 , report_qyarter_2)
        query = f"""SELECT
                        a.id_patient as id_patient from ({query_1}) a
                        left join
                        ({query_2}) b on a.id_patient=b.id_patient
                        where b.id_patient is not null"""
        return query

    def get_ovc_serv_semester(self, report_year_1 , report_quarter_1, report_year_2, report_qyarter_2):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = self.get_ovc_serv_semester_query(report_year_1 , report_quarter_1, report_year_2, report_qyarter_2)
                cursor.execute(query)
                return cursor.fetchall()
                # return pd.read_sql(query, conn)
            except Exception as e:
                print(e)
                return []