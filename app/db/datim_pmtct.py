from ..core import engine, sql_achemy_engine
import  pandas as pd

class DatimPmtct:
    def __init__(self) -> None:
        pass

    # Get direct beneficiaries script for an interval of time
    def get_beneficiaries_by_interval(self, start_date:str, end_date:str):
        query = f"""
            SELECT DISTINCT
            patient.patient_code
            FROM
            session
                INNER JOIN
            club_session ON club_session.id = session.id_club_session
                INNER JOIN
            patient ON patient.id = session.id_patient
            WHERE
            is_present = 1
                AND club_session.date BETWEEN "{start_date}" AND "{end_date}" 
            UNION ALL SELECT DISTINCT
            p.patient_code
            FROM
            (SELECT 
                qm.id_patient, date
            FROM
                questionnaire_motherhivknowledge qm UNION ALL SELECT 
                qms.id_patient, date
            FROM
                questionnaire_mothersurvey qms UNION ALL SELECT 
                qn.id_patient, date
            FROM
                questionnaire_newmotherhivknowledge qn) q
                INNER JOIN
            patient p ON p.id = q.id_patient
            WHERE
            date BETWEEN "{start_date}" AND "{end_date}" 
            UNION ALL SELECT DISTINCT
            odk_pregnancy_visit.health_id AS patient_code
            FROM
            openfn.odk_pregnancy_visit
            WHERE
            date_of_visit BETWEEN "{start_date}" AND "{end_date}" 
            UNION ALL SELECT DISTINCT
            p.patient_code
            FROM
            tracking_motherbasicinfo
                INNER JOIN
            patient p ON p.id = tracking_motherbasicinfo.id_patient
            WHERE
            tracking_motherbasicinfo.PTME_date BETWEEN "{start_date}" AND "{end_date}" 
            UNION ALL SELECT DISTINCT
            p.patient_code
            FROM
            tracking_pregnancy
                INNER JOIN
            patient p ON p.id = tracking_pregnancy.id_patient_mother
            WHERE
            (ptme_enrollment_date BETWEEN "{start_date}" AND "{end_date}")
                OR (actual_delivery_date BETWEEN "{start_date}" AND "{end_date}") 
            UNION ALL SELECT DISTINCT
            patient_code
            FROM
            odk_tracking_other_visit_ptme
            WHERE
            date_of_visit BETWEEN "{start_date}" AND "{end_date}" 
            UNION ALL SELECT DISTINCT
            p.patient_code
            FROM
            tracking_motherfollowup tmf
                INNER JOIN
            patient p ON p.id = tmf.id_patient
            WHERE
            tmf.date BETWEEN "{start_date}" AND "{end_date}" 
            UNION ALL SELECT DISTINCT
            tpv.patient_code
            FROM
            tracking_ptme_visit tpv
            WHERE
            tpv.date_of_visit BETWEEN "{start_date}" AND "{end_date}" 
            UNION ALL SELECT DISTINCT
            topf.patient_code
            FROM
            tracking_odk_phone_followup topf
            WHERE
            topf.eccm_joignable_par_tel != 0
                AND topf.eccm_date BETWEEN "{start_date}" AND "{end_date}"
                AND topf.name = 'Enquette Corona club meres'
            """
        return query
    def get_direct_ben_by_commune(self, q1_start_date:str, q1_end_date:str, q2_start_date, q2_end_date):
        e = engine()
        q1_script = self.get_beneficiaries_by_interval(q1_start_date,q1_end_date)
        q2_script = self.get_beneficiaries_by_interval(q2_start_date,q2_end_date)

        q1interq2_script = f"""
        SELECT  lc.name as commune,
        SUM(TIMESTAMPDIFF(YEAR, tmi.dob, NOW()) < 1) AS female_under_1,
        SUM((TIMESTAMPDIFF(YEAR, tmi.dob, NOW()) BETWEEN 1 AND 4)) AS female_1_4,
        SUM((TIMESTAMPDIFF(YEAR, tmi.dob, NOW()) BETWEEN 5 AND 9)) AS female_5_9,
        SUM((TIMESTAMPDIFF(YEAR, tmi.dob, NOW()) BETWEEN 10 AND 14)) AS female_10_14,
        SUM((TIMESTAMPDIFF(YEAR, tmi.dob, NOW()) BETWEEN 15 AND 17)) AS female_15_17,
        SUM((TIMESTAMPDIFF(YEAR, tmi.dob, NOW()) BETWEEN 18 AND 20)) AS female_18_20,
        SUM(TIMESTAMPDIFF(YEAR, tmi.dob, NOW()) > 20) AS female__21_plus
        FROM ({q1_script}) a
        INNER JOIN ({q2_script}) b on a.patient_code=b.patient_code
        INNER join patient p on p.patient_code=a.patient_code
        LEFT JOIN tracking_motherbasicinfo tmi on tmi.id_patient=p.id
        LEFT join lookup_hospital lh on lh.city_code=p.city_code and lh.hospital_code=p.hospital_code
        left join lookup_commune lc on lc.id=lh.commune
        GROUP BY lc.id
        """
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(q1interq2_script)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
            
    def get_household_query(self, q1_start_date:str, q1_end_date:str, q2_start_date, q2_end_date):
        q1_script = self.get_beneficiaries_by_interval(q1_start_date,q1_end_date)
        q2_script = self.get_beneficiaries_by_interval(q2_start_date,q2_end_date)
        query = f""" SELECT
                        lc.name as commune,
                        TIMESTAMPDIFF(YEAR, hm.dob,"{q1_start_date}") AS age_adjusted,
                        hm.*
                    FROM household_mother hm
                     INNER join patient p on p.id=hm.id_patient
                    INNER JOIN  ({q1_script}) a on a.patient_code=p.patient_code
                    INNER JOIN ({q2_script}) b on a.patient_code=b.patient_code
                    LEFT join lookup_hospital lh on lh.city_code=p.city_code and lh.hospital_code=p.hospital_code
                    left join lookup_commune lc on lc.id=lh.commune
                    """
        return query
    def get_indirect_ben_by_commune(self, q1_start_date:str, q1_end_date:str, q2_start_date, q2_end_date):
        e = engine()
        household_query = self.get_household_query(q1_start_date, q1_end_date, q2_start_date, q2_end_date)
        with e as conn:
            try:
                cursor = conn.cursor()
                query = f""" SELECT 
                                a.commune,
                                SUM(CASE WHEN a.gender = 2 AND a.age_adjusted < 1 THEN 1 ELSE 0 END) AS female_under_1,
                                SUM(CASE WHEN a.gender = 2 AND a.age_adjusted BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS female_1_4,
                                SUM(CASE WHEN a.gender = 2 AND a.age_adjusted BETWEEN 5 AND 9 THEN 1 ELSE 0 END) AS female_5_9,
                                SUM(CASE WHEN a.gender = 2 AND a.age_adjusted BETWEEN 10 AND 14 THEN 1 ELSE 0 END) AS female_10_14,
                                SUM(CASE WHEN a.gender = 2 AND a.age_adjusted BETWEEN 15 AND 17 THEN 1 ELSE 0 END) AS female_15_17,
                                SUM(CASE WHEN a.gender = 1 AND a.age_adjusted < 1 THEN 1 ELSE 0 END) AS male_under_1,
                                SUM(CASE WHEN a.gender = 1 AND a.age_adjusted BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS male_1_4,
                                SUM(CASE WHEN a.gender = 1 AND a.age_adjusted BETWEEN 5 AND 9 THEN 1 ELSE 0 END) AS male_5_9,
                                SUM(CASE WHEN a.gender = 1 AND a.age_adjusted BETWEEN 10 AND 14 THEN 1 ELSE 0 END) AS male_10_14,
                                SUM(CASE WHEN a.gender = 1 AND a.age_adjusted BETWEEN 15 AND 17 THEN 1 ELSE 0 END) AS male_15_17
                            FROM ({household_query}) a
                            GROUP BY a.commune
                            """
                with open('query.sql', 'w') as f:
                    f.write(query)
                cursor.execute(query)
                result = cursor.fetchall()
                return result
            except Exception as e:
                print(e)
                return []



