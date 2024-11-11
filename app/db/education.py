from ..core import engine, sql_achemy_engine
import  pandas as pd

class Education:
    def __init__(self) -> None:
        pass
    def get_education_by_commune(self, start_date:str, end_date:str):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = f"""
                SELECT 
                    a.group_commune AS commune,
                    SUM(TIMESTAMPDIFF(YEAR, a.dob, NOW()) < 1
                        AND a.sexe = 0) AS female_under_1,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 1 AND 4)
                        AND a.sexe = 0) AS female_1_4,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 5 AND 9)
                        AND a.sexe = 0) AS female_5_9,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 10 AND 14)
                        AND a.sexe = 0) AS female_10_14,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 15 AND 17)
                        AND a.sexe = 0) AS female_15_17,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 18 AND 20)
                        AND a.sexe = 0) AS female_18_20,
                    SUM(TIMESTAMPDIFF(YEAR, a.dob, NOW()) > 20
                        AND a.sexe = 0) AS female__21_plus,
                    SUM(TIMESTAMPDIFF(YEAR, a.dob, NOW()) < 1
                        AND a.sexe = 1) AS male_under_1,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 1 AND 4)
                        AND a.sexe = 1) AS male_1_4,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 5 AND 9)
                        AND a.sexe = 1) AS male_5_9,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 10 AND 14)
                        AND a.sexe = 1) AS male_10_14,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 15 AND 17)
                        AND a.sexe = 1) AS male_15_17,
                    SUM((TIMESTAMPDIFF(YEAR, a.dob, NOW()) BETWEEN 18 AND 20)
                        AND a.sexe = 1) AS male_18_20,
                    SUM(TIMESTAMPDIFF(YEAR, a.dob, NOW()) > 20
                        AND a.sexe = 0) AS female__21_plus
                FROM
                    (SELECT 
                        ep.date_de_presence,em.sexe,em.dob,eg.group_commune, sum(ep.is_member_present) as nbre_presence
                    FROM
                        caris_db.education_presence ep
                    LEFT JOIN caris_db.education_members em ON em.case_id = ep.member_case_id
                    LEFT JOIN caris_db.education_groupes eg ON eg.case_id = em.parent_id
                    WHERE
                        ep.date_de_presence BETWEEN '{start_date}' AND '{end_date}'
                            AND is_member_present = 1
                    GROUP BY member_case_id
                    HAVING nbre_presence > 8
                    ) a
                GROUP BY a.group_commune
                """
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []