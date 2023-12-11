from ....core import settings, sql_achemy_engine, engine
from datetime import datetime, date
import pandas as pd

class Education:
    def __init__(self) -> None:
        pass

    def get_ovc_query(self, start_date:date, end_date:date):
        query = f"""
                SELECT 
                    ld.name AS departement,
                    a.commune,
                    COUNT(*) AS total,
                    SUM(a.sexe = 1) AS male,
                    SUM(a.sexe = 0) AS female,
                    SUM(a.sexe = 0) AS f_10_14,
                    SUM(a.sexe = 1) AS m_10_14
                FROM
                    (SELECT 
                        em.case_id,
                            COALESCE(em.age, TIMESTAMPDIFF(YEAR, em.dob, em.date_opened), 0) AS age,
                            em.sexe,
                            COUNT(*) AS nbre,
                            IF(eg.group_commune = 'Cayes', 'Les Cayes', IF(eg.group_commune IN ('Ducis' , 'Guilgo'), 'Torbeck', eg.group_commune)) AS commune,
                            em.dob
                    FROM
                        caris_db.education_session es
                    LEFT JOIN caris_db.education_members em ON em.case_id = es.member_case_id
                    LEFT JOIN caris_db.education_groupes eg ON eg.case_id = em.parent_id
                    WHERE
                        (date_de_presence BETWEEN '{start_date}' AND '{end_date}')
                            AND is_member_present = 1
                    GROUP BY em.case_id
                    HAVING nbre >= 8 AND age BETWEEN 10 AND 14) a
                        LEFT JOIN
                    lookup_commune lc ON lc.name = a.commune
                        LEFT JOIN
                    lookup_departement ld ON ld.id = lc.departement
                GROUP BY a.commune
        """
        return query
    

    def get_ovc_education_by_period(self,start_date,end_date):
        e=engine()
        with e as conn:
            try:
                query = self.get_ovc_query(start_date,end_date)

                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []