from ..core import engine, sql_achemy_engine
import  pandas as pd

class Pmtct:
    def __init__(self) -> None:
        pass
    def get_club_6_month_active_infos(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = """
                    SELECT 
                        ld.name AS departement,
                        lc.name AS commune,
                        concat(lh.city_code,"/", lh.hospital_code) AS site,
                        COUNT(a.id_club) AS nbre_club_active
                    FROM
                        (SELECT DISTINCT
                            cs.id_club, c.id_hospital
                        FROM
                            club_session cs
                        LEFT JOIN club c ON c.id = cs.id_club
                        WHERE
                            TIMESTAMPDIFF(MONTH, cs.date, NOW()) <= 6
                                AND c.club_type = 1 group by cs.id_club) a
                            LEFT JOIN
                        lookup_hospital lh ON lh.id = a.id_hospital
                            LEFT JOIN
                        lookup_commune lc ON lc.id = lh.commune
                            LEFT JOIN
                        lookup_departement ld ON ld.id = lc.departement
                    GROUP BY lh.id
                """
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []

    def get_number_pmtct_active_in_club(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = """
                    SELECT 
                        ld.name AS departement,
                        lc.name AS commune,
                        concat(lh.city_code,"/", lh.hospital_code) AS site,
                        COUNT(a.id_patient) AS nbre_pmtct_active
                    FROM
                        (SELECT 
                            s.id_patient, c.id_hospital
                        FROM
                            session s
                        LEFT JOIN club_session cs ON cs.id = s.id_club_session
                        LEFT JOIN club c ON c.id = cs.id_club
                        WHERE
                            TIMESTAMPDIFF(MONTH, cs.date, NOW()) <= 6
                                AND c.club_type = 1 and s.is_present = 1
                        GROUP BY s.id_patient) a
                            LEFT JOIN
                        lookup_hospital lh ON lh.id = a.id_hospital
                            LEFT JOIN
                        lookup_commune lc ON lc.id = lh.commune
                            LEFT JOIN
                        lookup_departement ld ON ld.id = lc.departement
                    GROUP BY lh.id
                """
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []