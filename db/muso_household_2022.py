from db.mysql import engine, sql_achemy_engine
import pandas as pd

class MusoHousehold2022:
    def __init__(self) -> None:
        pass

    def get_max_pos_by_beneficiaires(self):
        e = engine()
        with e as conn:
            try:
                cursor= conn.cursor()
                query = '''
                SELECT
                    p.id AS id_patient, COALESCE(MAX(pos), 0) AS max_pos,p.muso_case_id
                FROM
                    muso_household_2022 mh
                        RIGHT JOIN
                    patient p ON p.id = mh.id_patient
                WHERE
                    p.muso_case_id IS NOT NULL
                GROUP BY p.id
                '''
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
    def get_muso_household2022(self):
        e = engine()
        with e as conn:
            try:
                cursor=conn.cursor()
                query = '''
                SELECT * from muso_household_2022
                '''
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []