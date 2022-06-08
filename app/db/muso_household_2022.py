from ..db.mysql import engine, sql_achemy_engine
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

    def insert_household2022(self,ben):
        e = engine()
        with e as conn:
            try:
                cursor=conn.cursor()
                cursor.execute(" INSERT INTO `caris_db`.`muso_household_2022`(\
                    `pos`,\
                    `age`,\
                    `id_patient`,\
                    `sexe`,\
                    `arv`,\
                    `test`,\
                    `often_sick`,\
                    `case_id`,\
                    `created_by`,\
                    `user_id`)\
                    VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s,%s)",
                    (ben["pos"], ben["age"],ben["id_patient"],ben["sexe"],ben["arv"],ben["test"],ben["often_sick"],ben["case_id"],120, ben["user_id"]))
                print("insert :",ben["id_patient"])
                conn.commit()
            except Exception as e:
                print(e)
                raise Exception(e)