from db.mysql import engine, sql_achemy_engine
from sqlalchemy import text
import  pandas as pd

class MusoGroup:
    def __init__(self) -> None:
        pass
    def get_muso_groups(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM muso_group")
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
    def get_muso_group(self,muso_group_id):
        e = engine()
        with e.raw_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM muso_group WHERE id=%s",(muso_group_id))
            return cursor.fetchone()
    def get_muso_groupes_df(self):
        e = sql_achemy_engine()
        return pd.read_sql_table('muso_group',e)
    def insert_groupes(self,groupes):
        if isinstance(groupes,pd.DataFrame):
            print("this is a DataFrame")
            groupes.to_sql('muso_group',con=sql_achemy_engine(),if_exists='append',index=False)
            return True
        else:
            e = engine()
            with e as conn:
                try:
                    cursor = conn.cursor()
                    cursor.executemany("INSERT INTO muso_group(office,code,name,external_id) VALUES(%s,%s,%s,%s)",groupes)
                    conn.commit()
                except Exception as e:
                    print(e)
                    return False
            return True
    def update_groupes_case_id(self,groupes):
        if isinstance(groupes,list):
            e = engine()
            with e as conn:
                try:
                    cursor = conn.cursor()
                    for group in groupes:
                        cursor.execute("UPDATE muso_group SET case_id=%s WHERE id=%s",(group['case_id'],group['external_id']))
                    conn.commit()
                except Exception as e:
                    print(e)
                    return False
            return True
        else:
            raise Exception("groupes must be a list")

    def groupes_without_case_id(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT id,name,code,office, case_id,commune FROM muso_group WHERE case_id IS NULL")
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
    def update_groupes_status(self,groupes):
        if isinstance(groupes,list):
            e = engine()
            with e as conn:
                try:
                    cursor = conn.cursor()
                    for group in groupes:
                        print(group)
                        cursor.execute("UPDATE muso_group SET is_graduated=%s , graduation_date=%s ,is_inactive=%s,inactive_date=%s   WHERE case_id=%s",(group['is_graduated'],group['graduation_date'],group['is_inactive'],group['inactive_date'],group['case_id']))
                    conn.commit()
                except Exception as e:
                    print(e)
                    return False
            return True
        else:
            raise Exception("groupes must be a list")