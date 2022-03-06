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
                cursor.execute("SELECT id,\
                                name,\
                                code,\
                                office,\
                                localite,\
                                localite_name,\
                                commune,\
                                section,\
                                is_inactive,\
                                meeting_day,\
                                officer_name,\
                                is_graduated,\
                                formed_by_members_from,\
                                actual_cycle,\
                                office FROM muso_group")
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