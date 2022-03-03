from db.mysql import engine
from sqlalchemy import text
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