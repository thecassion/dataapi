from db.mysql import engine
class MusoGroup:
    def __init__(self) -> None:
        pass
    def get_muso_groups(self):
        e = engine()
        with e.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM muso_group")
            return cursor.fetchall()
    def get_muso_group(self,muso_group_id):
        e = engine()
        with e.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM muso_group WHERE id=%s",(muso_group_id,))
            return cursor.fetchone()