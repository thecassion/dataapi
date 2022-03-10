from db.mysql import engine, sql_achemy_engine
import pandas as pd
class MusoBeneficiary:
    def __init__(self) -> None:
        pass
    def get_muso_beneficiaries(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM muso_group_members mgm JOIN muso_group mg ON mg.id=mgm.id_group JOIN beneficiary b ON b.id=mgm.id_patient")
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []

    def update_muso_beneficiaries_case_id(self,beneficiaries):
        if isinstance(beneficiaries,list):
            e = engine()
            with e as conn:
                try:
                    cursor = conn.cursor()
                    for beneficiary in beneficiaries:
                        cursor.execute("UPDATE patient SET case_id=%s WHERE id=%s",(beneficiary['case_id'],beneficiary['id']))
                    conn.commit()
                except Exception as e:
                    print(e)
                    return False
            return True
        else:
            raise Exception("beneficiaries must be a list")

    def get_max_rank_beneficiaries_by_groups(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = ''' SELECT
                    coalesce(max(a.rank),0) AS max_rank, b.case_id as group_case_id
                FROM
                    caris_db.muso_group_members  as a
                        RIGHT JOIN
                    muso_group as b ON a.id_group = b.id
                WHERE
                    b.case_id IS NOT NULL
                GROUP BY b.case_id '''
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []