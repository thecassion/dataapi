from ..core import engine, sql_achemy_engine
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
                        cursor.execute("UPDATE patient SET muso_case_id=%s WHERE id=%s",(beneficiary['case_id'],beneficiary['id']))
                        conn.commit()
                        print("inserted case_id:",beneficiary['case_id'])
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
                    coalesce(max(a.rank),0) AS max_rank, b.case_id as group_case_id,office,code,b.id as id_group
                FROM
                    caris_db.muso_group_members  as a
                        RIGHT JOIN
                    muso_group as b ON a.id_group = b.id
                WHERE
                    b.case_id IS NOT NULL
                GROUP BY b.case_id  HAVING id_group is not null'''
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
    def insert_beneficiary(self,beneficiary):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO `caris_db`.`patient`(\
                                `city_code`,\
                                `hospital_code`,\
                                `patient_number`,\
                                `patient_code`,\
                                `linked_to_id_patient`,\
                                `which_program`,\
                                `muso_case_id`)\
                                VALUES (%s,%s,%s,%s,%s,%s,%s)",(beneficiary['city_code'],beneficiary['hospital_code'],beneficiary['patient_number'],beneficiary['patient_code'],beneficiary['linked_to_id_patient'],beneficiary['which_program'],beneficiary['case_id']))
                id_patient = cursor.lastrowid
                print("id_patient:",id_patient)
                cursor.execute("INSERT INTO `caris_db`.`beneficiary`\
                                (`id_patient`,\
                                `first_name`,\
                                `last_name`,\
                                `dob`,\
                                `gender`,\
                                `phone`,\
                                `address`,\
                                `is_pvvih`,\
                                `created_by`)\
                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                (id_patient,beneficiary['first_name'],beneficiary['last_name'],beneficiary['dob'],beneficiary['gender'],beneficiary["phone"],beneficiary["address"],beneficiary["is_pvvih"],beneficiary["created_by"]))
                cursor.execute("INSERT INTO `caris_db`.`muso_group_members`\
                                (`id_patient`,\
                                `id_group`,\
                                `is_inactive`,\
                                `inactive_date`,\
                                `is_abandoned`,\
                                `abandoned_date`,\
                                `rank`,\
                                `graduated`,\
                                `graduation_date`,\
                                `created_by`)\
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(id_patient,beneficiary['id_group'],beneficiary['is_inactive'],beneficiary['inactive_date'],beneficiary['is_abandoned'],beneficiary['abandoned_date'],beneficiary['rank'],beneficiary['graduated'],beneficiary['graduation_date'],beneficiary['created_by']))


                conn.commit()
            except Exception as e:
                print(beneficiary)
                raise Exception(e)
                return False


    def update_benficiaries_status(self, beneficiairies):
        if isinstance(beneficiairies,list):
            e = engine()
            with e as conn:
                try:
                    cursor = conn.cursor()
                    i=0
                    for ben in beneficiairies:
                        i=i+1
                        # ben["graduated"]=int(ben["graduated"])
                        # ben["is_inactive"]=int(ben["is_inactive"])
                        # ben["closed"]=int(ben["closed"])
                        cursor.execute("UPDATE muso_group_members mgm LEFT JOIN patient p on p.id=mgm.id_patient SET mgm.graduated=%s , mgm.graduation_date=%s ,mgm.is_inactive=%s,mgm.inactive_date=%s,mgm.closed_on_commcare=%s   WHERE p.muso_case_id=%s",(ben['graduated'],ben['graduation_date'],ben['is_inactive'],ben['inactive_date'],ben["closed"],ben['case_id']))
                        print("status sync for beneficiaire "+str(i),ben)
                    conn.commit()
                except Exception as e:
                    print(e)
                    return False
            return True
        else:
            raise Exception("beneficiaries must be a list")


    def update_beneficiairies_household_not_applicable(self,beneficiairies):
        if isinstance(beneficiairies,list):
            e = engine()
            with e as conn:
                try:
                    cursor = conn.cursor()
                    i=0
                    for ben in beneficiairies:
                        i=i+1
                        # ben["graduated"]=int(ben["graduated"])
                        # ben["is_inactive"]=int(ben["is_inactive"])
                        # ben["closed"]=int(ben["closed"])
                        cursor.execute("UPDATE muso_group_members mgm LEFT JOIN patient p on p.id=mgm.id_patient SET mgm.is_household_applicable=%s   WHERE p.muso_case_id=%s",(ben['is_household_applicable'],ben['case_id']))
                        print("status sync for beneficiaire "+str(i),ben)
                        conn.commit()
                except Exception as e:
                    print(e)
                    return False
            return True
        else:
            raise Exception("beneficiaries must be a list")