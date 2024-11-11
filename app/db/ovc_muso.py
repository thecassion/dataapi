from ..core import engine, sql_achemy_engine
import  pandas as pd

class OvcMuso:
    def __init__(self) -> None:
        pass
    def get_direct_ben_by_commune(self, start_date:str, end_date:str):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()

                query = f"""
                            SELECT 
                                ld.name AS departement,
                                lc.name AS commune,
                                SUM(b.gender IN (0 , 2, 3)) AS female_18_plus,
                                SUM(b.gender = 1) AS male_18_plus
                            FROM
                                muso_group_members mgm
                                    INNER JOIN
                                muso_group mg ON mg.id = mgm.id_group
                                    INNER JOIN
                                beneficiary b ON b.id_patient = mgm.id_patient
                                    LEFT JOIN
                                lookup_commune_old lc ON lc.id = mg.commune
                                    INNER JOIN
                                lookup_departement_old ld ON ld.id = lc.departement
                            WHERE
                                (mgm.is_inactive = 0
                                    OR mgm.inactive_date >= '{start_date}')
                                    AND (mgm.graduated = 0
                                    OR mgm.graduation_date >= '{start_date}')
                                    AND (mg.is_inactive = 0
                                    OR mg.inactive_date >= '{start_date}')
                                    AND (mg.is_graduated = 0
                                    OR mg.graduation_date >= '{start_date}')
                                    and (mg.creation_date is null or mg.creation_date <='{end_date}')
                            GROUP BY mg.commune
                        """
                cursor.execute(query)
                result = cursor.fetchall()
                return result
            except Exception as e:
                print(e)
                return []
    def get_household_query(self, start_date:str, end_date:str):
        query = f"""
                    SELECT
                        ben.departement,
                        ben.commune,
                        CASE
                            WHEN mh.sexe IN ('1', 'n', 'nm', 'oui', 'M', 'm') THEN 'Male'
                            WHEN mh.sexe IN ('2', '3', 'Non', 'F', 'f') THEN 'Female'
                            ELSE 'Female'
                        END AS gender,
                        CASE
                            WHEN mh.age = -1 THEN 1
                            ELSE 0
                        END AS age_adjusted,
                        CASE
                            WHEN mh.arv IN ('n', 'nn', 'N', '') THEN 'no' 
                            WHEN mh.arv IS NULL THEN 'no'
                            ELSE 'yes'
                        END AS arv,
                        CASE
                            WHEN mh.test IN ('n', 'm', 'nm', 'nn') THEN 'no' 
                            WHEN mh.test IS NULL THEN 'no'
                            ELSE 'yes'
                        END AS test,
                        CASE
                            WHEN mh.often_sick IN ('n', 'nn', 'nom', 'None', '14', '65', 'N', '') THEN 'no' 
                            WHEN mh.often_sick IS NULL THEN 'no'
                            ELSE 'yes'
                        END AS often_sick
                    FROM muso_household_2022 mh
                    INNER JOIN
                    (SELECT
                        ld.name AS departement,
                        lc.name AS commune,
                        mgm.id_patient
                    FROM
                        muso_group_members mgm
                            INNER JOIN
                        muso_group mg ON mg.id = mgm.id_group
                            INNER JOIN
                        beneficiary b ON b.id_patient = mgm.id_patient
                            LEFT JOIN
                        lookup_commune_old lc ON lc.id = mg.commune
                            INNER JOIN
                        lookup_departement_old ld ON ld.id = lc.departement
                    WHERE
                        (mgm.is_inactive = 0
                            OR mgm.inactive_date >= '{start_date}')
                            AND (mgm.graduated = 0
                            OR mgm.graduation_date >= '{start_date}')
                            AND (mg.is_inactive = 0
                            OR mg.inactive_date >= '{start_date}')
                            AND (mg.is_graduated = 0
                            OR mg.graduation_date >= '{start_date}')
                            and (mg.creation_date is null or mg.creation_date <='{end_date}')
                    GROUP BY mgm.id_patient) ben on ben.id_patient = mh.id_patient
                    """
        return query
    def get_indirect_ben_by_commune(self, start_date:str, end_date:str):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                subquery = self.get_household_query(start_date, end_date)
                query = f"""
                            SELECT 
                                a.departement,
                                a.commune,
                                SUM(CASE WHEN a.gender = 'Female' AND a.age_adjusted < 1 THEN 1 ELSE 0 END) AS female_under_1,
                                SUM(CASE WHEN a.gender = 'Female' AND a.age_adjusted BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS female_1_4,
                                SUM(CASE WHEN a.gender = 'Female' AND a.age_adjusted BETWEEN 5 AND 9 THEN 1 ELSE 0 END) AS female_5_9,
                                SUM(CASE WHEN a.gender = 'Female' AND a.age_adjusted BETWEEN 10 AND 14 THEN 1 ELSE 0 END) AS female_10_14,
                                SUM(CASE WHEN a.gender = 'Female' AND a.age_adjusted BETWEEN 15 AND 17 THEN 1 ELSE 0 END) AS female_15_17,
                                SUM(CASE WHEN a.gender = 'Male' AND a.age_adjusted < 1 THEN 1 ELSE 0 END) AS male_under_1,
                                SUM(CASE WHEN a.gender = 'Male' AND a.age_adjusted BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS male_1_4,
                                SUM(CASE WHEN a.gender = 'Male' AND a.age_adjusted BETWEEN 5 AND 9 THEN 1 ELSE 0 END) AS male_5_9,
                                SUM(CASE WHEN a.gender = 'Male' AND a.age_adjusted BETWEEN 10 AND 14 THEN 1 ELSE 0 END) AS male_10_14,
                                SUM(CASE WHEN a.gender = 'Male' AND a.age_adjusted BETWEEN 15 AND 17 THEN 1 ELSE 0 END) AS male_15_17
                            FROM ({subquery}) as a

                            GROUP BY a.departement, a.commune
                        """
                cursor.execute(query)
                result = cursor.fetchall()
                return result
            except Exception as e:
                print(e)
                return []
