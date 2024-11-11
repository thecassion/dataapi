from ..core import engine, sql_achemy_engine
import  pandas as pd
from datetime import datetime

class DatimSchooling:
    def __init__(self) -> None:
        pass
    def get_direct_ben_by_commune(self, start_date:str, end_date:str):
        e = engine()
        # return schooling year base on start_date and end_date , if start_date and end_date is greater than 1 october of a year, then it is the next year, else it is the current year
        def get_schooling_year(date):
            date = datetime.strptime(date, "%Y-%m-%d")
            year = date.year
            if date.month > 9:
                return year + 1
            return year
        schooling_year = str(get_schooling_year(start_date)-1)+"-"+str(get_schooling_year(start_date))
        with e as conn:
            try:
                cursor = conn.cursor()
                subquery = f"""
                    SELECT 
                        COALESCE(sp.school_commune_1, sp.commune) AS commune,
                        sp.case_id,
                        TIMESTAMPDIFF(YEAR,
                            sp.infant_dob,
                            '{start_date}') AS age,
                        sp.eskew_peye,
                        sp.gender,
                        'patient' AS type
                    FROM
                        caris_db.schooling_patient sp
                    WHERE
                        sp.schooling_year = '{schooling_year}'
                            AND sp.eskew_peye IN (1 , 'wi') 
                    UNION ALL SELECT 
                        COALESCE(so.school_commune_1, so.commune) AS commune,
                        so.case_id,
                        TIMESTAMPDIFF(YEAR,
                            so.infant_dob,
                            '{start_date}') AS age,
                        so.eskew_peye,
                        so.gender,
                        'oev' AS type
                    FROM
                        caris_db.schooling_oev so
                    WHERE
                        so.schooling_year = '{schooling_year}'
                            AND so.eskew_peye IN (1 , 'wi') 
                    UNION ALL SELECT 
                        COALESCE(ss.school_commune_1, ss.commune) AS commune,
                        ss.case_id,
                        TIMESTAMPDIFF(YEAR,
                            ss.infant_dob,
                            '{start_date}') AS age,
                        ss.eskew_peye,
                        ss.gender,
                        'sibling' AS type
                    FROM
                        caris_db.schooling_sibling ss
                    WHERE
                        ss.schooling_year = '{schooling_year}'
                            AND ss.eskew_peye IN (1 , 'wi') 
                    UNION ALL SELECT 
                        COALESCE(sd.school_commune_1, sd.infant_commune) AS commune,
                        sd.case_id,
                        TIMESTAMPDIFF(YEAR,
                            sd.infant_dob,
                            '{start_date}') AS age,
                        sd.eskew_peye,
                        sd.gender,
                        'dreams' AS type
                    FROM
                        caris_db.schooling_dreams sd
                    WHERE
                        sd.schooling_year = '{schooling_year}'
                            AND sd.eskew_peye IN (1 , 'wi') 
                    UNION ALL SELECT 
                        COALESCE(sc.school_commune_1, sc.commune) AS commune,
                        sc.case_id,
                        TIMESTAMPDIFF(YEAR,
                            COALESCE(sc.dob,sc.real_dob, sc.infant_dob),
                            '{start_date}') AS age,
                        sc.eskew_peye,
                        sc.gender_sex,
                        'cwv_enrollment' AS type
                    FROM
                        caris_db.schooling_cwv_enrollment sc
                    WHERE
                        sc.schooling_year = '{schooling_year}'
                            AND sc.eskew_peye IN (1 , 'wi')
                """
                query = f"""
                select a.commune, 
                SUM(CASE WHEN a.gender in (1,'m','M') AND a.age < 1 THEN 1 ELSE 0 END) AS male_under_1,
                SUM(CASE WHEN a.gender in (1,'m','M') AND a.age BETWEEN 1 AND 4 THEN 1 ELSE 0 END) as male_1_4,
                SUM(CASE WHEN a.gender in (1,'m','M') AND a.age BETWEEN 5 AND 9 THEN 1 ELSE 0 END) as male_5_9,
                SUM(CASE WHEN a.gender in (1,'m','M') AND a.age BETWEEN 10 AND 14 THEN 1 ELSE 0 END) as male_10_14,
                SUM(CASE WHEN a.gender in (1,'m','M') AND a.age BETWEEN 15 AND 17 THEN 1 ELSE 0 END) as male_15_17,
                SUM(CASE WHEN a.gender in (1,'m','M') AND a.age BETWEEN 18 AND 20 THEN 1 ELSE 0 END) as male_18_20,
                SUM(CASE WHEN a.gender in (1,'m','M') AND a.age > 20 THEN 1 ELSE 0 END) as male_21_plus,
                SUM(CASE WHEN a.gender in (2,'f','F') AND a.age < 1 THEN 1 ELSE 0 END) AS female_under_1,
                SUM(CASE WHEN a.gender in (2,'f','F') AND a.age BETWEEN 1 AND 4 THEN 1 ELSE 0 END) as female_1_4,
                SUM(CASE WHEN a.gender in (2,'f','F') AND a.age BETWEEN 5 AND 9 THEN 1 ELSE 0 END) as female_5_9,
                SUM(CASE WHEN a.gender in (2,'f','F') AND a.age BETWEEN 10 AND 14 THEN 1 ELSE 0 END) as female_10_14,
                SUM(CASE WHEN a.gender in (2,'f','F') AND a.age BETWEEN 15 AND 17 THEN 1 ELSE 0 END) as female_15_17,
                SUM(CASE WHEN a.gender in (2,'f','F') AND a.age BETWEEN 18 AND 20 THEN 1 ELSE 0 END) as female_18_20,
                SUM(CASE WHEN a.gender in (2,'f','F') AND a.age > 20 THEN 1 ELSE 0 END) as female_21_plus
                from ({subquery}) as a
                GROUP BY a.commune
                """
                cursor.execute(query)
                result = cursor.fetchall()
                return result
            except Exception as e:
                print(e)
                return []