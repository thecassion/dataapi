from ....core import settings, sql_achemy_engine, engine
from datetime import datetime, date
import pandas as pd

class Dreams:
    def __init__(self) -> None:
        pass


    def get_ovc_query(self, start_date:date, end_date:date):
        nbre_presence_to_complete_cc= 17
        nbre_presence_to_complete_p = 12
        query = f"""
                SELECT b.id_patient, timestampdiff(year, ben.dob, '{end_date}') as age FROM
                ((SELECT 
                    a.id_patient
                FROM
                    (SELECT 
                        COUNT(*) AS nbre_presence, dga.id_patient
                    FROM
                        dream_group_attendance dga
                    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
                    WHERE
                        dga.value = 'P'
                            AND (dgs.date BETWEEN '{start_date}' AND '{end_date}')
                    GROUP BY dga.id_patient
                    HAVING nbre_presence >= {nbre_presence_to_complete_cc}) a) UNION (SELECT 
                    a.id_patient
                FROM
                    (SELECT 
                        COUNT(*) AS nbre_presence, dpga.id_patient
                    FROM
                        dream_parenting_group_attendance dpga
                    LEFT JOIN dream_parenting_group_session dpgs ON dpgs.id = dpga.id_parenting_group_session
                    WHERE
                        (dpga.parent_g = 'P'
                            OR dpga.parent_vd = 'P'
                            OR dpga.yg_g = 'P'
                            OR dpga.yg_vd = 'P')
                            AND (dpgs.date BETWEEN '{start_date}' AND '{end_date}')
                    GROUP BY dpga.id_patient
                    HAVING nbre_presence >= {nbre_presence_to_complete_p}) a) UNION (SELECT 
                    dh.id_patient
                FROM
                    dream_hivinfos dh
                WHERE
                    (dh.has_been_sensibilize_for_condom = 1
                        AND dh.condom_sensibilization_date BETWEEN '{start_date}' AND '{end_date}')
                        OR (dh.condoms_reception_date BETWEEN '{start_date}' AND '{end_date}')
                        OR (dh.contraceptive_reception_date BETWEEN '{start_date}' AND '{end_date}')
                        OR (dh.test_date BETWEEN '{start_date}' AND '{end_date}')
                        OR (dh.vbg_treatment_date BETWEEN '{start_date}' AND '{end_date}')
                        OR (dh.gynecological_care_date BETWEEN '{start_date}' AND '{end_date}')
                        OR (dh.prep_initiation_date BETWEEN '{start_date}' AND '{end_date}')) UNION (SELECT 
                    mgm.id_patient
                FROM
                    muso_group_members mgm
                        INNER JOIN
                    dream_member dm ON dm.id_patient = mgm.id_patient) UNION (SELECT 
                    dm.id_patient
                FROM
                    gardening_beneficiary gb
                        INNER JOIN
                    patient p ON p.patient_code = gb.code_dreams
                        INNER JOIN
                    dream_member dm ON p.id = dm.id_patient
                WHERE
                    gb.date_modified BETWEEN '{start_date}' AND '{end_date}') UNION (SELECT 
                    dga.id_patient
                FROM
                    dream_group_attendance dga
                        LEFT JOIN
                    dream_group_session dgs ON dgs.id = dga.id_group_session
                WHERE
                    dgs.topic IN (8 , 10, 11, 18)
                        AND dga.value = 'P'
                        AND (dgs.date BETWEEN '{start_date}' AND '{end_date}')) ) b
                    left join beneficiary ben on ben.id_patient = b.id_patient
                    where timestampdiff(year, ben.dob, '{end_date}') <= 17
                """
        
        return query
    
    def get_ovc_query_with_disagregation(self, start_date:date, end_date:date, type_of_aggregation:str):
        aggregations = [
            {"departement": "ld.name"},
            {"commune": "lc.name"},
        ]

        select = ""
        order_by = ""
        group_by = ""
        aggregation_keys = [key for aggregation in aggregations for key in aggregation.keys()]
        if type_of_aggregation in aggregation_keys:
            select =""
            for aggregation in aggregations:
                for key, value in aggregation.items():
                    select += f"{value} AS {key} ,"
                    order_by += f"{value} ,"
                    group_by += f"{value} ,"
                if type_of_aggregation in aggregation.keys():
                    break
            group_by = "group by "+group_by[:-1]
            order_by = "order by "+order_by[:-1]

            select = select + f''' count(*) as total ,
            count(*) as female,
            SUM( a.age<1 and a.age is not null) as f_under_1,
            sum( (a.age between 1 and 4) and  a.age is not null ) as f_1_4,
            sum( (a.age between 5 and 9) and  a.age is not null ) as f_5_9,
            sum( (a.age between 10 and 14) and a.age is not null ) as f_10_14,
            sum( (a.age between 15 and 17) and a.age is not null ) as f_15_17,
            sum( (a.age between 18 and 20)  and a.age is not null ) as f_18_20,
            sum( a.age>20 and a.age is not null ) as f_caregiver
            '''
            ovc_query = self.get_ovc_query(start_date, end_date)
            query_final = f"""
            SELECT {select} FROM ({ovc_query}) a
            inner join dream_member dm on dm.id_patient = a.id_patient
            inner join dream_group dg on dg.id=dm.id_group
            inner join dream_hub dh on dh.id=dg.id_dream_hub
            inner join lookup_commune lc on lc.id=dh.commune
            inner join lookup_departement ld on ld.id=lc.departement
            {group_by}
            """
        return query_final
    
    def get_ovc_dreams_by_period(self,start_date:date, end_date:date, type_of_aggregation:str):
        e = engine()
        with e as cn:
            try:
                cursor = cn.cursor()
                query = self.get_ovc_query_with_disagregation(start_date, end_date, type_of_aggregation)
                cursor.execute(query)

                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []