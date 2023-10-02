from ....core import settings, sql_achemy_engine, engine
from datetime import datetime, date
import pandas as pd

class Gardening:
    def __init__(self) -> None:
        pass

    def get_ovc_query(self, start_date:date, end_date:date):

        query = f"""
            select gb.case_id from gardening_beneficiary gb
            where
            (
            gb.date_opened between  '{start_date}' and '{end_date}'
            or gb.start_date between  '{start_date}' and '{end_date}'
            or gb.start_date_Cycle_2 between  '{start_date}' and '{end_date}'

            or gb.cycle_1_start_date between  '{start_date}' and '{end_date}'
            or gb.cycle_2_start_date between  '{start_date}' and '{end_date}'
            or gb.cycle_3_start_date between  '{start_date}' and '{end_date}'
            or gb.cycle_4_start_date between  '{start_date}' and '{end_date}'

            or gb.end_date between  '{start_date}' and '{end_date}'
            or gb.cycle_1_end_date between  '{start_date}' and '{end_date}'
            or gb.cycle_2_end_date between  '{start_date}' and '{end_date}'
            or gb.cycle_3_end_date between  '{start_date}' and '{end_date}'
            or gb.cycle_4_end_date between  '{start_date}' and '{end_date}'

            or gb.fail_date_cycle_1 between  '{start_date}' and '{end_date}'
            or gb.cycle_2_fail_Date between  '{start_date}' and '{end_date}'

            or gb.start_date_Cycle_2 between  '{start_date}' and '{end_date}'
            or gb.date_suivi_telefonik between  '{start_date}' and '{end_date}'

            or garden_established_date between  '{start_date}' and '{end_date}'
            or garden_pre_questionnaire_date between  '{start_date}' and '{end_date}'
            or date_suivi_telefonik between  '{start_date}' and '{end_date}'
            or dat_gradyasyon between  '{start_date}' and '{end_date}'
            or date_modified between  '{start_date}' and '{end_date}'

            or hh_date between  '{start_date}' and '{end_date}'
            or date_closed between  '{start_date}' and '{end_date}'

            or garden_post_questionnaire_cycle_1 between  '{start_date}' and '{end_date}'
            or garden_post_questionnaire_cycle_2 between  '{start_date}' and '{end_date}'
            )
            UNION
            (
            select g.case_id from gardening_registration_forms g where g.timeStart between  '{start_date}' and '{end_date}'
            )
            UNION
            (
            select f.case_id from odk_form_gardening_followup f where f.timeStart between  '{start_date}' and '{end_date}'
            )
            UNION
            (
            select fo.case_id from odk_form_gardening_final_observation fo where fo.timeStart between  '{start_date}' and '{end_date}'
            )
        """
        return query
    

    def get_ovc_gardening_by_period(self,start_date_1,end_date_1, start_date_2,end_date_2,type_of_disagregation, type_of_report="ovc"):
        gardening_household_query = f"""
            SELECT 
                `mh`.`parent_id` AS `id`,
                SUM(((`mh`.`gender` = '2'))) AS `h_female`,
                SUM(((`mh`.`gender` = '1'))) AS `h_male`,
                SUM(((`mh`.`gender` = '2')
                    AND (`mh`.`age` < 1))) AS `h_f_under_1`,
                SUM(((`mh`.`gender` = '1')
                    AND (`mh`.`age` < 1))) AS `h_m_under_1`,
                SUM(((`mh`.`gender` = '2')
                    AND (`mh`.`age` BETWEEN 1 AND 4))) AS `h_f_1_4`,
                SUM(((`mh`.`gender` = '1')
                    AND (`mh`.`age` BETWEEN 1 AND 4))) AS `h_m_1_4`,
                SUM(((`mh`.`gender` = '2')
                    AND (`mh`.`age` BETWEEN 5 AND 9))) AS `h_f_5_9`,
                SUM(((`mh`.`gender` = '1')
                    AND (`mh`.`age` BETWEEN 5 AND 9))) AS `h_m_5_9`,
                SUM(((`mh`.`gender` = '2')
                    AND (`mh`.`age` BETWEEN 10 AND 14))) AS `h_f_10_14`,
                SUM(((`mh`.`gender` = '1')
                    AND (`mh`.`age` BETWEEN 10 AND 14))) AS `h_m_10_14`,
                SUM(((`mh`.`gender` = '2')
                    AND (`mh`.`age` BETWEEN 15 AND 17))) AS `h_f_15_17`,
                SUM(((`mh`.`gender` = '1')
                    AND (`mh`.`age` BETWEEN 15 AND 17))) AS `h_m_15_17`,
                SUM(((`mh`.`gender` = '2')
                    AND (`mh`.`age` > 17))) AS `h_f_caregiver`,
                SUM(((`mh`.`gender` = '1')
                    AND (`mh`.`age` > 17))) AS `h_m_caregiver`
            FROM
                (SELECT 
                    `hg`.*, TIMESTAMPDIFF(YEAR, `hg`.`dob`, '{end_date_2}') AS age
                FROM
                    `caris_db`.`houshold_garden` `hg`) `mh`
                    where `mh`.`age` <= 17
            GROUP BY `mh`.`parent_id`
                """
        e=engine()
        with e as conn:
            try:
                query1 = self.get_ovc_query(start_date_1,end_date_1)
                query2 = self.get_ovc_query(start_date_2,end_date_2)

                aggregations = [
                {"departement": "b.address_department"},
                {"commune": "b.address_commune"},
                ]

                select = ""
                order_by = ""
                group_by = ""
                aggregation_keys = [key for aggregation in aggregations for key in aggregation.keys()]
                if type_of_disagregation in aggregation_keys:
                    select =""
                    for aggregation in aggregations:
                        for key, value in aggregation.items():
                            select += f"{value} AS {key} ,"
                            order_by += f"{value} ,"
                            group_by += f"{value} ,"
                        if type_of_disagregation in aggregation.keys():
                            break
                    group_by = "group by "+group_by[:-1]
                    order_by = "order by "+order_by[:-1]
                    select = select + f"""count(*) as f_caregiver ,count(*) as total,count(*) as female,sum(a.h_male)+sum(a.h_female) as h_total,
                        sum(a.h_male) as h_male, 
                        sum(a.h_female) as h_female,
                        SUM(a.h_f_under_1) as h_f_under_1,
                        sum(a.h_f_1_4) as h_f_1_4,
                        sum(a.h_f_5_9 ) as h_f_5_9,
                        sum(a.h_f_10_14) as h_f_10_14,
                        sum(a.h_f_15_17) as h_f_15_17,
                        sum(a.h_f_caregiver) as h_f_caregiver,
                        SUM(a.h_m_under_1) as h_m_under_1,
                        sum(a.h_m_1_4) as h_m_1_4,
                        sum(a.h_m_5_9) as h_m_5_9,
                        sum(a.h_m_10_14) as h_m_10_14,
                        sum(a.h_m_15_17) as h_m_15_17,
                        sum(a.h_m_caregiver) as h_m_caregiver
                    """
                
                where_clause = ""
                if type_of_report == "ovc":
                    where_clause = "where gb.beneficiary_type not in ('Caris','dreams','malnutrition_program','muso','safe_space')"
                elif type_of_report == "program":
                    where_clause =""
                select_query = f"""
                select {select} from 
                (select gb.* from ({query1}) as t1
                inner join ({query2}) as t2 on t1.case_id = t2.case_id
                left join gardening_beneficiary gb on gb.case_id = t1.case_id
                {where_clause}
                ) b
                left join ({gardening_household_query}) as a on a.id COLLATE utf8mb3_general_ci = b.case_id COLLATE utf8mb3_general_ci

                {group_by}
                """
                cursor = conn.cursor()
                cursor.execute(select_query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []