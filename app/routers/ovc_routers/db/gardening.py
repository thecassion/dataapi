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
        e=engine()
        with e as conn:
            try:
                query1 = self.get_ovc_query(start_date_1,end_date_1)
                query2 = self.get_ovc_query(start_date_2,end_date_2)

                aggregations = [
                {"departement": "gb.address_department"},
                {"commune": "gb.address_commune"},
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
                    select = select + "count(*) as f_caregiver ,count(*) as total"
                
                where_clause = ""
                if type_of_report == "ovc":
                    where_clause = "where gb.beneficiary_type not in ('Caris','dreams','malnutrition_program','muso','safe_space')"
                elif type_of_report == "program":
                    where_clause =""
                select_query = f"""
                select {select} from ({query1}) as t1
                inner join ({query2}) as t2 on t1.case_id = t2.case_id
                left join gardening_beneficiary gb on gb.case_id = t1.case_id
                {where_clause}
                {group_by}
                """
                cursor = conn.cursor()
                cursor.execute(select_query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []