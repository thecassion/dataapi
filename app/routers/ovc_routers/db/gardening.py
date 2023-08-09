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
    

    def get_ovc_gardening_by_period(self,start_date,end_date,type_of_disagregation):
        e=engine()
        with e as conn:
            try:
                query = self.get_ovc_query(start_date,end_date)
                select_query = f"select count(*) as total from ({query}) as t"
                cursor = conn.cursor()
                cursor.execute(select_query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []