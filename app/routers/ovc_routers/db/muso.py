from ....core import engine, sql_achemy_engine, settings


class Muso():
    def __init__(self) -> None:
        pass

    def get_ovc_muso_query(self, report_year_1, report_quarter_1):
        query = f"""
        SELECT
            mgm.id_patient AS id_patient,
            lc.name AS commune,
            ld.name AS departement,
            mg.name AS group_name,
            b.gender,
            b.dob,
            timestampdiff(YEAR, b.dob, CURDATE()) AS age
        FROM
            muso_group_members mgm
                LEFT JOIN
            beneficiary b ON b.id_patient = mgm.id_patient
                LEFT JOIN
            muso_group mg ON mg.id = mgm.id_group
                LEFT JOIN
            lookup_commune lc ON lc.id = mg.commune
                LEFT JOIN
        lookup_departement ld ON ld.id = lc.departement
        WHERE
            (((mgm.is_inactive IS NULL
                OR mgm.is_inactive = 0)
                AND (mg.is_inactive IS NULL
                OR mg.is_inactive = 0)
                AND (mgm.graduated IS NULL
                OR mgm.graduated = 0)
                AND (mg.is_graduated IS NULL
                OR mg.is_graduated = 0))
                OR (mgm.is_inactive = 1
                AND mgm.inactive_date > CONCAT({report_year_1},
                    '-',
                    ({report_quarter_1} - 1) * 3 + 1,
                    '-',
                    '01'))
                OR (mg.is_inactive = 1
                AND (mgm.is_inactive = 0
                OR mgm.is_inactive IS NULL)
                AND mg.inactive_date > CONCAT({report_year_1},
                    '-',
                    ({report_quarter_1} - 1) * 3 + 1,
                    '-',
                    '01')))
        """
        return query

    def get_disagregation_query(self,query,type_of_disagregation):

        aggregations = [
            {"departement":"departement"},
            {"commune": "commune"}
        ]

        group_by = ""
        order_by = ""
        select = ""
        aggregations_keys = [key for aggregation in aggregations for key in aggregation.keys()]
        if type_of_disagregation in aggregations_keys:
            for aggregation in aggregations:
                for key,value in aggregation.items():
                    select += f"{value} AS {key} ,"
                    group_by += f"{value} ,"
                    order_by += f"{value} ,"
                if type_of_disagregation in aggregation.keys():
                    break
            group_by = " group by "+group_by[:-1]
            male =1
            female = 2
            select = select +f''' count(*) as total ,
            sum(a.gender={male} and a.gender is not null) as male, 
            sum(a.gender={female} and a.gender is not null) as female,
            sum(a.gender is null) as unknown_gender,
            SUM(a.gender={female} and a.age<1 and (a.gender is not null) and a.age is not null) as f_under_1,
            sum(a.gender={female} and (a.age between 1 and 4) and (a.gender is not null) and a.age is not null ) as f_1_4,
            sum(a.gender={female} and (a.age between 5 and 9) and (a.gender is not null) and a.age is not null ) as f_5_9,
            sum( a.gender={female} and (a.age between 10 and 14) and (a.gender is not null) and a.age is not null ) as f_10_14,
            sum( a.gender={female} and (a.age between 15 and 17) and (a.gender is not null) and a.age is not null ) as f_15_17,
            sum(a.gender={female} and (a.age between 18 and 20) and ( a.gender is not null) and a.age is not null ) as f_18_20,
            sum( a.gender={female} and a.age>20 and (a.gender is not null) and a.age is not null ) as f_over_20,
            SUM(a.gender={male} and a.age<1 and (a.gender is not null) and a.age is not null) as m_under_1,
            sum(a.gender={male} and (a.age between 1 and 4) and (a.gender is not null) and a.age is not null ) as m_1_4,
            sum(a.gender={male} and (a.age between 5 and 9) and (a.gender is not null) and a.age is not null ) as m_5_9,
            sum( a.gender={male} and (a.age between 10 and 14) and (a.gender is not null) and a.age is not null ) as m_10_14,
            sum( a.gender={male} and (a.age between 15 and 17) and (a.gender is not null) and a.age is not null ) as m_15_17,
            sum( a.gender={male} and (a.age between 18 and 20) and ( a.gender is not null) and a.age is not null ) as m_18_20,
            sum( a.gender={male} and (a.age>20) and (a.gender is not null) and a.age is not null ) as m_over_20

            '''
            order_by = " order by "+order_by[:-1]

            query = f""" SELECT {select} from ({query}) a {group_by} {order_by}"""

            return query






    def get_ovc_muso_query_without_caris_member(self, report_year_1, report_quarter_1, limit=1000, offset=0):
        ovc_muso_query = self.get_ovc_muso_query(report_year_1, report_quarter_1)
        query = f"""
                {ovc_muso_query}
                AND (mgm.is_pvvih IN (0 , 2)
                OR (mgm.is_pvvih IS NULL) )
                AND (
                (mgm.is_caris_member IS NULL) OR
                (mgm.is_caris_member NOT IN (1 , 2, 3)))
                """
        return query



    def get_ovc_muso_all(self, report_year_1, report_quarter_1, type_of_disagregation="commune"):
        e = engine()
        query = self.get_ovc_muso_query(report_year_1, report_quarter_1)
        aggregate_query = self.get_disagregation_query(query,type_of_disagregation)
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(aggregate_query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
            
    def get_ovc_muso_without_caris_member(self, report_year_1, report_quarter_1,type_of_disagregation="commune"):
        e = engine()
        query = self.get_ovc_muso_query_without_caris_member(report_year_1, report_quarter_1)
        aggregate_query = self.get_disagregation_query(query,type_of_disagregation)

        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(aggregate_query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
