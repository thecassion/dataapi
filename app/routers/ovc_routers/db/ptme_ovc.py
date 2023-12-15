from ....core import settings, sql_achemy_engine, engine, mongo_database, mongoclient

import pandas as pd


class PtmeOvc:
    def __init__(self) -> None:
        pass

    def get_appel_ptme_from_mongo(self, report_year, report_quarter, type_appel="APPELS_PTME"):
        pipeline = [
            {
                '$addFields': {
                    'year_appel_ptme': {
                        '$year': {
                            '$toDate': '$form.'+type_appel+'.date_appel'
                        }
                    },
                    'quarter_appel_ptme': {
                        '$substr': [
                            {
                                '$add': [
                                    {
                                        '$divide': [
                                            {
                                                '$subtract': [
                                                    {
                                                        '$month': {
                                                            '$toDate': '$form.'+type_appel+'.date_appel'
                                                        }
                                                    }, 1
                                                ]
                                            }, 3
                                        ]
                                    }, 1
                                ]
                            }, 0, 1
                        ]
                    }
                }
            }, {
                '$match': {
                    '$and': [
                        {
                            'year_appel_ptme': int(report_year),
                            'quarter_appel_ptme': str(report_quarter)
                        }
                    ]
                }
            }, {
                '$project': {
                    '_id': 0,
                    'patient_code': '$form.'+type_appel+'.patient_code'
                }
            }
        ]
        type_collection = {
            "APPELS_PTME": "femme_allaitante", "appels_oev": "appel_oev"}
        collection = mongo_database[type_collection[type_appel]]
        cursor = collection.aggregate(pipeline)

        list__ = list(cursor)

        return list__
    

    def get_ptme_query_by_year_quarter(self, report_year, report_quarter):
        query = f"""  (SELECT
                            ss.id_patient
                        FROM
                            session ss
                                INNER JOIN
                            club_session cs ON cs.id = ss.id_club_session
                                INNER JOIN
                            tracking_motherbasicinfo ti ON ss.id_patient = ti.id_patient
                        WHERE
                            QUARTER(cs.date) = {report_quarter}
                                AND YEAR(cs.date) = {report_year}
                                AND ss.is_present = 1)
                        UNION (
                        select id_patient from tracking_motherfollowup tmf where QUARTER(tmf.date) = {report_quarter}
                                AND YEAR(tmf.date) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.eccm_date) = {report_quarter}
                                AND YEAR(topf.eccm_date) = {report_year} AND name='Enquette Corona club meres'
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.eccm_date) = {report_quarter}
                                AND YEAR(topf.eccm_date) = {report_year} AND name='Enquette Corona club meres'
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select id_patient from tracking_regime tr
                        where ((QUARTER(tr.start_date) = {report_quarter}
                                AND YEAR(tr.start_date) = {report_year})
                                or (QUARTER(tr.end_date) = {report_quarter}
                                AND YEAR(tr.end_date) = {report_year})) AND tr.category='regime_mother_treatment'
                        )
                        union (
                        SELECT
                            qmhk.id_patient
                        FROM
                            questionnaire_motherhivknowledge qmhk
                                where QUARTER(qmhk.date) = {report_quarter}
                                AND YEAR(qmhk.date) = {report_year}
                            )
                               UNION
                                ( SELECT
                            qms.id_patient
                        FROM
                            questionnaire_mothersurvey qms
                                where QUARTER(qms.date) = {report_quarter}
                                AND YEAR(qms.date) = {report_year} )
                            UNION (SELECT
                            qnhk.id_patient
                        FROM
                            questionnaire_newmotherhivknowledge qnhk
                            where QUARTER(qnhk.date) = {report_quarter}
                                    AND YEAR(qnhk.date) = {report_year}
                        )
                        UNION (SELECT
                            tp.id_patient_mother as id_patient
                            from tracking_pregnancy tp
                            where (QUARTER(tp.ptme_enrollment_date) = {report_quarter}
                                    AND YEAR(tp.ptme_enrollment_date) = {report_year}) or
                                    (QUARTER(tp.actual_delivery_date ) = {report_quarter}
                                    AND YEAR(tp.actual_delivery_date ) = {report_year})
                        )
                        UNION (SELECT
                            tmb.id_patient
                            from tracking_motherbasicinfo tmb
                            where QUARTER(tmb.PTME_date) = {report_quarter}
                                    AND YEAR(tmb.PTME_date) = {report_year}
                        )
                        UNION (SELECT
                        tracking_motherbasicinfo.id_patient
                            FROM
                                tracking_motherbasicinfo
                            LEFT JOIN patient ON patient.id = tracking_motherbasicinfo.id_patient
                            LEFT JOIN testing_mereenfant ON CONCAT(testing_mereenfant.mother_city_code, '/', testing_mereenfant.mother_hospital_code, '/', testing_mereenfant.mother_code) = patient_code
                            WHERE QUARTER(testing_mereenfant.date) = {report_quarter}
                                                            AND YEAR(testing_mereenfant.date) = {report_year}
                        )
                        UNION
                        (
                            SELECT p.id as id_patient from odk_appel oa
                            left join patient p on p.patient_code=oa.patient_code
                            WHERE QUARTER(oa.date_appel) = {report_quarter} AND YEAR(oa.date_appel) = {report_year}
                       )
                        """
        return query

    def get_ovc_query_by_year_quarter(self, report_year, report_quarter):
        query = f"""(SELECT
                            id_patient
                        FROM
                            testing_mereenfant
                        WHERE
                            QUARTER(date) = {report_quarter}
                                AND YEAR(date) = {report_year} ) UNION (SELECT
                            ss.id_patient
                        FROM
                            session ss
                                LEFT JOIN
                            club_session cs ON cs.id = ss.id_club_session
                                LEFT JOIN
                            tracking_infant ti ON ss.id_patient = ti.id_patient
                        WHERE
                            QUARTER(cs.date) = {report_quarter}
                                AND YEAR(cs.date) = {report_year}
                                AND ss.is_present = 1) UNION (SELECT
                            ss.id_patient
                        FROM
                            session ss
                                LEFT JOIN
                            club_session cs ON cs.id = ss.id_club_session
                                LEFT JOIN
                            tracking_motherbasicinfo ti ON ss.id_patient = ti.id_patient
                        WHERE
                            QUARTER(cs.date) = {report_quarter}
                                AND YEAR(cs.date) = {report_year}
                                AND ss.is_present = 1) UNION (SELECT
                            tf.id_patient
                        FROM
                            tracking_followup tf
                                LEFT JOIN
                            tracking_infant ti ON ti.id_patient = tf.id_patient
                        WHERE
                            QUARTER(tf.date) = {report_quarter}
                                AND YEAR(tf.date) = {report_year})
                        UNION (
                        select ts.id_patient from testing_specimen ts
                        left join testing_mereenfant tm on tm.id_patient=ts.id_patient
                        WHERE
                            QUARTER(ts.date_blood_taken) = {report_quarter}
                                AND YEAR(ts.date_blood_taken) = {report_year}
                                )
                        UNION (
                        select id_patient from tracking_motherfollowup tmf where QUARTER(tmf.date) = {report_quarter}
                                AND YEAR(tmf.date) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.eccm_date) = {report_quarter}
                                AND YEAR(topf.eccm_date) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_odk_phone_followup topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.eccm_date) = {report_quarter}
                                AND YEAR(topf.eccm_date) = {report_year}
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from odk_tracking_other_visit_ptme topf 
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join patient p on p.patient_code=UPPER(topf.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select p.id as id_patient from tracking_ptme_visit topf
                        left join odk_hivhaiti_backward_compatibility obc on obc.odk_case_id=topf.case_id
                        left join patient p on p.patient_code=UPPER(obc.patient_code)
                        where QUARTER(topf.date_of_visit) = {report_quarter}
                                AND YEAR(topf.date_of_visit) = {report_year}
                        )
                        union (
                        select id_patient from tracking_regime tr
                        where (QUARTER(tr.start_date) = {report_quarter}
                                AND YEAR(tr.start_date) = {report_year})
                                or (QUARTER(tr.end_date) = {report_quarter}
                                AND YEAR(tr.end_date) = {report_year})
                        )
                        union (
                        SELECT
                            qmhk.id_patient
                        FROM
                            questionnaire_motherhivknowledge qmhk
                                where QUARTER(qmhk.date) = {report_quarter}
                                AND YEAR(qmhk.date) = {report_year}
                            )
                               UNION
                                ( SELECT
                            qms.id_patient
                        FROM
                            questionnaire_mothersurvey qms
                                where QUARTER(qms.date) = {report_quarter}
                                AND YEAR(qms.date) = {report_year} )
                            UNION (SELECT
                            qnhk.id_patient
                        FROM
                            questionnaire_newmotherhivknowledge qnhk
                            where QUARTER(qnhk.date) = {report_quarter}
                                    AND YEAR(qnhk.date) = {report_year}
                        )
                        UNION (SELECT
                            tp.id_patient_mother as id_patient
                            from tracking_pregnancy tp
                            where (QUARTER(tp.ptme_enrollment_date) = {report_quarter}
                                    AND YEAR(tp.ptme_enrollment_date) = {report_year}) or
                                    (QUARTER(tp.actual_delivery_date ) = {report_quarter}
                                    AND YEAR(tp.actual_delivery_date ) = {report_year})
                        )
                        UNION (SELECT
                            tmb.id_patient
                            from tracking_motherbasicinfo tmb
                            where QUARTER(tmb.PTME_date) = {report_quarter}
                                    AND YEAR(tmb.PTME_date) = {report_year}
                        )
                        UNION (SELECT
                        tracking_motherbasicinfo.id_patient
                            FROM
        tracking_motherbasicinfo
    LEFT JOIN patient ON patient.id = tracking_motherbasicinfo.id_patient
    LEFT JOIN testing_mereenfant ON CONCAT(testing_mereenfant.mother_city_code, '/', testing_mereenfant.mother_hospital_code, '/', testing_mereenfant.mother_code) = patient_code
    WHERE QUARTER(testing_mereenfant.date) = {report_quarter}
                                    AND YEAR(testing_mereenfant.date) = {report_year}
                        )
                        UNION (SELECT DISTINCT q.id_patient
                            FROM questionnaire_child q
                            WHERE QUARTER(q.date) = {report_quarter}
                                    AND YEAR(q.date) = {report_year}
                        )
                        UNION (SELECT DISTINCT id_patient FROM questionnaire_proftraining18 WHERE QUARTER(date) = {report_quarter}
                                    AND YEAR(date) = {report_year})
                        UNION ( SELECT DISTINCT id_patient FROM tracking_infant ti 
                        WHERE (QUARTER(ti.positive_pcr_1) = {report_quarter}
                                    AND YEAR(ti.positive_pcr_1) = {report_year})
                                    OR (QUARTER(ti.positive_pcr_2) = {report_quarter}
                                    AND YEAR(ti.positive_pcr_2) = {report_year})

                        )
                        UNION (SELECT DISTINCT id_patient FROM testing_result tr
                        WHERE (QUARTER(tr.blood_draw_date) = {report_quarter}
                                    AND YEAR(tr.blood_draw_date) = {report_year})
                                )
                        UNION (SELECT DISTINCT
                                patient.id as id_patient
                            FROM
                                caris_db.odk_child_visit
                                LEFT JOIN patient ON patient.patient_code = odk_child_visit.patient_code
                            WHERE
                                (QUARTER(odk_child_visit.date_of_visit) = {report_quarter}
                                    AND YEAR(odk_child_visit.date_of_visit) = {report_year})
                                     AND is_available_at_time_visit = 1
                        )
                        UNION
                        (
                            SELECT p.id as id_patient from odk_appel oa
                            left join patient p on p.patient_code=oa.patient_code
                            where QUARTER(oa.date_appel) = {report_quarter} AND YEAR(oa.date_appel) = {report_year}
                       )
                         UNION
                        (
                            SELECT p.id as id_patient from odk_appel oa
                            left join patient p on p.patient_code=oa.patient_code
                            WHERE QUARTER(oa.date_appel) = {report_quarter} AND YEAR(oa.date_appel) = {report_year}
                       )
                        """
        return query

    def get_ovc_serv_semester_query(self, report_year_1, report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation="commune"):
        query_1 = self.get_ovc_query_by_year_quarter(
            report_year_1, report_quarter_1)
        query_2 = self.get_ovc_query_by_year_quarter(
            report_year_2, report_qyarter_2)
        query = f"""SELECT
                        a.id_patient as id_patient from ({query_1}) a
                        left join
                        ({query_2}) b on a.id_patient=b.id_patient
                        where b.id_patient is not null"""
        query_final = self.get_aggregation_query(type_of_aggregation, query)

        return query_final

    def get_aggregation_query(self, type_of_aggregation, query):

        ptme_ovc_household_query = f"""
        SELECT 
                *
            FROM
                ((SELECT 
                    `a`.`id_patient`,
                        SUM(((`a`.`gender` = '2'))) AS `h_female`,
                        SUM(((`a`.`gender` = '1'))) AS `h_male`,
                        SUM(((`a`.`gender` = '2') AND (`a`.`age` < 1))) AS `h_f_under_1`,
                        SUM(((`a`.`gender` = '1') AND (`a`.`age` < 1))) AS `h_m_under_1`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 1 AND 4))) AS `h_f_1_4`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 1 AND 4))) AS `h_m_1_4`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 5 AND 9))) AS `h_f_5_9`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 5 AND 9))) AS `h_m_5_9`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 10 AND 14))) AS `h_f_10_14`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 10 AND 14))) AS `h_m_10_14`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 15 AND 17))) AS `h_f_15_17`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 15 AND 17))) AS `h_m_15_17`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` > 17))) AS `h_f_caregiver`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` > 17))) AS `h_m_caregiver`
                FROM
                    (SELECT 
                    hm.id_patient,
                        TIMESTAMPDIFF(YEAR, hm.dob, '2023-09-30') AS age,
                        IF(hm.gender = 'Masculin', 1, IF(hm.gender = 'Feminin', 2, hm.gender)) AS gender
                FROM
                    caris_db.household_mother hm
                WHERE
                    hm.id_patient IS NOT NULL
                HAVING age < 18) a
                GROUP BY a.id_patient) UNION (SELECT 
                    `a`.`id_patient`,
                        SUM(((`a`.`gender` = '2' AND `a`.`age` < 18))) + IF(SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` > 17))) > 0, 1, 0) AS `h_female`,
                        SUM(((`a`.`gender` = '1' AND `a`.`age` < 18))) + IF(SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` > 17))) > 0, 1, 0) AS `h_male`,
                        SUM(((`a`.`gender` = '2') AND (`a`.`age` < 1))) AS `h_f_under_1`,
                        SUM(((`a`.`gender` = '1') AND (`a`.`age` < 1))) AS `h_m_under_1`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 1 AND 4))) AS `h_f_1_4`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 1 AND 4))) AS `h_m_1_4`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 5 AND 9))) AS `h_f_5_9`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 5 AND 9))) AS `h_m_5_9`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 10 AND 14))) AS `h_f_10_14`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 10 AND 14))) AS `h_m_10_14`,
                        SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` BETWEEN 15 AND 17))) AS `h_f_15_17`,
                        SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` BETWEEN 15 AND 17))) AS `h_m_15_17`,
                        IF(SUM(((`a`.`gender` = '2')
                            AND (`a`.`age` > 17))) > 0, 1, 0) AS `h_f_caregiver`,
                        IF(SUM(((`a`.`gender` = '1')
                            AND (`a`.`age` > 17))) > 0, 1, 0) AS `h_m_caregiver`
                FROM
                    (SELECT 
                    hm.id_patient,
                        TIMESTAMPDIFF(YEAR, hm.dob, '2023-09-30') AS age,
                        IF(hm.gender = 'Masculin', 1, IF(hm.gender = 'Feminin', 2, hm.gender)) AS gender
                FROM
                    caris_db.household_child hm
                WHERE
                    hm.id_patient IS NOT NULL) a
                GROUP BY a.id_patient)) d
            GROUP BY d.id_patient
        """
        aggregations = [
            {"departement": "d.name"},
            {"commune": "c.name"}
        ]
        select = "a.id_patient as id_patient"
        group_by = ""
        order_by = ""
        aggregation_keys = [
            key for aggregation in aggregations for key in aggregation.keys()]
        if type_of_aggregation in aggregation_keys:
            select = ""
            for aggregation in aggregations:
                for key, value in aggregation.items():
                    select += f"{value} as {key} ,"
                    group_by += f"{value} ,"
                    order_by += f"{value} ,"
                if type_of_aggregation in aggregation.keys():
                    break
            group_by = " group by "+group_by[:-1]
            male = 1
            female = 2
            select = select + f''' count(*) as total ,
            sum(pg.gender={male} and pg.gender is not null) as male,
            sum(pg.gender={female} and pg.gender is not null) as female,
            sum(pg.gender is null) as unknown_gender,
            SUM(pg.gender={female} and pg.age<1 and (pg.gender is not null) and pg.age is not null) as f_under_1,
            sum(pg.gender={female} and (pg.age between 1 and 4) and (pg.gender is not null) and pg.age is not null ) as f_1_4,
            sum(pg.gender={female} and (pg.age between 5 and 9) and (pg.gender is not null) and pg.age is not null ) as f_5_9,
            sum( pg.gender={female} and (pg.age between 10 and 14) and (pg.gender is not null) and pg.age is not null ) as f_10_14,
            sum( pg.gender={female} and (pg.age between 15 and 17) and (pg.gender is not null) and pg.age is not null ) as f_15_17,
            sum( pg.gender={female} and (pg.age between 18 and 20) and (tmb.id_patient is null) and ( pg.gender is not null) and pg.age is not null ) as f_18_20,
            sum( pg.gender={female} and (pg.age>20 or ( (tmb.id_patient is not null) and pg.age>17 )) and (pg.gender is not null) and pg.age is not null ) as f_caregiver,
            SUM(pg.gender={male} and pg.age<1 and (pg.gender is not null) and pg.age is not null) as m_under_1,
            sum(pg.gender={male} and (pg.age between 1 and 4) and (pg.gender is not null) and pg.age is not null ) as m_1_4,
            sum(pg.gender={male} and (pg.age between 5 and 9) and (pg.gender is not null) and pg.age is not null ) as m_5_9,
            sum( pg.gender={male} and (pg.age between 10 and 14) and (pg.gender is not null) and pg.age is not null ) as m_10_14,
            sum( pg.gender={male} and (pg.age between 15 and 17) and (pg.gender is not null) and pg.age is not null ) as m_15_17,
            sum( pg.gender={male} and (pg.age between 18 and 20) and ( pg.gender is not null) and pg.age is not null ) as m_18_20,
            sum( pg.gender={male} and (pg.age>20) and (pg.gender is not null) and pg.age is not null ) as m_cargiver,
            sum(hs.h_male) as h_male, 
            sum(hs.h_female) as h_female,
            SUM(hs.h_f_under_1) as h_f_under_1,
            sum(hs.h_f_1_4) as h_f_1_4,
            sum(hs.h_f_5_9 ) as h_f_5_9,
            sum(hs.h_f_10_14) as h_f_10_14,
            sum(hs.h_f_15_17) as h_f_15_17,
            sum(hs.h_f_caregiver) as h_f_caregiver,
            SUM(hs.h_m_under_1) as h_m_under_1,
            sum(hs.h_m_1_4) as h_m_1_4,
            sum(hs.h_m_5_9) as h_m_5_9,
            sum(hs.h_m_10_14) as h_m_10_14,
            sum(hs.h_m_15_17) as h_m_15_17,
            sum(hs.h_m_caregiver) as h_m_caregiver
            '''
            order_by = " order by "+order_by[:-1]

        query_final = f"""SELECT
                         {select} from ({query}) a
                         left join ({ptme_ovc_household_query}) hs on hs.id_patient=a.id_patient
                        left join patient_gender_age_view pg on pg.id_patient=a.id_patient
                        left join patient p on p.id=a.id_patient
                        left join lookup_hospital h on h.city_code=p.city_code and h.hospital_code=p.hospital_code
                        left join lookup_commune c on h.commune=c.id
                        left join lookup_departement d on d.id=c.departement
                        left join lookup_office o on o.id=h.office
                        left join tracking_motherbasicinfo tmb on tmb.id_patient=a.id_patient
                        where p.linked_to_id_patient=0 and h.network !=6
                        {group_by}
                        """
                        
        return query_final
    

    def get_ptme_semester_query(self, report_year_1, report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation="commune"):
        query_1 = self.get_ptme_query_by_year_quarter(
            report_year_1, report_quarter_1)
        query_2 = self.get_ptme_query_by_year_quarter(
            report_year_2, report_qyarter_2)
        query = f"""SELECT
                        a.id_patient as id_patient from ({query_1}) a
                        left join
                        ({query_2}) b on a.id_patient=b.id_patient
                        where b.id_patient is not null"""
        query_final = self.get_aggregation_query(type_of_aggregation, query)
        return query_final
    


    def get_ovc_serv_semester(self, report_year_1, report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation=None):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = self.get_ovc_serv_semester_query(
                    report_year_1, report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation)
                cursor.execute(query)
                return cursor.fetchall()
                # return pd.read_sql(query, conn)
            except Exception as e:
                print(e)
                return []
            
    
    def get_ptme_semester(self, report_year_1, report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation=None):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = self.get_ptme_semester_query(
                    report_year_1, report_quarter_1, report_year_2, report_qyarter_2, type_of_aggregation)
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
    
            
    
