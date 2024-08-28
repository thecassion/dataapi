from ....core import engine, sql_achemy_engine

class OVC:
    def __init__(self):
        pass
    def get_club_6_month_active_info_by_site(self):
        query = """
            SELECT 
                a.departement,
                a.commune,
                a.site,
                COUNT(*) AS nbre,
                SUM(a.club_type = 5) AS enfant_3_5,
                SUM(a.club_type = 6) AS enfant_6_8,
                SUM(a.club_type = 2) AS enfant_9_12,
                SUM(a.club_type = 3) AS enfant_13_17,
                SUM(a.club_type = 4) AS enfant_18_plus
            FROM
                (SELECT 
                    cs.id_club,
                        c.id_hospital,
                        lc.name AS commune,
                        ld.name AS departement,
                        concat(lh.city_code,"/", lh.hospital_code) AS site,
                        c.club_type
                FROM
                    club_session cs
                LEFT JOIN club c ON c.id = cs.id_club
                LEFT JOIN lookup_club_type lct ON lct.id = c.club_type
                LEFT JOIN lookup_hospital lh ON lh.id = c.id_hospital
                LEFT JOIN lookup_commune lc ON lc.id = lh.commune
                LEFT JOIN lookup_departement ld ON ld.id = lc.departement
                WHERE
                    TIMESTAMPDIFF(MONTH, cs.date, NOW()) <= 6
                        AND c.club_type != 1
                GROUP BY cs.id_club) a
            GROUP BY a.site
        """
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
            

    def get_club_6_month_active_ovc_in_club_by_site(self):
        query = """
            SELECT 
                a.departement,
                a.commune,
                a.site,
                COUNT(*) AS nbre,
                SUM(a.club_type = 5) AS enfant_3_5,
                SUM(a.club_type = 6) AS enfant_6_8,
                SUM(a.club_type = 2) AS enfant_9_12,
                SUM(a.club_type = 3) AS enfant_13_17,
                SUM(a.club_type = 4) AS enfant_18_plus
            FROM
                (SELECT 
                    s.id_patient,
                        cs.id_club,
                        c.id_hospital,
                        concat(lh.city_code, lh.hospital_code) AS site,
                        lc.name AS commune,
                        ld.name AS departement,
                        c.club_type
                FROM
                    session s
                LEFT JOIN club_session cs ON cs.id = s.id_club_session
                LEFT JOIN club c ON c.id = cs.id_club
                LEFT JOIN lookup_club_type lct ON lct.id = c.club_type
                LEFT JOIN lookup_hospital lh ON lh.id = c.id_hospital
                LEFT JOIN lookup_commune lc ON lc.id = lh.commune
                LEFT JOIN lookup_departement ld ON ld.id = lc.departement
                WHERE
                    TIMESTAMPDIFF(MONTH, cs.date, NOW()) <= 6
                        AND c.club_type != 1 AND s.is_present=1
                GROUP BY s.id_patient) a
            GROUP BY a.site
        """
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []
    
    def get_positive_child_and_art_by_site_infos(self):
        query = """
SELECT 
    m.departement,
    m.commune,
    m.site,
    COUNT(m.positive_id) AS nbre_positive,
    SUM(m.positive_id IS NOT NULL
        AND (m.active_patient IS NOT NULL)) AS nbre_positive_active,
    SUM(m.positive_id IS NOT NULL AND m.age < 18) AS nbre_positive_under_18,
    SUM(m.positive_id IS NOT NULL AND m.age < 18
        AND (m.active_patient IS NOT NULL)) AS nbre_positive_active_under_18,
    COUNT(m.arv_id) AS nbre_on_arv,
    SUM(m.arv_id IS NOT NULL
        AND (m.active_patient IS NOT NULL)) AS nbre_on_arv_active,
    SUM(m.arv_id IS NOT NULL AND m.age < 18) AS nbre_on_arv_under_18,
    SUM(m.arv_id IS NOT NULL AND m.age < 18
        AND (m.active_patient IS NOT NULL)) AS nbre_on_arv_active_under_18,
    SUM(m.viral_load_test_in_the_last_12months_id_patient IS NOT NULL
        AND m.age < 18) AS nbre_viral_load_test_in_the_last_12months_under_18,
    SUM(m.viral_load_test_in_the_last_12months_id_patient IS NOT NULL
        AND m.age < 18
        AND (m.active_patient IS NOT NULL)) AS nbre_viral_load_test_in_the_last_12months_active_under_18,
    SUM(m.viral_load_result_in_the_last_12months_and_suppress_id_patient IS NOT NULL
        AND m.age < 18) AS nbre_viral_load_result_in_the_last_12months_and_suppress_under_18,
    SUM(m.viral_load_result_in_the_last_12months_and_suppress_id_patient IS NOT NULL
        AND m.age < 18
        AND (m.active_patient IS NOT NULL)) AS nbre_viral_load_result_in_the_last_12months_and_suppress_active_under_18,
    SUM(m.last_viral_load_suppress_in_the_last_12months_id_patient IS NOT NULL
        AND m.age < 18) AS nbre_last_viral_load_suppress_in_the_last_12months_under_18,
    SUM(m.last_viral_load_suppress_in_the_last_12months_id_patient IS NOT NULL
        AND m.age < 18
        AND (m.active_patient IS NOT NULL)) AS nbre_last_viral_load_suppress_in_the_last_12months_active_under_18
FROM
    (SELECT 
        ld.name AS departement,
            lc.name AS commune,
            CONCAT(lh.city_code, '/', lh.hospital_code) AS site,
            a.id_patient AS positive_id,
            arv.id_patient AS arv_id,
            vt.id_patient AS viral_load_test_in_the_last_12months_id_patient,
            vrs.id_patient AS viral_load_result_in_the_last_12months_and_suppress_id_patient,
            lvrs.id_patient AS last_viral_load_suppress_in_the_last_12months_id_patient,
            ap.id_patient AS active_patient,
            TIMESTAMPDIFF(YEAR, ti.dob, NOW()) AS age
    FROM
        (SELECT 
        id AS id_patient
    FROM
        view_patient_positive UNION (SELECT 
        id_patient
    FROM
        tracking_regime t_reg
    WHERE
        t_reg.category = 'regime_infant_treatment') UNION (SELECT 
        id_patient
    FROM
        club_patient cp
    LEFT JOIN club c ON c.id = cp.id_club
    WHERE
        c.club_type != 1)) a
    LEFT JOIN (SELECT 
        p.id AS id_patient
    FROM
        tracking_regime tr
    LEFT JOIN lookup_arv la ON la.id = tr.id_arv
    LEFT JOIN patient p ON p.id = tr.id_patient
    LEFT JOIN tracking_infant ti ON ti.id_patient = p.id
    WHERE
        tr.category = 'regime_infant_treatment'
            AND ti.id_patient IS NOT NULL
    GROUP BY tr.id_patient) arv ON arv.id_patient = a.id_patient
    LEFT JOIN (SELECT DISTINCT
        tf.id_patient
    FROM
        tracking_followup tf
    WHERE
        TIMESTAMPDIFF(MONTH, tf.viral_load_collection_date, NOW()) <= 12) vt ON vt.id_patient = a.id_patient
    LEFT JOIN (SELECT DISTINCT
        tf.id_patient
    FROM
        tracking_followup tf
    WHERE
        TIMESTAMPDIFF(MONTH, tf.viral_load_date, NOW()) <= 12
            AND (tf.viral_load_count IS NULL
            OR tf.viral_load_count < 1000)) vrs ON vrs.id_patient = a.id_patient
    LEFT JOIN (SELECT DISTINCT
        tf.id_patient
    FROM
        tracking_followup tf
    WHERE
        TIMESTAMPDIFF(MONTH, tf.viral_load_date, NOW()) <= 12
            AND tf.viral_load_date = (SELECT 
                MAX(tf2.viral_load_date)
            FROM
                tracking_followup tf2
            WHERE
                tf2.id_patient = tf.id_patient)
            AND (tf.viral_load_count < 1000
            OR (tf.viral_load_count IS NULL))
    GROUP BY tf.id_patient) lvrs ON lvrs.id_patient = a.id_patient
    LEFT JOIN (SELECT DISTINCT
        id_patient
    FROM
        tracking_followup tf
    WHERE
        TIMESTAMPDIFF(MONTH, tf.date, NOW()) <= 3) ap ON ap.id_patient = a.id_patient
    LEFT JOIN tracking_infant ti ON ti.id_patient = a.id_patient
    LEFT JOIN tracking_motherbasicinfo tm ON tm.id_patient = a.id_patient
    LEFT JOIN patient p ON p.id = a.id_patient
    LEFT JOIN lookup_hospital lh ON CONCAT(lh.city_code, '/', lh.hospital_code) = CONCAT(p.city_code, '/', p.hospital_code)
    LEFT JOIN lookup_commune lc ON lc.id = lh.commune
    LEFT JOIN lookup_departement ld ON ld.id = lc.departement
    WHERE
        (tm.id_patient IS NULL)
            AND (ti.is_dead IS NULL OR ti.is_dead = 0)
            AND (ti.is_abandoned IS NULL
            OR ti.is_abandoned = 0)) m
GROUP BY m.site
        """
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []


    def get_mastersheet_stat_by_site(self):
        query = """
                SELECT
                    a.office,
                    departement,
                    commune,
                    a.site,
                    COUNT(*) AS total,
                    SUM(LTFU_30days = 'No') AS nbr_non_LTFU_30days,
                    SUM(TIMESTAMPDIFF(MONTH,
                        next_appointment_date,
                        NOW()) >= 12) AS nbr_LTFU_12months_plus,
                    SUM(TIMESTAMPDIFF(MONTH,
                        a.next_appointment_date,
                        NOW()) >= 1
                        AND TIMESTAMPDIFF(MONTH,
                        a.next_appointment_date,
                        NOW()) < 6) AS SUM_LTFU_1month_inf_6months,
                    SUM(TIMESTAMPDIFF(MONTH,
                        a.next_appointment_date,
                        NOW()) >= 6
                        AND TIMESTAMPDIFF(MONTH,
                        a.next_appointment_date,
                        NOW()) < 12) AS SUM_LTFU_6_inf12months,
                    SUM(TIMESTAMPDIFF(MONTH,
                        a.next_appointment_date,
                        NOW()) >= 12) AS SUM_LTFU_12months_plus,
                    SUM(a.age < 15 AND (is_abandoned=0 or is_abandoned is null) and LTFU_30days="No"
                    AND (b.network not in ('PIH', 'MSPP', 'UGP'))
                    ) AS TX_CURR,
                    SUM(
                    (a.last_viral_load_collection_date is not null) AND
                    TIMESTAMPDIFF(DAY,
                        a.last_viral_load_collection_date,
                        NOW()) <= 365
                        AND a.age < 15
                        AND (is_abandoned=0 or is_abandoned is null)
                        and LTFU_30days="No"
                        AND (b.network not in ('PIH', 'MSPP', 'UGP'))
                        ) AS coverage,
                    SUM(
                    (a.last_viral_load_collection_date is not null) and
                    TIMESTAMPDIFF(DAY,
                        a.last_viral_load_collection_date,
                        NOW()) <= 365
                        AND 
                    TIMESTAMPDIFF(DAY,
                    a.viral_load_date,
                    NOW()) <=365
                    AND 
                        a.age < 15
                        AND a.indetectable_ou_inf_1000 = 'OUI'
                        AND (is_abandoned=0 or is_abandoned is null)
                        and LTFU_30days="No"
                        AND (b.network not in ('PIH', 'MSPP', 'UGP'))
                        ) AS suppression
                FROM
                    caris_db.mastersheet_children a
                        LEFT JOIN
                    (SELECT
                        ld.name AS departement,
                            lc.name AS commune,
                            ls.name AS section,
                            CONCAT(lh.city_code, '/', lh.hospital_code) AS site,
                            lh.name AS hospital_name,
                            ln.name AS network
                    FROM
                        lookup_hospital lh
                    LEFT JOIN lookup_section ls ON ls.id = lh.section
                    LEFT JOIN lookup_commune lc ON lc.id = lh.commune
                    LEFT JOIN lookup_departement ld ON ld.id = lc.departement
                    LEFT JOIN lookup_network ln ON ln.id = lh.network) b ON a.site = b.site
                WHERE
                    NOT (b.departement IN ('Nippes' , 'Sud', 'Sud-Est', 'Grand-Anse')
                        OR office IN ('JER' , 'CAY', 'FDN', 'MIR'))
                        AND is_ugp != 'Yes'
                GROUP BY site
                ORDER BY COUNT(*) DESC

        """

        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []

    def get_mastersheet_stat(self):
        query = """
SELECT
    COUNT(*) AS total,
    SUM(LTFU_30days = 'No') AS nbr_non_LTFU_30days,
    SUM(TIMESTAMPDIFF(MONTH,
        next_appointment_date,
        NOW()) >= 12) AS nbr_LTFU_12months_plus,
    SUM(TIMESTAMPDIFF(MONTH,
        a.next_appointment_date,
        NOW()) >= 1
        AND TIMESTAMPDIFF(MONTH,
        a.next_appointment_date,
        NOW()) < 6) AS SUM_LTFU_1month_inf_6months,
    SUM(TIMESTAMPDIFF(MONTH,
        a.next_appointment_date,
        NOW()) >= 6
        AND TIMESTAMPDIFF(MONTH,
        a.next_appointment_date,
        NOW()) < 12) AS SUM_LTFU_6_inf12months,
    SUM(TIMESTAMPDIFF(MONTH,
        a.next_appointment_date,
        NOW()) >= 12) AS SUM_LTFU_12months_plus,
    SUM(a.age < 15 AND (is_abandoned=0 or is_abandoned is null) and LTFU_30days="No"
    AND (b.network not in ('PIH', 'MSPP', 'UGP'))
    ) AS TX_CURR,
    SUM(
    (a.last_viral_load_collection_date is not null) AND
    TIMESTAMPDIFF(DAY,
        a.last_viral_load_collection_date,
        NOW()) <= 365
        AND a.age < 15
         AND (is_abandoned=0 or is_abandoned is null)
		and LTFU_30days="No"
        AND (b.network not in ('PIH', 'MSPP', 'UGP'))
        
        ) AS vl_coverage,
    SUM(
    (a.last_viral_load_collection_date is not null) and
    TIMESTAMPDIFF(DAY,
        a.last_viral_load_collection_date,
        NOW()) <= 365
        AND 
       TIMESTAMPDIFF(DAY,
       a.viral_load_date,
       NOW()) <=365
       AND 
        a.age < 15
        AND a.indetectable_ou_inf_1000 = 'OUI'
        AND (is_abandoned=0 or is_abandoned is null)
		and LTFU_30days="No"
        AND (b.network not in ('PIH', 'MSPP', 'UGP'))
        ) AS vl_suppression
FROM
    caris_db.mastersheet_children a
        LEFT JOIN
    (SELECT 
        ld.name AS departement,
            lc.name AS commune,
            ls.name AS section,
            CONCAT(lh.city_code, '/', lh.hospital_code) AS site,
            lh.name AS hospital_name,
            ln.name AS network
    FROM
        lookup_hospital lh
    LEFT JOIN lookup_section ls ON ls.id = lh.section
    LEFT JOIN lookup_commune lc ON lc.id = lh.commune
    LEFT JOIN lookup_departement ld ON ld.id = lc.departement
    LEFT JOIN lookup_network ln ON ln.id = lh.network) b ON a.site = b.site
WHERE
    NOT (b.departement IN ('Nippes' , 'Sud', 'Sud-Est', 'Grand-Anse')
        OR office IN ('JER' , 'CAY', 'FDN', 'MIR'))
        AND 
        is_ugp != 'Yes'
            """
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []