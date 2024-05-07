from ....core import engine, sql_achemy_engine

class OVC:
    def __init__(self):
        pass
    def get_club_6_month_active_info_by_commune(self):
        query = """
            SELECT 
                a.departement,
                a.commune,
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
            GROUP BY a.commune
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
            

    def get_club_6_month_active_ovc_in_club_by_commune(self):
        query = """
            SELECT 
                a.departement,
                a.commune,
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
            GROUP BY a.commune
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
    
    def get_positive_child_and_art_by_commune_infos(self):
        query = """
                SELECT 
                    m.departement,
                    m.commune,
                    COUNT(m.positive_id) AS nbre_positive,
                    COUNT(m.arv_id) AS nbre_on_arv
                FROM
                    (SELECT 
                        ld.name AS departement,
                            lc.name AS commune,
                            a.id_patient AS positive_id,
                            arv.id_patient AS arv_id
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
                    LEFT JOIN tracking_infant ti ON ti.id_patient = a.id_patient
                    LEFT JOIN patient p ON p.id = ti.id_patient
                    LEFT JOIN lookup_hospital lh ON CONCAT(lh.city_code, '/', lh.hospital_code) = CONCAT(p.city_code, '/', p.hospital_code)
                    LEFT JOIN lookup_commune lc ON lc.id = lh.commune
                    LEFT JOIN lookup_departement ld ON ld.id = lc.departement
                    WHERE
                        (ti.id_patient IS NOT NULL)
                            AND (ti.is_dead IS NULL OR ti.is_dead = 0) and (ti.is_abandoned is null or ti.is_abandoned=0) ) m
                GROUP BY m.departement , m.commune
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