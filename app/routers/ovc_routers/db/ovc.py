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