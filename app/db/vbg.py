from ..core import engine, sql_achemy_engine
import  pandas as pd

class Vbg:
    def __init__(self) -> None:
        pass
    def get_vbg_groupes_infos(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = """
                    SELECT
                        vg.departement_name AS departement,
                        vg.commune_name AS commune,
                        COUNT(*) AS nbre,
                        SUM(vg.age_category = '10_14') AS '10_14',
                        SUM(vg.age_category = '15_17') AS '15_17',
                        SUM(vg.age_category = '18_plus') AS '18_plus'
                    FROM
                        caris_db.vbg_groupes vg
                    WHERE
                        vg.user_id != 'bf27059115614b0a88d507f8e17cf4c0'
                    GROUP BY vg.commune_name
                """
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []

    def get_vbg_groupe_members_infos(self):
        e = engine()
        with e as conn:
            try:
                cursor = conn.cursor()
                query = """
                    SELECT 
                        vgm.departement_name AS departement,
                        vgm.commune_name AS commune,
                        COUNT(*) AS nbre,
                        SUM(vgm.violence_type LIKE '%violence_verbale%') AS violence_verbale,
                        SUM(vgm.violence_type LIKE '%physique%') AS physique,
                        SUM(vgm.violence_type LIKE '%sexuelle%') AS sexuelle,
                        SUM(vgm.violence_type LIKE '%economique%') AS economique,
                        SUM(vgm.violence_type LIKE '%psychologique%') AS psychologique,
                        SUM(vgm.violence_type LIKE '%domestique%') AS domestique,
                        SUM(vgm.violence_type LIKE '%conjugale%') AS conjugale,
                        SUM(vgm.violence_type LIKE '%harcelement_sexuel%') AS harcelement_sexuel,
                        SUM(vgm.violence_type IS NULL) AS null_violence_type
                    FROM
                        caris_db.vbg_groupe_members vgm
                    WHERE
                        vgm.user_id != 'bf27059115614b0a88d507f8e17cf4c0'
                    GROUP BY vgm.commune_name
                """
                cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                print(e)
                return []