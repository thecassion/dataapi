from ..CommCare import GardenRegistrationForm
import pandas as pd
from ..core import sql_achemy_engine, engine
class Gardening:
    def __init__(self):
        pass
    def get_registration_forms(self):
        garden_obj = GardenRegistrationForm()
        forms = garden_obj.get()


        df = pd.DataFrame(forms)
        #remove # and @ from column names
        df.columns = df.columns.str.replace("#","")
        df.columns = df.columns.str.replace("@","")
        df.columns = df.columns.str.replace(" ","_")
        df.columns = df.columns.str.replace("(","")
        df.columns = df.columns.str.replace(")","")
        df.columns = df.columns.str.replace("/","_")
        df.columns = df.columns.str.replace("-","_")
        df.columns = df.columns.str.replace(".","_")
        df.columns = df.columns.str.replace("__","_")
        df.columns = df.columns.str.replace("__","_")
        df.drop(columns=["meta","case"], inplace=True)

        df.to_sql("gardening_registration_forms",sql_achemy_engine(), if_exists="replace")
        return forms