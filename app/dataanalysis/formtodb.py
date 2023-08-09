from ..core import CommCareAPI, sql_achemy_engine
import pandas as pd
from flatten_dict import flatten 
from flatten_dict.reducers import make_reducer
class FormToDB:
    def __init__(self, form_xlmns, table_name) -> None:
        self.form_xlmns = form_xlmns
        # exception if table_name is not starting with odk_form_
        try:
            if table_name[:9] != "odk_form_":
                raise Exception("table_name must start with odk_form_")
            # if it is null, raise exception
            elif table_name == None or table_name == "" or form_xlmns == None or form_xlmns == "":
                raise Exception("table_name and form_xlmns must not be null")
            else:
                self.table_name = table_name
        except Exception as e:
            raise e
        self.cc = CommCareAPI("caris-test", "0.5")

    def getForms(self):
        try:
            forms = self.cc.get_forms(self.form_xlmns, 1000)
            properties = [flatten({**case["form"],"case_id":case["form"]["case"]["@case_id"] },reducer=make_reducer(delimiter="_")) for case in forms]
            return properties
        except Exception as e:
            print("exception form : ", e)
            raise e
    
    def save(self):
        forms = self.getForms()
        if len(forms) > 0:
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

            # take the last 62 characters in the name of a column if it is more than 62 characters
            df.columns = [col[-62:] if len(col) > 62 else col for col in df.columns]
            # df.columns = [col[:50] if len(col) > 62 else col for col in df.columns]
            # df.drop(columns=["meta","case"], inplace=True)
            df.to_sql(self.table_name,sql_achemy_engine(), if_exists="replace")
            return forms