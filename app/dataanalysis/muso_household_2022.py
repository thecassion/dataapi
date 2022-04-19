import  pandas as pd
from db.mysql import engine, sql_achemy_engine
from db.muso_household_2022 import MusoHousehold2022 as MusoHousehold2022db

class MusoHousehold2022:
    def __init__(self, data):
        if "hiv_households" in data:
            self.hiv_households = data["hiv_households"]
        else:
            Exception("hiv_households not found in data")
        if "cc_households" in data:
            self.cc_households = data["cc_households"]
        else:
            Exception("cc_households not found in data")
        if "max_pos_households_by_beneficiary" in data:
            self.max_pos_households_by_beneficiary = data["max_pos_households_by_beneficiary"]
        else:
            Exception("max_pos_households_by_beneficiary not found in data")

    def get_households_count(self):
        """
        Get the number of households
        """
        return len(self.hiv_households)
    def get_household_not_on_hiv(self):
        df_cc = pd.DataFrame(self.cc_households)
        df_hiv = pd.DataFrame(self.hiv_households)
        __columns = list(df_cc.columns)

        """
        Verify that the case id is not in the household table
        """
        df = pd.merge(df_cc,df_hiv, how="left",right_on="case_id",left_on="case_id",suffixes=(None,"__hs"))
        df = df[df["id_patient"].isna()]
        df = df[__columns]
        df.fillna('None',inplace=True)
        return df.to_dict("records")


    def get_beneficiaries_max_pos_with_household(self):
        df_ben = pd.DataFrame(self.max_pos_households_by_beneficiary)
        df_hs_cc = pd.DataFrame(self.get_household_not_on_hiv())["parent_id"]
        df_hs_cc = df_hs_cc.drop_duplicates(keep='first')
        df = pd.merge(df_ben,df_hs_cc,how="left", left_on="muso_case_id", right_on="parent_id")
        df = df[df["parent_id"].notna()]
        df =df[list(df_ben.columns)]
        return df.to_dict("records")


    def generate_pos_by_beneficiary(self):
        """
        Generate the max_pos household by_beneficiary
        """
        households =[]
        for beneficiary in self.max_pos_households_by_beneficiary:
            i = 1
            hs = filter(lambda x: x["parent_id"]==beneficiary["muso_case_id"], self.get_household_not_on_hiv() )
            if hs is None:
                break
            for cc_household in hs:
                cc_household["pos"]=int(beneficiary["max_pos"])+ i
                if int(cc_household["sexe"])==1:
                    cc_household["sexe"]="M"
                elif int(cc_household["sexe"])==2:
                    cc_household["sexe"]= "F"
                cc_household['id_patient']=int(beneficiary["id_patient"])
                households.append(cc_household)
                i+=1
            print(" finish with beneficiary : ", beneficiary["id_patient"])
        return households

    def insert_households_to_db(self):
        """
        Generate the max_pos household by_beneficiary
        """
        households =[]
        __beneficiaries = self.get_beneficiaries_max_pos_with_household()
        ln_ben = len(__beneficiaries)
        j = 0
        for beneficiary in __beneficiaries :
            j+=1
            i = 1
            hs = filter(lambda x: x["parent_id"]==beneficiary["muso_case_id"], self.get_household_not_on_hiv() )
            if hs is None:
                break
            for cc_household in hs:
                cc_household["pos"]=beneficiary["max_pos"]+ i
                if int(cc_household["sexe"])==1:
                    cc_household["sexe"]="M"
                elif int(cc_household["sexe"])==2:
                    cc_household["sexe"]= "F"
                cc_household["created_by"]=120
                cc_household['id_patient']=int(beneficiary["id_patient"])
                households.append(cc_household)
                # db_muso_household = MusoHousehold2022db()
                # db_muso_household.insert_household2022(cc_household)
                i+=1
            print(" finish with beneficiary : "+str(beneficiary["id_patient"]) +"  "+str(j)+"/"+str(ln_ben))
        print("Save with pandas")
        df_hs = pd.DataFrame(households)[['pos','age','id_patient','sexe','arv','test','often_sick','case_id','created_by','user_id']]
        df_hs.to_sql("muso_household_2022", con=sql_achemy_engine(), if_exists="append", index=False)
        return households