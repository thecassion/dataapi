import  pandas as pd
from db.mysql import engine, sql_achemy_engine
from db.muso_household_2022 import MusoHousehold2022

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

    def generate_pos_by_beneficiary(self):
        """
        Generate the max_pos household by_beneficiary
        """
        households =[]
        for beneficiary in self.max_pos_households_by_beneficiary:
            i = 1
            hs = filter(lambda x: x["parent_id"]==beneficiary["muso_case_id"], self.cc_households )
            if hs is None:
                break
            for cc_household in hs:
                cc_household["pos"]=beneficiary["max_pos"]+ i
                if int(cc_household["sexe"])==1:
                    cc_household["sexe"]="M"
                elif int(cc_household["sexe"])==2:
                    cc_household["sexe"]= "F"
                cc_household['id_patient']=int(beneficiary["id_patient"])
                households.append(cc_household)
                i+=1
            print(" finish with beneficiary : ", beneficiary["id_patient"])
        return households