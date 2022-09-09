import pandas as pd

from app.db import muso_beneficiary
from ..core import engine, sql_achemy_engine

from ..db import MusoBeneficiary

class MusoBeneficiaries:
    """
    Class to store Muso beneficiaries data
    """
    def __init__(self, data):
        if isinstance(data, dict):
            if "hiv_beneficiaries" in data:
                self.hiv_beneficiaries = data["hiv_beneficiaries"]
            else:
                Exception("hiv_beneficiaries not found in data")
            if "cc_beneficiaries" in data:
                self.cc_beneficiaries = data["cc_beneficiaries"]
            else:
                Exception("cc_beneficiaries not found in data")
            if "max_rank_beneficiaries_by_groups" in data:
                self.max_rank_beneficiaries_by_groups = data["max_rank_beneficiaries_by_groups"]
            else:
                Exception("max_rank_beneficiaries_by__groups not found in data")

    def get_beneficiaries_count(self):
        """
        Get the number of beneficiaries
        """
        return self.beneficiaries_count
    def get_cc_beneficiairies_with_external_id(self):
        """
        Get the number of beneficiaries on HIV/Haiti but not on CommCare
        """
        df = pd.DataFrame(self.cc_beneficiaries)
        # df["external_id"] = pd.to_numeric(df["external_id"], errors="coerce")
        # df["external_id"] = df["external_id"].astype(int)
        df = df[df["external_id"].notna()]
        return df.to_dict("records")

    def update_beneficiaries_case_id(self):
        """
        Update the case_id of beneficiaries on CommCare
        """
        df = pd.DataFrame(self.cc_beneficiaries)
        df = df[df["external_id"].notna()]
        # df["external_id"] = pd.to_numeric(df["external_id"], errors="coerce")
        # df["external_id"] = df["external_id"].astype(int)
        df = df[df["external_id"].notna()]
        df = df[["case_id","external_id"]]
        df["id"]=df["external_id"]
        df = df[["case_id","id"]]
        g = df.to_dict("records")
        muso_beneficiaries = MusoBeneficiary()
        muso_beneficiaries.update_muso_beneficiaries_case_id(g)

    def cc_beneficiaries_without_external_id(self):
        """
        Get the number of beneficiaries on CommCare but not on HIV/Haiti
        """
        df = pd.DataFrame(self.cc_beneficiaries)
        df = df[df["external_id"].isna()]
        return df.to_dict("records")
    def cc_beneficiaries_without_external_id_and_patient_code(self):
        """
        Get the number of beneficiaries on CommCare but not on HIV/Haiti
        """
        df = pd.DataFrame(self.cc_beneficiaries)
        df = df[df["external_id"].isna()]
        df = df[(df["patient_code"].isna()) | (df["patient_code"] == "//")]
        __columns = list(df.columns)
        """
        Verify that the case id is not in the patient table with muso_case_id
        """
        df_patient = pd.read_sql_table("patient", sql_achemy_engine())
        df = pd.merge(df, df_patient, how="left", right_on="muso_case_id", left_on="case_id",suffixes=(None, "__patient"))
        df = df[df["patient_code__patient"].isna()]
        df = df[__columns]

        __listes = ["is_inactive","graduated","is_abandoned","is_pvvih"]

        for __liste in __listes:
            df[__liste] = pd.to_numeric(df[__liste], errors="coerce").fillna(0).astype(int)


        df[["inactive_date","abandoned_date","graduation_date"]] = df[["inactive_date","abandoned_date","graduation_date"]].fillna('')


        df.fillna('', inplace=True)
        return df.to_dict("records")

    def generate_rank_by_groups(self):
        """
        Generate the rank of each beneficiary by groups
        """
        beneficiaries=[]
        __cc_benificiary_without_external_id = self.cc_beneficiaries_without_external_id_and_patient_code()
        for group in self.max_rank_beneficiaries_by_groups:
            i=1
            for cc_benificiary in __cc_benificiary_without_external_id:
                if cc_benificiary["parent_id"] == group["group_case_id"]:
                    cc_benificiary["rank"] = group["max_rank"] + i
                    cc_benificiary["which_program"] = "MUSO"
                    cc_benificiary["linked_to_id_patient"] = 0
                    cc_benificiary["created_by"] = 120
                    cc_benificiary["id_group"] = group["id_group"]
                    if cc_benificiary["gender"].lower()=="m":
                        cc_benificiary["gender"]=1
                    elif cc_benificiary["gender"].lower()=="f":
                        cc_benificiary["gender"]=2
                    elif cc_benificiary["gender"]=="":
                        cc_benificiary["gender"]=0

                    l_dates = ["inactive_date","abandoned_date","graduation_date"]
                    # for l_date in l_dates:
                    #     if cc_benificiary[l_date] ==null:
                    #         cc_benificiary[l_date] = None

                    # cc_benificiary["gender"]=int(cc_benificiary["gender"])
                    # if "pvih" in cc_benificiary:
                    #     if cc_benificiary["is_pvih"] !=None:
                    #         cc_benificiary["is_pvvih"] = cc_benificiary["is_pvih"]
                    # cc_benificiary["is_pvvih"] = int(cc_benificiary["is_pvvih"])
                    # if(cc_benificiary["patient_code"]==None):
                    cc_benificiary["city_code"] = group["office"]
                    cc_benificiary["hospital_code"] = "MUSO"
                    cc_benificiary["patient_number"]= "{:05d}".format(int(group["code"]))+"{:03d}".format(cc_benificiary["rank"])
                    cc_benificiary["patient_code"] = cc_benificiary["city_code"]+"/"+cc_benificiary["hospital_code"]+"/"+cc_benificiary["patient_number"]
                    # else:
                    #     patient_codes = cc_benificiary["patient_code"].split("/")
                    #     cc_benificiary["city_code"] = patient_codes[0]
                    #     cc_benificiary["hospital_code"] = patient_codes[1]
                    #     cc_benificiary["patient_number"] = patient_codes[2]
                    beneficiaries.append(cc_benificiary)
                    i+=1
        return beneficiaries

        def update_muso_groupmembers_non_applicable(self):
            df_hiv_ben = pd.DataFrame(self.hiv_beneficiaries)
            df_cc_ben = pd.DataFrame(self.cc_beneficiaries)



    def update_beneficiaries_status(self):
        """
            Update beneficiaries status : is_inactive , graduated , is_abandonned
        """
        muso_beneficiary = MusoBeneficiary()
        df = pd.DataFrame(self.cc_beneficiaries)
        df[["graduated","is_inactive"]]=df[["graduated","is_inactive"]].fillna(0)
        df[["inactive_date","graduation_date"]] = df[["inactive_date","graduation_date"]].fillna("")
        df["graduated"] = df["graduated"].astype(int)
        df["is_inactive"] = df["is_inactive"].astype(int)
        df["closed"]=df["closed"].astype(int)

        df = df[(df["closed"]==1) | (df["graduated"]==1) | (df["is_inactive"]==1) ]
        rows = df.to_dict("records")
        # __rows = list(filter(lambda r: (r["graduated"]==1 or r["is_inactive"]==1 or r["closed"]==1) , rows))
        return muso_beneficiary.update_benficiaries_status(rows)


    def update_beneficiaries_household_not_applicable(self):
        """
            Update beneficiaries status : is_inactive , graduated , is_abandonned
        """
        muso_beneficiary = MusoBeneficiary()
        df = pd.DataFrame(self.cc_beneficiaries)
        # df["household_number_2022"]=df["household_number_2022"].astype(int)
        df_house  = df[df["household_number_2022"]=="0"]
        df_house["is_household_applicable"]="yes"
        print(df_house)

        rows = df_house.to_dict("records")
        return muso_beneficiary.update_beneficiairies_household_not_applicable(rows)