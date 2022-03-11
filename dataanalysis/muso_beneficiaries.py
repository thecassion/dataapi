import pandas as pd

from db.muso_beneficiary import MusoBeneficiary

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
        df = df[df["patient_code"].isna()]
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
                    if "pvih" in cc_benificiary:
                        if cc_benificiary["pvih"] !=None:
                            cc_benificiary["pvvih"] = cc_benificiary["pvih"]
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