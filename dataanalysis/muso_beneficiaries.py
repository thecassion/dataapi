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