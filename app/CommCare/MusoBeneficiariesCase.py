from core.CommCareAPI import CommCareAPI
class MusoBeneficiariesCase:
    def __init__(self) -> None:
        pass

    def get(self):
        cc = CommCareAPI("caris-test", "0.5")
        cases = cc.get_cases("muso_beneficiaries", 5000)
        properties = []
        for case in cases:
            if "parent" in case["indices"]:
                properties.append({**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"],"parent_id":case["indices"]["parent"]["case_id"] })
            else:
                properties.append({**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"] })
        return properties