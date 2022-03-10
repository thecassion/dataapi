from core.CommCareAPI import CommCareAPI
class MusoBeneficiariesCase:
    def __init__(self) -> None:
        pass

    def get(self):
        cc = CommCareAPI("caris-test", "0.5")
        cases = cc.get_cases("muso_beneficiaries", 5000)
        properties = [{**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"] }for case in cases]
        return properties