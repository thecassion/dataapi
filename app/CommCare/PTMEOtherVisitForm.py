from ..core import CommCareAPI

class PTMEOtherVisitForm:
    def __init__(self):
        pass

    def get(self):
        cc = CommCareAPI("caris-test", "0.5")
        forms = cc.get_forms("muso_groupes", 1000)
        properties = [{**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"] }for case in cases]
        return properties
