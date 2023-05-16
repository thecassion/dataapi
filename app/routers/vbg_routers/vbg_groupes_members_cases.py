from ...core import CommCareAPI
class VBGGroupesMembersCase:
    def __init__(self) -> None:
        pass

    def get(self):
        cc = CommCareAPI("caris-test", "0.5")
        parameters = {"type": "vbg_groupe_members", "limit": 5000}
        cases = cc.get_cases_by_parameters(parameters)
        properties = [{**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"],"closed":case["closed"] }for case in cases]
        return cases