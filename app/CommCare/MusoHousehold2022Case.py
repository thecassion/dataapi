from ..core import CommCareAPI
class MusoHousehold2022Case:
    def __init__(self) -> None:
        pass

    def get(self):
        cc = CommCareAPI("caris-test", "0.5")
        cases = cc.get_cases("muso_household_2022", 5000)
        properties = []
        for case in cases:
            if "parent" in case["indices"]:
                properties.append({**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"],"parent_id":case["indices"]["parent"]["case_id"] })
            else:
                properties.append({**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"] })
        return properties
