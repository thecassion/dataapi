from ..core import CommCareAPI

class GardenRegistrationForm:
    def __init__(self)->None:
        pass

    def get(self):
        cc = CommCareAPI("caris-test", "0.5")
        forms = cc.get_forms("http://openrosa.org/formdesigner/E280D1FC-3D0D-4138-B174-33D88DA39AAD", 1000)
        properties = [{**case["form"],"case_id":case["form"]["case"]["@case_id"], **case["form"]["meta"] } for case in forms]
        return properties
