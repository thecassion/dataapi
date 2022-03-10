import re
from typing import Optional

from fastapi import FastAPI
from core.CommCareAPI import CommCareAPI
import  pandas as pd
import io
from starlette.responses import StreamingResponse
from dataanalysis.muso_groupes import MusoGroupes
from db.muso_group import MusoGroup
from db.muso_beneficiary import MusoBeneficiary
from CommCare.MusoGroupesCase import MusoGroupesCase
from CommCare.MusoBeneficiariesCase import MusoBeneficiariesCase
from dataanalysis.muso_beneficiaries import MusoBeneficiaries

app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "/docs"}


@app.get("/cases/{type}/xlsx")
def read_cases_to_excel(type:str):
    cc = CommCareAPI("caris-test", "0.5")
    cases = cc.get_cases(type, 5000)
    properties = [{**case["properties"],"case_id":case["case_id"],"user_id":case["user_id"] }for case in cases]
    if len(properties)>0:
        df = pd.DataFrame(properties)

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer) as writer:
            df.to_excel(writer, sheet_name="cases", index=False)
            writer.save()
        buffer.seek(0)
        headers = {"Content-Disposition": "attachment; filename="+type+"_cases.xlsx"}
        return StreamingResponse(buffer, headers=headers)
    else:
        return {"message":"No cases found"}

@app.get("/muso/groups/xlsx")
def read_muso_groups_to_excel():
    hiv_groups = MusoGroup().get_muso_groups()
    # df_hiv_groups = MusoGroup().get_muso_groupes_df()
    # hiv_groups_list = [dict(zip(group)) for group in hiv_groups]
    cc_groups = MusoGroupesCase().get()
    muso_groupes = MusoGroupes(cc_groups, hiv_groups)
    groups_not_on_hiv_list = muso_groupes.groups_not_on_hiv()
    df_hiv_groups = pd.DataFrame(hiv_groups)
    df_cc_groups = pd.DataFrame(cc_groups)
    print(df_cc_groups.dtypes)
    # df_groups_not_on_hiv = pd.concat([df_hiv_groups,df_cc_groups])
    df_groups_not_on_hiv = pd.DataFrame(groups_not_on_hiv_list)
    df_groups_duplicated_on_cc = muso_groupes.groups_duplicated_on_cc_df()
    df_max_group_code_by_office_df = muso_groupes.max_group_code_by_office_df()
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df_hiv_groups.to_excel(writer, sheet_name="hiv_groups", index=False)
        df_cc_groups.to_excel(writer, sheet_name="cc_groups", index=False)
        df_groups_not_on_hiv.to_excel(writer, sheet_name="muso_groups_not_on_hiv", index=False)
        df_groups_duplicated_on_cc.to_excel(writer, sheet_name="muso_groups_duplicated_on_cc", index=False)
        df_max_group_code_by_office_df.to_excel(writer, sheet_name="max_group_code_by_office", index=False)
        writer.save()
    buffer.seek(0)
    headers = {"Content-Disposition": "attachment; filename=muso_groups.xlsx"}
    return StreamingResponse(buffer, headers=headers)

@app.get("/muso/groups/sync")
def sync_muso_groups():
    hiv_groups = MusoGroup().get_muso_groups()
    cc_groups = MusoGroupesCase().get()
    muso_groupes = MusoGroupes(cc_groups, hiv_groups)
    muso_groupes.insert_groupes_without_code_cc()
    muso_groupes.insert_groupes_duplicated_on_cc()
    muso_groupes.insert_groupes_not_on_hiv()
    return {"message":"muso groups synced"}

@app.get("/muso/groups/hiv_case_id")
def get_hiv_case_id():
    hiv_groups = MusoGroup().get_muso_groups()
    cc_groups = MusoGroupesCase().get()
    muso_groupes = MusoGroupes(cc_groups, hiv_groups)
    muso_groupes.update_groupes_case_id()

@app.get("/muso/groups/sync/code")
def sync_muso_groups_code():
    hiv_groups = MusoGroup().get_muso_groups()
    cc_groups = MusoGroupesCase().get()
    muso_groupes = MusoGroupes(cc_groups, hiv_groups)
    groupes = muso_groupes.generate_code_for_group_without_code()
    response = update_code_on_cc(groupes)
    print(response.text)
    groupes_dup_with_code = muso_groupes.generate_code_for_groupes_duplicated_on_cc()
    response_dup = update_code_on_cc(groupes_dup_with_code)
    print(response_dup.text)
    return {"message":"muso groups synced"}

def update_code_on_cc(groupes):
    groupes_with_new_code = [{"case_id":group["case_id"],"code":group["code"]} for group in groupes]
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        pd.DataFrame(groupes_with_new_code).to_excel(writer, sheet_name="groupes_with_new_code",index=False)
        writer.save()
    buffer.seek(0)
    data = {
        "file": ("muso_group_to_update_code_with_api.xlsx",buffer),
        "case_type": "muso_groupes",
    }
    cc = CommCareAPI("caris-test", "0.5")
    response = cc.bulkupload(data)
    return response

@app.get("/muso/beneficiaries/xlsx")
def beneficiaries_to_excel():
    hiv_beneficiaries = MusoBeneficiary().get_muso_beneficiaries()
    cc_beneficiaries = MusoBeneficiariesCase().get()
    analysis_muso_beneficiaries = MusoBeneficiaries({"cc_beneficiaries":cc_beneficiaries, "hiv_beneficiaries":hiv_beneficiaries})
    cc_beneficiaries_with_external_id = analysis_muso_beneficiaries.get_cc_beneficiairies_with_external_id()
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        pd.DataFrame(hiv_beneficiaries).to_excel(writer, sheet_name="hiv_beneficiaries", index=False)
        pd.DataFrame(cc_beneficiaries).to_excel(writer, sheet_name="beneficiaries_cc", index=False)
        pd.DataFrame(cc_beneficiaries_with_external_id).to_excel(writer, sheet_name="beneficiaries_cc_with_external_id", index=False)
        writer.save()
    buffer.seek(0)
    headers = {"Content-Disposition": "attachment; filename=beneficiaries.xlsx"}
    return StreamingResponse(buffer, headers=headers)

@app.get("/muso/beneficiaries/sync_to_hivhaiti/case_id")
def sync_beneficiaries_case_id():
    hiv_beneficiaries = MusoBeneficiary().get_muso_beneficiaries()
    cc_beneficiaries = MusoBeneficiariesCase().get()
    analysis_muso_beneficiaries = MusoBeneficiaries({"cc_beneficiaries":cc_beneficiaries, "hiv_beneficiaries":hiv_beneficiaries})
    analysis_muso_beneficiaries.update_beneficiaries_case_id()
    return {"message":"beneficiaries synced"}