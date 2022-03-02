from typing import Optional

from fastapi import FastAPI
from core.CommCareAPI import CommCareAPI
import  pandas as pd
import io
from starlette.responses import StreamingResponse

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