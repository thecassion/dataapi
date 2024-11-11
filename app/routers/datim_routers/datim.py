from fastapi import (
    APIRouter,
    HTTPException
)

from ...db.pmtct import Pmtct
from ...db.education import Education
from ...db.ovc_muso import OvcMuso
from ...db.datim_schooling import DatimSchooling
from ...db.datim_pmtct import DatimPmtct
from starlette.responses import StreamingResponse
import io
import pandas as pd
import datetime

router = APIRouter(
    prefix="/datim",
    tags=["DATIM"]
)


@router.get("/ovcpreventive/education")
def education_by_commune(start_date: str = None, end_date: str = None):
    return Education().get_education_by_commune(start_date, end_date)

@router.get("/ovcpreventive/education/xlsx")
def education_by_commune(start_date: str = None, end_date: str = None):
    df = Education().get_education_by_commune(start_date, end_date)
    df = pd.DataFrame(df)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, sheet_name="Education")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y%m%d")
    headers = {
        'Content-Disposition': f'attachment; filename=education_{date}.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)



@router.get("/ovcpreventive/muso/ben_direct")
def muso_ben_direct(start_date: str = None, end_date: str = None):
    return OvcMuso().get_direct_ben_by_commune(start_date, end_date)

# export to excel
@router.get("/ovcpreventive/muso/ben_direct/xlsx")
def muso_ben_direct_xlsx(start_date: str = None, end_date: str = None):
    df = OvcMuso().get_direct_ben_by_commune(start_date, end_date)
    df = pd.DataFrame(df)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, sheet_name="Muso_Ben_Direct")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y%m%d")
    headers = {
        'Content-Disposition': f'attachment; filename=muso_ben_direct_{date}.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)

# ovc preventive muso ben indirect by commune
@router.get("/ovcpreventive/muso/ben_indirect")
def muso_ben_indirect(start_date: str = None, end_date: str = None):
    return OvcMuso().get_indirect_ben_by_commune(start_date, end_date)

# export to excel
@router.get("/ovcpreventive/muso/ben/xlsx")
def muso_ben_indirect_xlsx(start_date: str = None, end_date: str = None):
    df = OvcMuso().get_indirect_ben_by_commune(start_date, end_date)
    df = pd.DataFrame(df)

    df_direct = OvcMuso().get_direct_ben_by_commune(start_date, end_date)
    df_direct = pd.DataFrame(df_direct)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df_direct.to_excel(writer, sheet_name="Muso_Ben_Direct")
        df.to_excel(writer, sheet_name="Muso_Ben_Indirect")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y%m%d")
    headers = {
        'Content-Disposition': f'attachment; filename=muso_ben_indirect_{date}.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)


# schoolling
@router.get("/ovcpreventive/schooling")
def schooling_by_commune(start_date: str = None, end_date: str = None):
    return DatimSchooling().get_direct_ben_by_commune(start_date, end_date)

@router.get("/ovcpreventive/schooling/xlsx")
def schooling_by_commune_xlsx(start_date: str = None, end_date: str = None):
    df = DatimSchooling().get_direct_ben_by_commune(start_date, end_date)
    df = pd.DataFrame(df)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, sheet_name="Schooling")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y%m%d")
    headers = {
        'Content-Disposition': f'attachment; filename=schooling_{date}.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)


# pmtct
@router.get("/ovccomprehensive/pmtct")
def pmtct_by_commune(q1_start_date: str = None, q1_end_date: str = None, q2_start_date:str =None, q2_end_date:str = None):
    return DatimPmtct().get_direct_ben_by_commune(q1_start_date, q1_end_date, q2_start_date, q2_end_date)

# pmtct to excel
@router.get("/ovccomprehensive/pmtct/xlsx")
def pmtct_by_commune_xlsx(q1_start_date: str = None, q1_end_date: str = None, q2_start_date:str =None, q2_end_date:str = None):
    df = DatimPmtct().get_direct_ben_by_commune(q1_start_date, q1_end_date, q2_start_date, q2_end_date)
    df = pd.DataFrame(df)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, sheet_name="PMTCT")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y%m%d")
    headers = {
        'Content-Disposition': f'attachment; filename=pmtct_{date}.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)

# pmtct indirect
@router.get("/ovccomprehensive/pmtct/indirect")
def pmtct_indirect_by_commune(q1_start_date: str = None, q1_end_date: str = None, q2_start_date:str =None, q2_end_date:str = None):
    return DatimPmtct().get_indirect_ben_by_commune(q1_start_date, q1_end_date, q2_start_date, q2_end_date)

# pmtct indirect to excel
@router.get("/ovccomprehensive/pmtct/indirect/xlsx")
def pmtct_indirect_by_commune_xlsx(q1_start_date: str = None, q1_end_date: str = None, q2_start_date:str =None, q2_end_date:str = None):
    df = DatimPmtct().get_indirect_ben_by_commune(q1_start_date, q1_end_date, q2_start_date, q2_end_date)
    df = pd.DataFrame(df)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, sheet_name="PMTCT_Indirect")
        writer.close()
    buffer.seek(0)
    date = datetime.datetime.now().strftime("%Y%m%d")
    headers = {
        'Content-Disposition': f'attachment; filename=pmtct_indirect_{date}.xlsx'
    }
    return StreamingResponse(buffer, headers=headers)

