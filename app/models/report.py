from pydantic import BaseModel, Field
from datetime import date
from enum import Enum

class DateRange(BaseModel):
    start_date: date
    end_date: date


class TypeOfReport(str, Enum):
    ovc = "ovc"
    program = "program"


class DreamsReportParameters(BaseModel):
    period_1: DateRange
    period_2: DateRange
    type_of_aggregation: str = None
    type_of_report: TypeOfReport = TypeOfReport.ovc


class MusoReportParameters(BaseModel):
    type_of_aggregation: str = "commune"
    report_year_1: int = 2022
    report_quarter_1: int = 1

class OVCQuarters(BaseModel):
    report_year_1: int = 2022
    report_quarter_1: int = 4
    report_year_2: int = 2023
    report_quarter_2: int = 1

class OVCReportParameters(BaseModel):
    period_1: DateRange = None
    period_2: DateRange = None
    quarters: OVCQuarters = None
    type_of_aggregation: str = "commune"
    type_of_report: TypeOfReport = TypeOfReport.ovc

class FiscalYearSemester(BaseModel):
    fiscal_year: str
    semester: int
    # Convert FiscalYearSemester to OVCReportParameters
    def to_ovc_quarters(self):
        return OVCQuarters(
            report_year_1 = int(self.fiscal_year.split("-")[0]),
            report_quarter_1 = 4 if self.semester ==1 else 2,
            report_year_2 = int(self.fiscal_year.split("-")[1]),
            report_quarter_2 = 1 if self.semester ==1 else 3
        )
    
    def to_ovc_report_parameters(self):
        ovc_quarters = self.to_ovc_quarters()
        return OVCReportParameters(
            # report_year_1 and quarter_1 to period_1
            period_1 = DateRange(
                start_date = date(ovc_quarters.report_year_1, (ovc_quarters.report_quarter_1-1)*3+1, 1),
                end_date = date(ovc_quarters.report_year_1, ovc_quarters.report_quarter_1*3, 31 if self.semester ==1 else 30)
            ),
            # report_year_2 and quarter_2 to period_2
            period_2 = DateRange(
                start_date = date(ovc_quarters.report_year_2, (ovc_quarters.report_quarter_2-1)*3+1, 1),
                end_date = date(ovc_quarters.report_year_2, ovc_quarters.report_quarter_2*3, 31 if self.semester ==1 else 30)
            ),
            quarters = self.to_ovc_quarters()
        )
