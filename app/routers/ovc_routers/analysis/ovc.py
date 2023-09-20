import pandas as pd
from ....models.report import OVCReportParameters
from ..db.ptme_ovc import PtmeOvc
from ..db.dreams import Dreams
from ..db.muso import Muso
from ..db.gardening import Gardening

class OVC:
    def __init__(self, OVCReportParameters: OVCReportParameters):
        self.OVCReportParameters = OVCReportParameters
        pass

    
    def get_ovc_serv_semester(self):
        ovc = PtmeOvc().get_ovc_serv_semester(self.OVCReportParameters.quarters.report_year_1, self.OVCReportParameters.quarters.report_quarter_1, self.OVCReportParameters.quarters.report_year_2, self.OVCReportParameters.quarters.report_quarter_2, self.OVCReportParameters.type_of_aggregation)
        dreams = Dreams().get_ovc_dreams_by_period(self.OVCReportParameters.period_1.start_date, self.OVCReportParameters.period_2.end_date, self.OVCReportParameters.type_of_aggregation)
        muso = Muso().get_ovc_muso_without_caris_member(self.OVCReportParameters.quarters.report_year_1, self.OVCReportParameters.quarters.report_quarter_1, self.OVCReportParameters.type_of_aggregation)
        gardening = Gardening().get_ovc_gardening_by_period(
            self.OVCReportParameters.period_1.start_date,
            self.OVCReportParameters.period_1.end_date,
            self.OVCReportParameters.period_2.start_date,
            self.OVCReportParameters.period_2.end_date,
            self.OVCReportParameters.type_of_aggregation)
        df_ovc = pd.DataFrame(ovc)
        df_ovc = df_ovc.fillna(0)
        df_dreams = pd.DataFrame(dreams)
        df_dreams = df_dreams.fillna(0)
        df_muso = pd.DataFrame(muso)
        df_muso = df_muso.fillna(0)
        df_gardening = pd.DataFrame(gardening)
        df_gardening = df_gardening.fillna(0)
        # Create a new data frame from MUSO by adding h_ to each column in MUSO without household
        df_muso_with_household = pd.DataFrame()
        columns = df_muso.columns
        # Remove household columns (columns starting by h_)

        columns = [column for column in columns if not column.startswith("h_")]

        columns_starting_by_h = [column for column in df_muso.columns if column.startswith("h_")]

        print("Columns start by _h", columns_starting_by_h)
        print("Columns", columns)

        for column in columns:
            df_muso_with_household[column] = df_muso[column]

        for column in columns_starting_by_h:
            df_muso_with_household[column.removeprefix("h_")] = df_muso_with_household[column.removeprefix("h_")]+ df_muso[column]
        
        print("Columns", df_muso_with_household.columns)
        print("Head", df_muso_with_household.head())
        df_muso_with_household = df_muso_with_household.fillna(0)


        df = pd.concat([df_ovc, df_dreams, df_muso_with_household,df_gardening])
        df = df.fillna(0)
        # lowercase departement and commune
        df["departement"] = df["departement"].str.lower()
        df["commune"] = df["commune"].str.lower()
        # remove accent
        df["departement"] = df["departement"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        df["commune"] = df["commune"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

        df=df.groupby(["departement", "commune"]).sum().reset_index()
        df = df.to_dict(orient="records")
        return df