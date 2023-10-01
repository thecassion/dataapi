import pandas as pd
from ....models.report import OVCReportParameters
from ..db.ptme_ovc import PtmeOvc
from ..db.dreams import Dreams
from ..db.muso import Muso
from ..db.gardening import Gardening
from ..db.education import Education

class OVC:
    def __init__(self, OVCReportParameters: OVCReportParameters):
        self.OVCReportParameters = OVCReportParameters
        pass

    
    def get_ovc_serv_semester(self):
        ovc = PtmeOvc().get_ovc_serv_semester(self.OVCReportParameters.quarters.report_year_1, self.OVCReportParameters.quarters.report_quarter_1, self.OVCReportParameters.quarters.report_year_2, self.OVCReportParameters.quarters.report_quarter_2, self.OVCReportParameters.type_of_aggregation)
        dreams = Dreams().get_ovc_dreams_by_period(self.OVCReportParameters.fiscal_year_range.start_date, self.OVCReportParameters.fiscal_year_range.end_date, self.OVCReportParameters.type_of_aggregation)
        muso = Muso().get_ovc_muso_without_caris_member(self.OVCReportParameters.quarters.report_year_1, self.OVCReportParameters.quarters.report_quarter_1, self.OVCReportParameters.type_of_aggregation)
        gardening = Gardening().get_ovc_gardening_by_period(
            self.OVCReportParameters.period_1.start_date,
            self.OVCReportParameters.period_1.end_date,
            self.OVCReportParameters.period_2.start_date,
            self.OVCReportParameters.period_2.end_date,
            self.OVCReportParameters.type_of_aggregation)
        education = Education().get_ovc_education_by_period(self.OVCReportParameters.fiscal_year_range.start_date, self.OVCReportParameters.fiscal_year_range.end_date)
        df_ovc = pd.DataFrame(ovc)
        df_ovc = df_ovc.fillna(0)
        df_dreams = pd.DataFrame(dreams)
        df_dreams = df_dreams.fillna(0)
        df_muso = pd.DataFrame(muso)
        df_muso = df_muso.fillna(0)
        df_gardening = pd.DataFrame(gardening)
        df_gardening = df_gardening.fillna(0)
        df_education = pd.DataFrame(education)
        df_education = df_education.fillna(0)

        df_muso_with_household= self._create_df_with_household(df_muso)
        df_gardening= self._create_df_with_household(df_gardening)


        df = pd.concat([df_ovc, df_dreams, df_muso_with_household,df_gardening, df_education])

        # weight per program
        
        # all per gender
        programs = [
            {
                "program":"ovc",
                "male":df_ovc["male"].sum(),
                "female":df_ovc["female"].sum(),
            },
            {
                "program":"dreams",
                "male":0,
                "female":df_dreams["female"].sum(),
            },
            {
                "program":"muso",
                "male":df_muso_with_household["male"].sum(),
                "female": df_muso_with_household["female"].sum(),
            },
            {
                "program":"gardening",
                "male":0,
                "female":df_gardening["female"].sum(),
            },
            {
                "program":"education",
                "male":df_education["male"].sum(),
                "female": df_education["female"].sum()
            }
        ]
        df_programs = pd.DataFrame(programs)
        df_programs = df_programs.fillna(0)
        programs = df_programs.to_dict(orient="records")
        df = df.fillna(0)
        # lowercase departement and commune
        df["departement"] = df["departement"].str.lower()
        df["commune"] = df["commune"].str.lower()
        # remove accent
        df["departement"] = df["departement"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        df["commune"] = df["commune"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

        df=df.groupby(["departement", "commune"]).sum().reset_index()
        df = df.to_dict(orient="records")
        return {
            "communes":df,
            "programs":programs
        }
    
        # Refactored method to create df with 'h_' columns added to corresponding columns
    def _create_df_with_household(self, df):
        df_with_household = df.copy()

        # Identify columns starting with 'h_'
        household_columns = [column for column in df.columns if column.startswith("h_")]

        # Remove the 'h_' prefix and add to the corresponding columns
        for column in household_columns:
            non_household_column = column.removeprefix("h_")
            df_with_household[non_household_column] = df_with_household.get(non_household_column, 0) + df_with_household[column]

        # Drop the original 'household' columns
        df_with_household = df_with_household.drop(household_columns, axis=1)

        # Fill NaN values with 0
        df_with_household = df_with_household.fillna(0)
        df_with_household = df_with_household.drop(columns=household_columns)

        return df_with_household