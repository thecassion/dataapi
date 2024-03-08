from pandas import read_sql_query, to_datetime
from sqlalchemy import text
from .db import engine
from .query import SDATA
from .model import *


def query_data():
    sdata = read_sql_query(text(SDATA), engine.connect(), parse_dates=True)
    engine.dispose()
    return sdata


def data_processing():
    sdata = query_data()
    # sdata.replace(r'^\s*$', nan, regex=True, inplace=True)
    sdata.code = sdata.code.fillna('---')
    sdata.interview_date = sdata.loc[:, 'interview_date'].apply(to_datetime)

    eligible_or_not = sdata
    eligible = sdata[
        (sdata.total >= 14) &
        (sdata.age_range != "not_valid_age") &
        (sdata.age_range != "25-29")
    ]
    to_be_served = sdata[
        (sdata.code == "---") &
        (sdata.total >= 14) &
        (sdata.age_range != "not_valid_age") &
        (sdata.age_range != "25-29")
    ]
    served = sdata[
        (sdata.code != "---") &
        (sdata.total >= 14) &
        (sdata.age_range != "not_valid_age") &
        (sdata.age_range != "25-29")
    ]

    thereturns = dict(
        eligibleornot=eligible_or_not,
        eligible=eligible,
        tobeserved=to_be_served,
        served=served
    )

    return thereturns


class EnrolementAnalysis():
    screened = data_processing()['eligibleornot']
    eligible = data_processing()['eligible']
    to_be_served = data_processing()['tobeserved']
    served = data_processing()['served']

    @staticmethod
    def to_be_served_per_trimester():
        unserved_Q2FY24 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2024-01-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2024-03-31")]
        unserved_Q1FY24 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2023-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2023-12-31")]
        unserved_Q1FY23 = EnrolementAnalysis.to_be_served[(EnrolementAnalysis.to_be_served.interview_date >=
                                                           "2022-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2022-12-31")]
        unserved_Q2FY23 = EnrolementAnalysis.to_be_served[(EnrolementAnalysis.to_be_served.interview_date >=
                                                           "2023-01-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2023-03-31")]
        unserved_Q3FY23 = EnrolementAnalysis.to_be_served[(EnrolementAnalysis.to_be_served.interview_date >=
                                                           "2023-04-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2023-06-30")]
        unserved_Q4FY23 = EnrolementAnalysis.to_be_served[(EnrolementAnalysis.to_be_served.interview_date >=
                                                           "2023-07-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2023-09-30")]
        return ServedPerTrimester(
            title="Présentation du nombre de bénéficiares à servir par trimestre",
            values=[
                ValuePerTrimester(
                    semester_fiscal_year="Q2FY24",
                    data=unserved_Q2FY24.shape[0],
                    notation="Unserved_Q2FY24"
                ),
                ValuePerTrimester(
                    semester_fiscal_year="Q1FY24",
                    data=unserved_Q1FY24.shape[0],
                    notation="Unserved_Q1FY24"
                ),
                ValuePerTrimester(
                    semester_fiscal_year="Q4FY23",
                    data=unserved_Q4FY23.shape[0],
                    notation="Unserved_Q4FY23"
                ),
                ValuePerTrimester(
                    semester_fiscal_year="Q3FY23",
                    data=unserved_Q3FY23.shape[0],
                    notation="Unserved_Q3FY23"
                ),
                ValuePerTrimester(
                    semester_fiscal_year="Q2FY23",
                    data=unserved_Q2FY23.shape[0],
                    notation="Unserved_Q2FY23"
                ),
                ValuePerTrimester(
                    semester_fiscal_year="Q1FY23",
                    data=unserved_Q1FY23.shape[0],
                    notation="Unserved_Q1FY23"
                )
            ]
        )

    @staticmethod
    def eligible_to_be_served():
        to_be_served_FY19 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2018-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2019-09-30")]
        to_be_served_FY20 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2019-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2020-09-30")]
        to_be_served_FY21 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2020-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2021-09-30")]
        to_be_served_FY22 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2021-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2022-09-30")]
        to_be_served_FY23 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2022-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2023-09-30")]
        to_be_served_FY24 = EnrolementAnalysis.to_be_served[(
            EnrolementAnalysis.to_be_served.interview_date >= "2023-10-01") & (EnrolementAnalysis.to_be_served.interview_date <= "2024-09-30")]

        eligible_FY19 = EnrolementAnalysis.eligible[(
            EnrolementAnalysis.eligible.interview_date >= "2018-10-01") & (EnrolementAnalysis.eligible.interview_date <= "2019-09-30")]
        eligible_FY20 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2019-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2020-09-30")]
        eligible_FY21 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2020-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2021-09-30")]
        eligible_FY22 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2021-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2022-09-30")]
        eligible_FY23 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2022-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2023-09-30")]
        eligible_FY24 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2023-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2024-09-30")]
        return EligibleVsToBeServed(
            title="Présentation du nombre de bénéficiares éligibles et à servir",
            values=[
                ValueEligibleToBeServed(
                    fiscal_year="FY19",
                    to_be_served=to_be_served_FY19.shape[0],
                    eligible=eligible_FY19.shape[0],
                    percentage_agyw_to_be_served=(
                        to_be_served_FY19.shape[0]/eligible_FY19.shape[0])*100
                ),
                ValueEligibleToBeServed(
                    fiscal_year="FY20",
                    to_be_served=to_be_served_FY20.shape[0],
                    eligible=eligible_FY20.shape[0],
                    percentage_agyw_to_be_served=(
                        to_be_served_FY20.shape[0]/eligible_FY20.shape[0])*100
                ),
                ValueEligibleToBeServed(
                    fiscal_year="FY21",
                    to_be_served=to_be_served_FY21.shape[0],
                    eligible=eligible_FY21.shape[0],
                    percentage_agyw_to_be_served=(
                        to_be_served_FY21.shape[0]/eligible_FY21.shape[0])*100
                ),
                ValueEligibleToBeServed(
                    fiscal_year="FY22",
                    to_be_served=to_be_served_FY22.shape[0],
                    eligible=eligible_FY22.shape[0],
                    percentage_agyw_to_be_served=(
                        to_be_served_FY22.shape[0]/eligible_FY22.shape[0])*100
                ),
                ValueEligibleToBeServed(
                    fiscal_year="FY23",
                    to_be_served=to_be_served_FY23.shape[0],
                    eligible=eligible_FY23.shape[0],
                    percentage_agyw_to_be_served=(
                        to_be_served_FY23.shape[0]/eligible_FY23.shape[0])*100
                ),
                ValueEligibleToBeServed(
                    fiscal_year="FY24",
                    to_be_served=to_be_served_FY24.shape[0],
                    eligible=eligible_FY24.shape[0],
                    percentage_agyw_to_be_served=(
                        to_be_served_FY24.shape[0]/eligible_FY24.shape[0])*100
                )
            ]
        )

    @staticmethod
    def screened_versus_eligible():
        screened_FY19 = EnrolementAnalysis.screened[(
            EnrolementAnalysis.screened.interview_date >= "2018-10-01") & (EnrolementAnalysis.screened.interview_date <= "2019-09-30")]
        screened_FY20 = EnrolementAnalysis.screened[(
            EnrolementAnalysis.screened.interview_date >= "2019-10-01") & (EnrolementAnalysis.screened.interview_date <= "2020-09-30")]
        screened_FY21 = EnrolementAnalysis.screened[(
            EnrolementAnalysis.screened.interview_date >= "2020-10-01") & (EnrolementAnalysis.screened.interview_date <= "2021-09-30")]
        screened_FY22 = EnrolementAnalysis.screened[(
            EnrolementAnalysis.screened.interview_date >= "2021-10-01") & (EnrolementAnalysis.screened.interview_date <= "2022-09-30")]
        screened_FY23 = EnrolementAnalysis.screened[(
            EnrolementAnalysis.screened.interview_date >= "2022-10-01") & (EnrolementAnalysis.screened.interview_date <= "2023-09-30")]
        screened_FY24 = EnrolementAnalysis.screened[(
            EnrolementAnalysis.screened.interview_date >= "2023-10-01") & (EnrolementAnalysis.screened.interview_date <= "2024-09-30")]

        eligible_FY19 = EnrolementAnalysis.eligible[(
            EnrolementAnalysis.eligible.interview_date >= "2018-10-01") & (EnrolementAnalysis.eligible.interview_date <= "2019-09-30")]
        eligible_FY20 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2019-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2020-09-30")]
        eligible_FY21 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2020-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2021-09-30")]
        eligible_FY22 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2021-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2022-09-30")]
        eligible_FY23 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2022-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2023-09-30")]
        eligible_FY24 = EnrolementAnalysis.eligible[(EnrolementAnalysis.eligible.interview_date >= "2023-10-01")
                                                    & (EnrolementAnalysis.eligible.interview_date <= "2024-09-30")]
        return ScreenedVsEligible(
            title=" Présentation du nombre de bénéficiares screenées et enrôlées",
            values=[
                ValueScreenedEligible(
                    fiscal_year="FY19",
                    screened=screened_FY19.shape[0],
                    eligible=eligible_FY19.shape[0],
                    percentage_agyw_screened=(
                        eligible_FY19.shape[0]/screened_FY19.shape[0])*100
                ),
                ValueScreenedEligible(
                    fiscal_year="FY20",
                    screened=screened_FY20.shape[0],
                    eligible=eligible_FY20.shape[0],
                    percentage_agyw_enrolled=(
                        eligible_FY20.shape[0]/screened_FY20.shape[0])*100
                ),
                ValueScreenedEligible(
                    fiscal_year="FY21",
                    screened=screened_FY21.shape[0],
                    eligible=eligible_FY21.shape[0],
                    percentage_agyw_enrolled=(
                        eligible_FY21.shape[0]/screened_FY21.shape[0])*100
                ),
                ValueScreenedEligible(
                    fiscal_year="FY22",
                    screened=screened_FY22.shape[0],
                    eligible=eligible_FY22.shape[0],
                    percentage_agyw_enrolled=(
                        eligible_FY22.shape[0]/screened_FY22.shape[0])*100
                ),
                ValueScreenedEligible(
                    fiscal_year="FY23",
                    screened=screened_FY23.shape[0],
                    eligible=eligible_FY23.shape[0],
                    percentage_agyw_enrolled=(
                        eligible_FY23.shape[0]/screened_FY23.shape[0])*100
                ),
                ValueScreenedEligible(
                    fiscal_year="FY24",
                    screened=screened_FY24.shape[0],
                    eligible=eligible_FY24.shape[0],
                    percentage_agyw_enrolled=(
                        eligible_FY24.shape[0]/screened_FY24.shape[0])*100
                )
            ]
        )
