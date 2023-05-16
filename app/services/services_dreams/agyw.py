from pandas import DataFrame
import polars as pl


class AgywPrev:
    """A class with properties and methods given the results of the indicator AGYW_PREV DATIM"""
    __who_am_I = "DATIM"
    __datim1_title = "Number of active DREAMS participants that have fully completed the entire DREAMS primary package of services but have not received any services beyond the primary package as of the past 6 months at Q2 or the past 12 months at Q4."
    __datim2_title = "Number of active DREAMS participants that have fully completed the entire DREAMS primary package of services AND at least one additional secondary service as of the past 6 months at Q2 or the past 12 months at Q4."
    __datim3_title = "Number of active DREAMS participants that have fully completed at least one DREAMS service/intervention but NOT the full primary package of services/interventions as of the past 6 months at Q2 or the past 12 months at Q4."
    __datim4_title = "Number of active DREAMS participants that have started a DREAMS service but have not yet completed it in the past 6 months at Q2 or 12 months at Q4."

    def __repr__(self):
        return f"<AgywPrev {self.__i_am}>"

    def __str__(self):
        return f"<AgywPrev {self.__i_am}>"

    @classmethod
    def datim_titleI(cls):
        return cls.__datim1_title

    @classmethod
    def datim_titleII(cls):
        return cls.__datim2_title

    @classmethod
    def datim_titleIII(cls):
        return cls.__datim3_title

    @classmethod
    def datim_titleIV(cls):
        return cls.__datim4_title

    def __init__(self, commune=None, data=None):
        self.__commune = commune
        self.__i_am = f"{AgywPrev.__who_am_I}"
        self.__data = data
        self. __total_mastersheet = self.__data.shape[0]
        if self.__commune == None:
            self.__dreams_valid = self.__data.filter(
                (pl.col("age_range") != "not_valid_age") & (
                    pl.col("age_range") != "25-29")
            )
        else:
            self.__dreams_valid = self.__data.filter(
                (pl.col("age_range") != "not_valid_age") & (
                    pl.col("age_range") != "25-29") & (pl.col("commune") == f"{commune}")
            )
        self.__total_dreams_valid = self.__dreams_valid.shape[0]
        self.__dreams_valid = self.__dreams_valid.with_columns(
            pl.when(
                (pl.col("ps_1014") == "primary")
                & (pl.col("hts") == "no")
                & (pl.col("prep") == "no")
                & (pl.col("condom") == "no")
                & (pl.col("post_violence_care") == "no")
                & (pl.col("socioeco_app") == "no")
                & (pl.col("parenting") == "no")
                & (pl.col("contraceptive") == "no")
                & (pl.col("education") == "no")
            ).then("full_primary_only")
            .when(
                (pl.col("ps_1519") == "primary")
                & (pl.col("hts") == "no")
                & (pl.col("prep") == "no")
                & (pl.col("post_violence_care") == "no")
                & (pl.col("socioeco_app") == "no")
                & (pl.col("parenting") == "no")
                & (pl.col("contraceptive") == "no")
                & (pl.col("education") == "no")
            ).then("full_primary_only")
            .when(
                (pl.col("ps_2024") == "primary")
                & (pl.col("hts") == "no")
                & (pl.col("prep") == "no")
                & (pl.col("post_violence_care") == "no")
                & (pl.col("socioeco_app") == "no")
                & (pl.col("parenting") == "no")
                & (pl.col("contraceptive") == "no")
                & (pl.col("education") == "no")
            ).then("full_primary_only")
            .otherwise("invalid")
            .alias("primary_only")
        )
        self.__dreams_valid = self.__dreams_valid.with_columns(
            pl.when(
                (pl.col("ps_1014") == "primary")
                & (
                    (pl.col("hts") == "yes")
                    | (pl.col("prep") == "yes")
                    | (pl.col("condom") == "yes")
                    | (pl.col("post_violence_care") == "yes")
                    | (pl.col("socioeco_app") == "yes")
                    | (pl.col("parenting") == "yes")
                    | (pl.col("contraceptive") == "yes")
                    | (pl.col("education") == "yes")
                )
            ).then("full_primary_leastOneSecondary")
            .when(
                (pl.col("ps_1519") == "primary")
                & (
                    (pl.col("hts") == "yes")
                    | (pl.col("prep") == "yes")
                    | (pl.col("post_violence_care") == "yes")
                    | (pl.col("socioeco_app") == "yes")
                    | (pl.col("parenting") == "yes")
                    | (pl.col("contraceptive") == "yes")
                    | (pl.col("education") == "yes")
                )
            ).then("full_primary_leastOneSecondary")
            .when(
                (pl.col("ps_2024") == "primary")
                & (
                    (pl.col("hts") == "yes")
                    | (pl.col("prep") == "yes")
                    | (pl.col("post_violence_care") == "yes")
                    | (pl.col("socioeco_app") == "yes")
                    | (pl.col("parenting") == "yes")
                    | (pl.col("contraceptive") == "yes")
                    | (pl.col("education") == "yes")
                )
            ).then("full_primary_leastOneSecondary")
            .otherwise("invalid")
            .alias("primary_and_OneSecondary_services")
        )
        self.__dreams_valid = self.__dreams_valid.with_columns(
            pl.when(
                (pl.col("age_range") == "10-14")
                & (pl.col("primary_only") == "invalid")
                & (pl.col("primary_and_OneSecondary_services") == "invalid")
                & (
                    (pl.col("hts") == "yes")
                    | (pl.col("condom") == "yes")
                    | (pl.col("prep") == "yes")
                    | (pl.col("post_violence_care") == "yes")
                    | (pl.col("socioeco_app") == "yes")
                    | (pl.col("parenting") == "yes")
                    | (pl.col("contraceptive") == "yes")
                    | (pl.col("education") == "yes")
                )
            ).then("primary_part_services")
            .when(
                (pl.col("age_range") == "15-19")
                & (pl.col("primary_only") == "invalid")
                & (pl.col("primary_and_OneSecondary_services") == "invalid")
                & (
                    (pl.col("hts") == "yes")
                    | (pl.col("condom") == "yes")
                    | (pl.col("prep") == "yes")
                    | (pl.col("post_violence_care") == "yes")
                    | (pl.col("socioeco_app") == "yes")
                    | (pl.col("parenting") == "yes")
                    | (pl.col("contraceptive") == "yes")
                    | (pl.col("education") == "yes")
                )
            ).then("primary_part_services")
            .when(
                (pl.col("age_range") == "20-24")
                & (pl.col("primary_only") == "invalid")
                & (pl.col("primary_and_OneSecondary_services") == "invalid")
                & (
                    (pl.col("hts") == "yes")
                    | (pl.col("condom") == "yes")
                    | (pl.col("prep") == "yes")
                    | (pl.col("post_violence_care") == "yes")
                    | (pl.col("socioeco_app") == "yes")
                    | (pl.col("parenting") == "yes")
                    | (pl.col("contraceptive") == "yes")
                    | (pl.col("education") == "yes")
                )
            ).then("primary_part_services")
            .otherwise("invalid")
            .alias("completed_one_service")
        )
        self.__dreams_valid = self.__dreams_valid.with_columns(
            pl.when(
                (pl.col("age_range") == "10-14")
                & (pl.col("primary_only") == "invalid")
                & (pl.col("primary_and_OneSecondary_services") == "invalid")
                & (pl.col("completed_one_service") == "invalid")
            ).then("yes")
            .when(
                (pl.col("age_range") == "15-19")
                & (pl.col("primary_only") == "invalid")
                & (pl.col("primary_and_OneSecondary_services") == "invalid")
                & (pl.col("completed_one_service") == "invalid")
            ).then("yes")
            .when(
                (pl.col("age_range") == "20-24")
                & (pl.col("primary_only") == "invalid")
                & (pl.col("primary_and_OneSecondary_services") == "invalid")
                & (pl.col("completed_one_service") == "invalid")
            ).then("yes")
            .otherwise("no")
            .alias("has_started_one_service")
        )
        self.__agyw_prevI = self.__dreams_valid.filter(
            pl.col("primary_only") == "full_primary_only"
        )
        self.__agyw_prevII = self.__dreams_valid.filter(
            pl.col(
                "primary_and_OneSecondary_services") == "full_primary_leastOneSecondary"
        )
        self.__agyw_prevIII = self.__dreams_valid.filter(
            pl.col("completed_one_service") == "primary_part_services"
        )
        self.__agyw_prevIV = self.__dreams_valid.filter(
            pl.col("has_started_one_service") == "yes"
        )
        self.__agyw_prevI_total = self.__agyw_prevI.shape[0]
        self.__agyw_prevII_total = self.__agyw_prevII.shape[0]
        self.__agyw_prevIII_total = self.__agyw_prevIII.shape[0]
        self.__agyw_prevIV_total = self.__agyw_prevIV.shape[0]
        self.__total_datim = self.__agyw_prevI_total + self.__agyw_prevII_total + \
            self.__agyw_prevIII_total + self.__agyw_prevIV_total

    @property
    def who_am_i(self):
        return self.__i_am

    @property
    def data_mastersheet(self):
        return self.__data

    @property
    def data_dreams_valid(self):
        return self.__dreams_valid

    @property
    def total_mastersheet(self):
        return self.__total_mastersheet

    @property
    def total_dreams_valid(self):
        return self.__total_dreams_valid

    @property
    def total_datimI(self):
        return self.__agyw_prevI_total

    @property
    def total_datimII(self):
        return self.__agyw_prevII_total

    @property
    def total_datimIII(self):
        return self.__agyw_prevIII_total

    @property
    def total_datimIV(self):
        return self.__agyw_prevIV_total

    @property
    def total_datim_general(self):
        return self.__total_datim

    @property
    def data_agyw_prevI(self):
        return self.__agyw_prevI

    @property
    def data_agyw_prevII(self):
        return self.__agyw_prevII

    @property
    def data_agyw_prevIII(self):
        return self.__agyw_prevIII

    @property
    def data_agyw_prevIV(self):
        return self.__agyw_prevIV

    def datim_vital_info(self):
        dt = pl.DataFrame._from_dict(
            data={
                "Number of active DREAMS participants that received an evidence-based intervention focused on preventing violence within the reporting period.": [
                    self.__dreams_valid.filter(
                        pl.col("curriculum") == "yes").shape[0]
                ],
                "Numberof active DREAMS participants that received educational support to remain in, advance, and/or rematriculate in school within the reporting period.": [
                    self.__dreams_valid.filter(
                        pl.col("education") == "yes").shape[0]
                ],
                "Number of active DREAMS participants that completed a comprehensive economic strengthening intervention within the past 6 months at Q2 or past 12 months at Q4.": [
                    self.__dreams_valid.filter(
                        pl.col("socioeco_app") == "yes").shape[0]
                ]
            }
        ).to_pandas()
        dt = (
            dt
            .transpose()
            .reset_index()
            .rename_axis(None, axis=1)
        )
        dt.rename(columns=dt.iloc[0], inplace=True)
        dt.drop(dt.index[0], inplace=True)
        return dt

    def datim_ArPAP_vital_info(self):
        dt_ArPAP = pl.DataFrame._from_dict(
            data={
                "Number of active DREAMS participants that received an evidence-based intervention focused on preventing violence within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Port-au-Prince")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Delmas")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Pétionville")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Tabarre")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Gressier")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Kenscoff")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Carrefour")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that received educational support to remain in, advance, and/or rematriculate in school within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Port-au-Prince")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Delmas")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Pétionville")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Tabarre")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Gressier")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Kenscoff")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Carrefour")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that completed a comprehensive economic strengthening intervention within the past 6 months at Q2 or past 12 months at Q4.": [
                    (self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Port-au-Prince")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Delmas")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Pétionville")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Tabarre")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Gressier")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Kenscoff")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Carrefour")).shape[0]) if True else 0
                ]
            }
        ).to_pandas()
        dt_ArPAP = (
            dt_ArPAP
            .transpose()
            .reset_index()
            .rename_axis(None, axis=1)
        )
        dt_ArPAP.rename(columns=dt_ArPAP.iloc[0], inplace=True)
        dt_ArPAP.drop(dt_ArPAP.index[0], inplace=True)
        return dt_ArPAP

    def datim_ArCAP_vital_info(self):
        dt_ArCAP = pl.DataFrame._from_dict(
            data={
                "Number of active DREAMS participants that received an evidence-based intervention focused on preventing violence within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Cap-Haïtien")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Plaine-du-Nord")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Limonade")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Milot")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Quartier-Morin")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that received educational support to remain in, advance, and/or rematriculate in school within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Cap-Haïtien")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Plaine-du-Nord")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Limonade")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Milot")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Quartier-Morin")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that completed a comprehensive economic strengthening intervention within the past 6 months at Q2 or past 12 months at Q4.": [
                    (self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Cap-Haïtien")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Plaine-du-Nord")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Limonade")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Milot")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Quartier-Morin")).shape[0]) if True else 0
                ]
            }
        ).to_pandas()
        dt_ArCAP = (
            dt_ArCAP
            .transpose()
            .reset_index()
            .rename_axis(None, axis=1)
        )
        dt_ArCAP.rename(columns=dt_ArCAP.iloc[0], inplace=True)
        dt_ArCAP.drop(dt_ArCAP.index[0], inplace=True)
        return dt_ArCAP

    def datim_ArSM_vital_info(self):
        dt_ArSM = pl.DataFrame._from_dict(
            data={
                "Number of active DREAMS participants that received an evidence-based intervention focused on preventing violence within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Saint-Marc")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Verrettes")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Montrouis")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Liancourt")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "La Chapelle")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that received educational support to remain in, advance, and/or rematriculate in school within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Saint-Marc")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Verrettes")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Montrouis")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Liancourt")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "La Chapelle")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that completed a comprehensive economic strengthening intervention within the past 6 months at Q2 or past 12 months at Q4.": [
                    (self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Saint-Marc")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Verrettes")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Montrouis")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Liancourt")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "La Chapelle")).shape[0]) if True else 0
                ]
            }
        ).to_pandas()
        dt_ArSM = (
            dt_ArSM
            .transpose()
            .reset_index()
            .rename_axis(None, axis=1)
        )
        dt_ArSM.rename(columns=dt_ArSM.iloc[0], inplace=True)
        dt_ArSM.drop(dt_ArSM.index[0], inplace=True)
        return dt_ArSM

    def datim_ArDESS_vital_info(self):
        dt_ArDESS = pl.DataFrame._from_dict(
            data={
                "Number of active DREAMS participants that received an evidence-based intervention focused on preventing violence within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Dessalines")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Desdunes")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Grande Saline")).shape[0] +
                     self.__dreams_valid.filter((pl.col("curriculum") == "yes") & (pl.col("commune") == "Petite Rivière de l'Artibonite")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that received educational support to remain in, advance, and/or rematriculate in school within the reporting period.": [
                    (self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Dessalines")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Desdunes")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Grande Saline")).shape[0] +
                     self.__dreams_valid.filter((pl.col("education") == "yes") & (pl.col("commune") == "Petite Rivière de l'Artibonite")).shape[0]) if True else 0
                ],
                "Number of active DREAMS participants that completed a comprehensive economic strengthening intervention within the past 6 months at Q2 or past 12 months at Q4.": [
                    (self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Dessalines")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Desdunes")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Grande Saline")).shape[0] +
                     self.__dreams_valid.filter((pl.col("socioeco_app") == "yes") & (pl.col("commune") == "Petite Rivière de l'Artibonite")).shape[0]) if True else 0
                ]
            }
        ).to_pandas()
        dt_ArDESS = (
            dt_ArDESS
            .transpose()
            .reset_index()
            .rename_axis(None, axis=1)
        )
        dt_ArDESS.rename(columns=dt_ArDESS.iloc[0], inplace=True)
        dt_ArDESS.drop(dt_ArDESS.index[0], inplace=True)
        return dt_ArDESS

    def datim_agyw_prevI(self):
        __PERIOD_DATIM = list(self.__data.select(pl.col(
            "month_in_program_range").unique().sort()).to_pandas()['month_in_program_range'])
        __PERIOD_DATIM.append("Total")
        __AGE_DATIM = list(self.__data.select(
            pl.col("age_range").unique().sort()).to_pandas()['age_range'])[0:4]
        try:
            pivotableI = self.__agyw_prevI.rename(
                {"age_range": "Time/Age/Sex", "month_in_program_range": "Time"}).to_pandas()
            agyw_prevI_pivot = pivotableI.pivot_table(index="Time/Age/Sex", columns="Time", values="id_patient",
                                                      aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotI = list(agyw_prevI_pivot.columns)
            indexes_pivotI = list(agyw_prevI_pivot.index)
            for period in __PERIOD_DATIM:
                if period not in columns_pivotI:
                    agyw_prevI_pivot[period] = 0
            for age in __AGE_DATIM:
                if age not in indexes_pivotI:
                    agyw_prevI_pivot.loc[age] = 0
            agyw_prevI_pivot = agyw_prevI_pivot.reindex(
                index=__AGE_DATIM, columns=__PERIOD_DATIM)
            agyw_prevI_pivot_final = agyw_prevI_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevI_results_final = DataFrame(
                agyw_prevI_pivot_final.to_records(index=False))
            agyw_prevI_results_final_temp = agyw_prevI_results_final.transpose()
            agyw_prevI_results_final = agyw_prevI_results_final_temp.reset_index(
            ).rename_axis(None, axis=1)
            agyw_prevI_results_final.rename(
                columns=agyw_prevI_results_final.iloc[0], inplace=True)
            agyw_prevI_results_final.drop(
                agyw_prevI_results_final.index[0], inplace=True)
        except ValueError:
            agyw_prevI_results_final = DataFrame(
                {
                    "Time/Age/Sex": [
                        "0-6 months",
                        "07-12 months",
                        "13-24 months",
                        "25+ months",
                        "Total"
                    ],
                    "10-14": [0, 0, 0, 0, 0],
                    "15-19": [0, 0, 0, 0, 0],
                    "20-24": [0, 0, 0, 0, 0],
                    "25-29": [0, 0, 0, 0, 0]
                }
            )

        return agyw_prevI_results_final

    def datim_agyw_prevII(self):
        __PERIOD_DATIM = list(self.__data.select(pl.col(
            "month_in_program_range").unique().sort()).to_pandas()['month_in_program_range'])
        __PERIOD_DATIM.append("Total")
        __AGE_DATIM = list(self.__data.select(
            pl.col("age_range").unique().sort()).to_pandas()['age_range'])[0:4]
        try:
            pivotableII = self.__agyw_prevII.rename(
                {"age_range": "Time/Age/Sex", "month_in_program_range": "Time"}).to_pandas()
            agyw_prevII_pivot = pivotableII.pivot_table(
                index="Time/Age/Sex", columns="Time", values="id_patient", aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotII = list(agyw_prevII_pivot.columns)
            indexes_pivotII = list(agyw_prevII_pivot.index)
            for period in __PERIOD_DATIM:
                if period not in columns_pivotII:
                    agyw_prevII_pivot[period] = 0
            for age in __AGE_DATIM:
                if age not in indexes_pivotII:
                    agyw_prevII_pivot.loc[age] = 0
            agyw_prevII_pivot = agyw_prevII_pivot.reindex(
                index=__AGE_DATIM, columns=__PERIOD_DATIM)
            agyw_prevII_pivot_final = agyw_prevII_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevII_results_final = DataFrame(
                agyw_prevII_pivot_final.to_records(index=False))
            agyw_prevII_results_final_temp = agyw_prevII_results_final.transpose()
            agyw_prevII_results_final = agyw_prevII_results_final_temp.reset_index(
            ).rename_axis(None, axis=1)
            agyw_prevII_results_final.rename(
                columns=agyw_prevII_results_final.iloc[0], inplace=True)
            agyw_prevII_results_final.drop(
                agyw_prevII_results_final.index[0], inplace=True)
        except ValueError:
            agyw_prevII_results_final = DataFrame(
                {
                    "Time/Age/Sex": [
                        "0-6 months",
                        "07-12 months",
                        "13-24 months",
                        "25+ months",
                        "Total"
                    ],
                    "10-14": [0, 0, 0, 0, 0],
                    "15-19": [0, 0, 0, 0, 0],
                    "20-24": [0, 0, 0, 0, 0],
                    "25-29": [0, 0, 0, 0, 0]
                }
            )
        return agyw_prevII_results_final

    def datim_agyw_prevIII(self):
        __PERIOD_DATIM = list(self.__data.select(pl.col(
            "month_in_program_range").unique().sort()).to_pandas()['month_in_program_range'])
        __PERIOD_DATIM.append("Total")
        __AGE_DATIM = list(self.__data.select(
            pl.col("age_range").unique().sort()).to_pandas()['age_range'])[0:4]
        try:
            pivotableIII = self.__agyw_prevIII.rename(
                {"age_range": "Time/Age/Sex", "month_in_program_range": "Time"}).to_pandas()
            agyw_prevIII_pivot = pivotableIII.pivot_table(
                index="Time/Age/Sex", columns="Time", values="id_patient", aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotIII = list(agyw_prevIII_pivot.columns)
            indexes_pivotIII = list(agyw_prevIII_pivot.index)
            for period in __PERIOD_DATIM:
                if period not in columns_pivotIII:
                    agyw_prevIII_pivot[period] = 0
            for age in __AGE_DATIM:
                if age not in indexes_pivotIII:
                    agyw_prevIII_pivot.loc[age] = 0
            agyw_prevIII_pivot = agyw_prevIII_pivot.reindex(
                index=__AGE_DATIM, columns=__PERIOD_DATIM)
            agyw_prevIII_pivot_final = agyw_prevIII_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevIII_results_final = DataFrame(
                agyw_prevIII_pivot_final.to_records(index=False))
            agyw_prevIII_results_final_temp = agyw_prevIII_results_final.transpose()
            agyw_prevIII_results_final = agyw_prevIII_results_final_temp.reset_index(
            ).rename_axis(None, axis=1)
            agyw_prevIII_results_final.rename(
                columns=agyw_prevIII_results_final.iloc[0], inplace=True)
            agyw_prevIII_results_final.drop(
                agyw_prevIII_results_final.index[0], inplace=True)
        except ValueError:
            agyw_prevIII_results_final = DataFrame(
                {
                    "Time/Age/Sex": [
                        "0-6 months",
                        "07-12 months",
                        "13-24 months",
                        "25+ months",
                        "Total"
                    ],
                    "10-14": [0, 0, 0, 0, 0],
                    "15-19": [0, 0, 0, 0, 0],
                    "20-24": [0, 0, 0, 0, 0],
                    "25-29": [0, 0, 0, 0, 0]
                }
            )
        return agyw_prevIII_results_final

    def datim_agyw_prevIV(self):
        __PERIOD_DATIM = list(self.__data.select(pl.col(
            "month_in_program_range").unique().sort()).to_pandas()['month_in_program_range'])
        __PERIOD_DATIM.append("Total")
        __AGE_DATIM = list(self.__data.select(
            pl.col("age_range").unique().sort()).to_pandas()['age_range'])[0:4]
        try:
            pivotableIV = self.__agyw_prevIV.rename(
                {"age_range": "Time/Age/Sex", "month_in_program_range": "Time"}).to_pandas()
            agyw_prevIV_pivot = pivotableIV.pivot_table(
                index="Time/Age/Sex", columns="Time", values="id_patient", aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotIV = list(agyw_prevIV_pivot.columns)
            indexes_pivotIV = list(agyw_prevIV_pivot.index)
            for period in __PERIOD_DATIM:
                if period not in columns_pivotIV:
                    agyw_prevIV_pivot[period] = 0
            for age in __AGE_DATIM:
                if age not in indexes_pivotIV:
                    agyw_prevIV_pivot.loc[age] = 0
            agyw_prevIV_pivot = agyw_prevIV_pivot.reindex(
                index=__AGE_DATIM, columns=__PERIOD_DATIM)
            agyw_prevIV_pivot_final = agyw_prevIV_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevIV_results_final = DataFrame(
                agyw_prevIV_pivot_final.to_records(index=False))
            agyw_prevIV_results_final_temp = agyw_prevIV_results_final.transpose()
            agyw_prevIV_results_final = agyw_prevIV_results_final_temp.reset_index(
            ).rename_axis(None, axis=1)
            agyw_prevIV_results_final.rename(
                columns=agyw_prevIV_results_final.iloc[0], inplace=True)
            agyw_prevIV_results_final.drop(
                agyw_prevIV_results_final.index[0], inplace=True)
        except ValueError:
            agyw_prevIV_results_final = DataFrame(
                {
                    "Time/Age/Sex": [
                        "0-6 months",
                        "07-12 months",
                        "13-24 months",
                        "25+ months",
                        "Total"
                    ],
                    "10-14": [0, 0, 0, 0, 0],
                    "15-19": [0, 0, 0, 0, 0],
                    "20-24": [0, 0, 0, 0, 0],
                    "25-29": [0, 0, 0, 0, 0]
                }
            )
        return agyw_prevIV_results_final


class AgywPrevCommune(AgywPrev):
    """A class that extend AgywPrev with the purpose of the indicator AGYW_PREV DATIM by commune"""
    __who_am_I = "DATIM"

    def __init__(self, name, data):
        self.__name = name
        self.__data = data
        self.__i_am = f"{AgywPrevCommune.__who_am_I} {self.__name}"
        super().__init__(commune=self.__name, data=self.__data)

    @property
    def who_am_i(self):
        return self.__i_am

    def __repr__(self):
        return f"<AgywPrevCommune {self.__i_am}>"

    def __str__(self):
        return f"<AgywPrevCommune {self.__i_am}>"
