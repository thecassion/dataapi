from dataclasses import dataclass, InitVar
import polars as pl


@dataclass
class AGYW_Analysis:
    def __init__(self, agyw_served, agyw_served_period):
        self.agyw_served: InitVar = agyw_served
        self.agyw_served_period: InitVar = agyw_served_period
        self.__actif_served = agyw_served.join(
            agyw_served_period, on="id_patient", how="semi")

    def data_actif_served(self):
        __actif_served = self.__actif_served
        __actif_served = __actif_served.with_columns(
            [pl.col(pl.Float64).cast(pl.Int64)])
        __actif_served = __actif_served.with_columns([
            pl.col("type_of_test_vih").fill_null(value="no"),
            pl.col("test_results").fill_null(value="0,"),
            pl.col("autotest_result").fill_null(value="no"),
            pl.col("has_comdom_topic").fill_null(value="no"),
            pl.col("has_preventive_vbg").fill_null(value="no"),
            pl.col("nbre_pres_for_inter").fill_null(strategy="zero"),
            pl.col("number_autotest_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_of_condoms_sensibilize").fill_null(strategy="zero"),
            pl.col("number_condoms_reception_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_test_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_gynecological_care_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_vbg_treatment_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_prep_initiation_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("nbre_parenting_coupe_present").fill_null(strategy="zero"),
            pl.col("number_contraceptive_reception_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_condoms_sensibilization_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_prep_awareness_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_prep_reference_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_hiv_test_awareness_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_hiv_treatment_start_date_in_the_interval").fill_null(
                strategy="zero"),
            pl.col("number_contraceptive_sensibilization_date_in_the_interval").fill_null(
                strategy="zero")
        ])
        __actif_served = __actif_served.with_columns(
            (pl.col("has_schooling_payment_in_the_interval")).alias("education")
        )
        __actif_served = __actif_served.with_columns(
            (pl.col("has_schooling_payment_in_the_interval")).alias("education")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("nbre_pres_for_inter") >=
                    17).then("yes").otherwise("no")
            .alias("curriculum")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("nbre_pres_for_inter") >= 17).then("yes").when(pl.col(
                "nbre_pres_for_inter").is_between(1, 16)).then("has_started").otherwise("no")
            .alias("curriculum_detailed")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("nbre_parenting_coupe_present")
                    >= 12).then("yes").otherwise("no")
            .alias("parenting")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("nbre_parenting_coupe_present") >= 12).then("yes").when(pl.col(
                "nbre_parenting_coupe_present").is_between(1, 11)).then("has_started").otherwise("no")
            .alias("parenting_detailed")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(
                (pl.col("has_comdom_topic") == "yes") | (pl.col("number_of_condoms_sensibilize") > 0) | (pl.col(
                    "number_condoms_reception_in_the_interval") > 0) | (pl.col("number_condoms_sensibilization_date_in_the_interval") > 0)
            ).then("yes").otherwise("no")
            .alias("condom")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_hiv_test_awareness_date_in_the_interval")
                    > 0).then("yes").otherwise("no")
            .alias("hts_awareness")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_test_date_in_the_interval")
                    > 0).then("yes").otherwise("no")
            .alias("hts")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("type_of_test_vih") == '0,').then("sanguin")
            .when(
                (pl.col("type_of_test_vih") == '1,') | (
                    pl.col("type_of_test_vih") == '0,,1,')
            ).then("autotest")
            .when(pl.col("type_of_test_vih") == "no").then("no_info")
            .otherwise("verify_me")
            .alias("hts_type_test")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("autotest_result") == "no").then("no_info")
            .when(pl.col("autotest_result") == "0,").then("indeterminee")
            .when(
                (pl.col("autotest_result") == '0,,1,') | (
                    pl.col("autotest_result") == '1,')
            ).then("non_reactif")
            .when(
                (pl.col("autotest_result") == '0,,2,') | (
                    pl.col("autotest_result") == '2,')
            ).then("reactif")
            .otherwise("verify_me")
            .alias("hts_autotest_result")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("test_results") == "0,").then("no_info")
            .when(
                (pl.col("test_results") == '0,,1,') | (
                    pl.col("test_results") == '1,')
            ).then("positif")
            .when(
                (pl.col("test_results") == '0,,2,') | (
                    pl.col("test_results") == '2,')
            ).then("negatif")
            .when(
                (pl.col("test_results") == '0,,3,') | (pl.col("test_results") == '0,,2,,3,') | (
                    pl.col("test_results") == '2,,3,') | (pl.col("test_results") == '3,')
            ).then("indeterminee")
            .otherwise("verify_me")
            .alias("hts_test_result")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_hiv_treatment_start_date_in_the_interval") > 0).then(
                "yes")
            .otherwise("no")
            .alias("hts_treatment_debut")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_vbg_treatment_date_in_the_interval")
                    > 0).then("yes")
            .otherwise("no")
            .alias("vbg")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_gynecological_care_date_in_the_interval") > 0).then(
                "yes")
            .otherwise("no")
            .alias("gyneco")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(
                (pl.col("number_vbg_treatment_date_in_the_interval") > 0) | (
                    pl.col("number_gynecological_care_date_in_the_interval") > 0)
            ).then("yes")
            .otherwise("no")
            .alias("post_violence_care")
        )
        __actif_served = __actif_served.with_columns(
            pl.when((pl.col("muso") == "yes") | (pl.col("gardening") == "yes")
                    ).then("yes")
            .otherwise("no")
            .alias("socioeco_app")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_prep_awareness_date_in_the_interval")
                    > 0).then("yes")
            .otherwise("no")
            .alias("prep_awareness")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_prep_reference_date_in_the_interval")
                    > 0).then("yes")
            .otherwise("no")
            .alias("prep_reference")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_prep_initiation_date_in_the_interval") > 0).then(
                "yes")
            .otherwise("no")
            .alias("prep")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col(
                "number_contraceptive_sensibilization_date_in_the_interval") > 0).then("yes")
            .otherwise("no")
            .alias("contraceptive_awareness")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(pl.col("number_contraceptive_reception_in_the_interval") > 0).then(
                "yes")
            .otherwise("no")
            .alias("contraceptive")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(
                (pl.col("age_range") == "10-14") & (pl.col("curriculum") == "yes")
            ).then("primary")
            .otherwise("no")
            .alias("ps_1014")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(
                (pl.col("age_range") == "15-19") & (pl.col("curriculum")
                                                    == "yes") & (pl.col("condom") == "yes")
            ).then("primary")
            .otherwise("no")
            .alias("ps_1519")
        )
        __actif_served = __actif_served.with_columns(
            pl.when(
                (pl.col("age_range") == "20-24") & (pl.col("curriculum")
                                                    == "yes") & (pl.col("condom") == "yes")
            ).then("primary")
            .otherwise("no")
            .alias("ps_2024")
        )
        return __actif_served
