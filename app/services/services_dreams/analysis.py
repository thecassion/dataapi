from dataclasses import dataclass, InitVar
from numpy import int16
from .utils import (
    prim_2024,
    prim_1519,
    prim_1014,
    socioeco,
    postcare,
    contraceptive,
    contraceptive_awareness,
    prep,
    prep_reference,
    prep_awareness,
    gyneco,
    vbg,
    hts,
    treatment_debut,
    hts_awareness,
    condom,
    parenting,
    parenting_detailed,
    curriculum,
    curriculum_detailed,
    vih_autotest_result,
    vih_test_result,
    type_test
)

@dataclass
class AGYW_Analysis:
    def __init__(self, agyw_served, agyw_served_period):
        self.agyw_served:InitVar = agyw_served
        self.agyw_served_period: InitVar = agyw_served_period
        self.__actif_served = agyw_served[agyw_served.id_patient.isin(
    agyw_served_period.id_patient)]
         

    def data_actif_served(self):
        __actif_served = self.__actif_served
        __actif_served.type_of_test_vih.fillna('no', inplace=True)
        __actif_served.test_results.fillna('0,', inplace=True)
        __actif_served.autotest_result.fillna('no', inplace=True)

        __actif_served.nbre_pres_for_inter.fillna(0, inplace=True)
        __actif_served.has_comdom_topic.fillna('no', inplace=True)
        __actif_served.number_of_condoms_sensibilize.fillna(0, inplace=True)
        __actif_served.number_condoms_reception_in_the_interval.fillna(0, inplace=True)
        __actif_served.number_test_date_in_the_interval.fillna(0, inplace=True)
        __actif_served.number_gynecological_care_date_in_the_interval.fillna(
            0, inplace=True)
        __actif_served.number_vbg_treatment_date_in_the_interval.fillna(0, inplace=True)
        __actif_served.number_prep_initiation_date_in_the_interval.fillna(
            0, inplace=True)
        __actif_served.nbre_parenting_coupe_present.fillna(0, inplace=True)
        __actif_served.number_contraceptive_reception_in_the_interval.fillna(
            0, inplace=True)
        __actif_served.number_condoms_sensibilization_date_in_the_interval.fillna(
            0, inplace=True)
        __actif_served.number_prep_awareness_date_in_the_interval.fillna(0, inplace=True)
        __actif_served.number_prep_reference_date_in_the_interval.fillna(0, inplace=True)
        __actif_served.number_hiv_test_awareness_date_in_the_interval.fillna(
            0, inplace=True)
        __actif_served.number_hiv_treatment_start_date_in_the_interval.fillna(
            0, inplace=True)
        __actif_served.number_contraceptive_sensibilization_date_in_the_interval.fillna(
            0, inplace=True)

        __actif_served.nbre_pres_for_inter = __actif_served.nbre_pres_for_inter.astype(
            int16)
        __actif_served.number_of_condoms_sensibilize = __actif_served.number_of_condoms_sensibilize.astype(
            int16)
        __actif_served.number_condoms_reception_in_the_interval = __actif_served.number_condoms_reception_in_the_interval.astype(
            int16)
        __actif_served.number_test_date_in_the_interval = __actif_served.number_test_date_in_the_interval.astype(
            int16)
        __actif_served.number_gynecological_care_date_in_the_interval = __actif_served.number_gynecological_care_date_in_the_interval.astype(
            int16)
        __actif_served.number_vbg_treatment_date_in_the_interval = __actif_served.number_vbg_treatment_date_in_the_interval.astype(
            int16)
        __actif_served.number_prep_initiation_date_in_the_interval = __actif_served.number_prep_initiation_date_in_the_interval.astype(
            int16)
        __actif_served.nbre_parenting_coupe_present = __actif_served.nbre_parenting_coupe_present.astype(
            int16)
        __actif_served.number_contraceptive_reception_in_the_interval = __actif_served.number_contraceptive_reception_in_the_interval.astype(
            int16)
        __actif_served.number_condoms_sensibilization_date_in_the_interval = __actif_served.number_condoms_sensibilization_date_in_the_interval.astype(
            int16)
        __actif_served.number_prep_awareness_date_in_the_interval = __actif_served.number_prep_awareness_date_in_the_interval.astype(
            int16)
        __actif_served.number_prep_reference_date_in_the_interval = __actif_served.number_prep_reference_date_in_the_interval.astype(
            int16)
        __actif_served.number_hiv_test_awareness_date_in_the_interval = __actif_served.number_hiv_test_awareness_date_in_the_interval.astype(
            int16)
        __actif_served.number_hiv_treatment_start_date_in_the_interval = __actif_served.number_hiv_treatment_start_date_in_the_interval.astype(
            int16)
        __actif_served.number_contraceptive_sensibilization_date_in_the_interval = __actif_served.number_contraceptive_sensibilization_date_in_the_interval.astype(
            int16)

        # services
        __actif_served['education'] = __actif_served.has_schooling_payment_in_the_interval
        __actif_served['parenting_detailed'] = __actif_served.nbre_parenting_coupe_present.map(
            parenting_detailed)
        __actif_served['parenting'] = __actif_served.nbre_parenting_coupe_present.map(
            parenting)

        __actif_served['curriculum_detailed'] = __actif_served.nbre_pres_for_inter.map(
            curriculum_detailed)
        __actif_served['curriculum'] = __actif_served.nbre_pres_for_inter.map(curriculum)

        __actif_served['condom'] = __actif_served.apply(lambda df: condom(df), axis=1)

        __actif_served['hts_awareness'] = __actif_served.number_hiv_test_awareness_date_in_the_interval.map(
            hts_awareness)
        __actif_served['hts_type_test'] = __actif_served.type_of_test_vih.map(type_test)
        __actif_served['hts'] = __actif_served.number_test_date_in_the_interval.map(hts)
        __actif_served['hts_autotest_result'] = __actif_served.autotest_result.map(
            vih_autotest_result)
        __actif_served['hts_test_result'] = __actif_served.test_results.map(
            vih_test_result)
        __actif_served['hts_treatment_debut'] = __actif_served.number_hiv_treatment_start_date_in_the_interval.map(
            treatment_debut)


        __actif_served['vbg'] = __actif_served.number_vbg_treatment_date_in_the_interval.map(
            vbg)
        __actif_served['gyneco'] = __actif_served.number_gynecological_care_date_in_the_interval.map(
            gyneco)

        __actif_served['post_violence_care'] = __actif_served.apply(
            lambda df: postcare(df), axis=1)

        __actif_served['socioeco_app'] = __actif_served.apply(
            lambda df: socioeco(df), axis=1)

        __actif_served['prep_awareness'] = __actif_served.number_prep_awareness_date_in_the_interval.map(
            prep_awareness)
        __actif_served['prep_reference'] = __actif_served.number_prep_reference_date_in_the_interval.map(
            prep_reference)
        __actif_served['prep'] = __actif_served.number_prep_initiation_date_in_the_interval.map(
            prep)

        __actif_served['contraceptive_awareness'] = __actif_served.number_contraceptive_sensibilization_date_in_the_interval.map(
            contraceptive_awareness)
        __actif_served['contraceptive'] = __actif_served.number_contraceptive_reception_in_the_interval.map(
            contraceptive)

        __actif_served['ps_1014'] = __actif_served.apply(lambda df: prim_1014(df), axis=1)
        __actif_served['ps_1519'] = __actif_served.apply(lambda df: prim_1519(df), axis=1)
        __actif_served['ps_2024'] = __actif_served.apply(lambda df: prim_2024(df), axis=1)
        return self.__actif_served
