from numpy import int16

from ..db import (
    agyw_served_period,
    agyw_served
)
from ..utils import(
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


# EDAnalysis
actif_served = agyw_served[agyw_served.id_patient.isin(
    agyw_served_period.id_patient)]
ua_served = agyw_served[~agyw_served.id_patient.isin(
    agyw_served_period.id_patient)]

actif_served.type_of_test_vih.fillna('no', inplace=True)
actif_served.test_results.fillna('0,', inplace=True)
actif_served.autotest_result.fillna('no', inplace=True)

actif_served.nbre_pres_for_inter.fillna(0, inplace=True)
actif_served.has_comdom_topic.fillna('no', inplace=True)
actif_served.number_of_condoms_sensibilize.fillna(0, inplace=True)
actif_served.number_condoms_reception_in_the_interval.fillna(0, inplace=True)
actif_served.number_test_date_in_the_interval.fillna(0, inplace=True)
actif_served.number_gynecological_care_date_in_the_interval.fillna(
    0, inplace=True)
actif_served.number_vbg_treatment_date_in_the_interval.fillna(0, inplace=True)
actif_served.number_prep_initiation_date_in_the_interval.fillna(
    0, inplace=True)
actif_served.nbre_parenting_coupe_present.fillna(0, inplace=True)
actif_served.number_contraceptive_reception_in_the_interval.fillna(
    0, inplace=True)
actif_served.number_condoms_sensibilization_date_in_the_interval.fillna(
    0, inplace=True)
actif_served.number_prep_awareness_date_in_the_interval.fillna(0, inplace=True)
actif_served.number_prep_reference_date_in_the_interval.fillna(0, inplace=True)
actif_served.number_hiv_test_awareness_date_in_the_interval.fillna(
    0, inplace=True)
actif_served.number_hiv_treatment_start_date_in_the_interval.fillna(
    0, inplace=True)
actif_served.number_contraceptive_sensibilization_date_in_the_interval.fillna(
    0, inplace=True)

actif_served.nbre_pres_for_inter = actif_served.nbre_pres_for_inter.astype(
    int16)
actif_served.number_of_condoms_sensibilize = actif_served.number_of_condoms_sensibilize.astype(
    int16)
actif_served.number_condoms_reception_in_the_interval = actif_served.number_condoms_reception_in_the_interval.astype(
    int16)
actif_served.number_test_date_in_the_interval = actif_served.number_test_date_in_the_interval.astype(
    int16)
actif_served.number_gynecological_care_date_in_the_interval = actif_served.number_gynecological_care_date_in_the_interval.astype(
    int16)
actif_served.number_vbg_treatment_date_in_the_interval = actif_served.number_vbg_treatment_date_in_the_interval.astype(
    int16)
actif_served.number_prep_initiation_date_in_the_interval = actif_served.number_prep_initiation_date_in_the_interval.astype(
    int16)
actif_served.nbre_parenting_coupe_present = actif_served.nbre_parenting_coupe_present.astype(
    int16)
actif_served.number_contraceptive_reception_in_the_interval = actif_served.number_contraceptive_reception_in_the_interval.astype(
    int16)
actif_served.number_condoms_sensibilization_date_in_the_interval = actif_served.number_condoms_sensibilization_date_in_the_interval.astype(
    int16)
actif_served.number_prep_awareness_date_in_the_interval = actif_served.number_prep_awareness_date_in_the_interval.astype(
    int16)
actif_served.number_prep_reference_date_in_the_interval = actif_served.number_prep_reference_date_in_the_interval.astype(
    int16)
actif_served.number_hiv_test_awareness_date_in_the_interval = actif_served.number_hiv_test_awareness_date_in_the_interval.astype(
    int16)
actif_served.number_hiv_treatment_start_date_in_the_interval = actif_served.number_hiv_treatment_start_date_in_the_interval.astype(
    int16)
actif_served.number_contraceptive_sensibilization_date_in_the_interval = actif_served.number_contraceptive_sensibilization_date_in_the_interval.astype(
    int16)

# services
actif_served['education'] = actif_served.has_schooling_payment_in_the_interval
actif_served['parenting_detailed'] = actif_served.nbre_parenting_coupe_present.map(
    parenting_detailed)
actif_served['parenting'] = actif_served.nbre_parenting_coupe_present.map(
    parenting)

actif_served['curriculum_detailed'] = actif_served.nbre_pres_for_inter.map(
    curriculum_detailed)
actif_served['curriculum'] = actif_served.nbre_pres_for_inter.map(curriculum)

actif_served['condom'] = actif_served.apply(lambda df: condom(df), axis=1)

actif_served['hts_awareness'] = actif_served.number_hiv_test_awareness_date_in_the_interval.map(
    hts_awareness)
actif_served['hts_type_test'] = actif_served.type_of_test_vih.map(type_test)
actif_served['hts'] = actif_served.number_test_date_in_the_interval.map(hts)
actif_served['hts_autotest_result'] = actif_served.autotest_result.map(
    vih_autotest_result)
actif_served['hts_test_result'] = actif_served.test_results.map(
    vih_test_result)
actif_served['hts_treatment_debut'] = actif_served.number_hiv_treatment_start_date_in_the_interval.map(
    treatment_debut)


actif_served['vbg'] = actif_served.number_vbg_treatment_date_in_the_interval.map(
    vbg)
actif_served['gyneco'] = actif_served.number_gynecological_care_date_in_the_interval.map(
    gyneco)

actif_served['post_violence_care'] = actif_served.apply(
    lambda df: postcare(df), axis=1)

actif_served['socioeco_app'] = actif_served.apply(
    lambda df: socioeco(df), axis=1)

actif_served['prep_awareness'] = actif_served.number_prep_awareness_date_in_the_interval.map(
    prep_awareness)
actif_served['prep_reference'] = actif_served.number_prep_reference_date_in_the_interval.map(
    prep_reference)
actif_served['prep'] = actif_served.number_prep_initiation_date_in_the_interval.map(
    prep)

actif_served['contraceptive_awareness'] = actif_served.number_contraceptive_sensibilization_date_in_the_interval.map(
    contraceptive_awareness)
actif_served['contraceptive'] = actif_served.number_contraceptive_reception_in_the_interval.map(
    contraceptive)

actif_served['ps_1014'] = actif_served.apply(lambda df: prim_1014(df), axis=1)
actif_served['ps_1519'] = actif_served.apply(lambda df: prim_1519(df), axis=1)
actif_served['ps_2024'] = actif_served.apply(lambda df: prim_2024(df), axis=1)

"""
actif_served['secondary_1014'] = actif_served.apply(lambda df: sec_1014(df),axis=1 )
actif_served['secondary_1519'] = actif_served.apply(lambda df: sec_1519(df),axis=1 )
actif_served['secondary_2024'] = actif_served.apply(lambda df: sec_2024(df),axis=1 )
actif_served['complete_1014'] = actif_served.apply(lambda df: comp_1014(df),axis=1 )
actif_served['complete_1519'] = actif_served.apply(lambda df: comp_1519(df),axis=1 )
actif_served['complete_2024'] = actif_served.apply(lambda df: comp_2024(df),axis=1 )
"""
