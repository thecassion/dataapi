EID = '''
select Patient_code,Code_mere,if(Code_mere in (select patient_code from patient),'yes','no') as Liaison_mere,left(patient_code,8) as Site_or_lab_code, hospital,Office, if(Network is not null,Network,'Autres') as Network, date_of_birth, gender, start_date as arv_start, if(Office = 'CAP', 'HUJ', lab) as lab, date_blood_taken, date_caris_received_sample, date_lab_received_sample, Result,
if(timestampdiff(day,date_of_birth,date_blood_taken)<=61,if(timestampdiff(day,date_of_birth,date_blood_taken)>=0,'0_2',''),if(timestampdiff(day,date_of_birth,date_blood_taken)<=365,if(timestampdiff(day,date_of_birth,date_blood_taken)>61,'2_12',''),'')) as tranche_age
,if(arv is not null,'yes','') as on_arv, timestampdiff(day,date_lab_received_sample,now()) as total_jours_Labo, timestampdiff(day,date_blood_taken,pcr_result_date) as from_specimen_to_result, timestampdiff(day,date_blood_taken,date_caris_received_sample) as from_specimen_to_caris,
timestampdiff(day,date_caris_received_sample,date_lab_received_sample) as from_caris_to_lab, timestampdiff(day,date_lab_received_sample,pcr_result_date) as from_lab_to_result,

if(!(pcr_result_date is null or pcr_result_date ='0000-00-00') and !(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'),timestampdiff(day,date_lab_received_sample,pcr_result_date) , IF(!(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'), timestampdiff(day,date_lab_received_sample,now()),"not_in_lab")) as days_in_lab,
case 
	when if(!(pcr_result_date is null or pcr_result_date ='0000-00-00') and !(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'),timestampdiff(day,date_lab_received_sample,pcr_result_date) , IF(!(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'), timestampdiff(day,date_lab_received_sample,now()),"not_in_lab"))>15 then 'greater than 15'
    when if(!(pcr_result_date is null or pcr_result_date ='0000-00-00') and !(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'),timestampdiff(day,date_lab_received_sample,pcr_result_date) , IF(!(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'), timestampdiff(day,date_lab_received_sample,now()),"not_in_lab"))<=15 then 'less or equal to 15'
    else'not in lab' end as Nbres_jours_Labo
   , 
   #timestampdiff(day,date_caris_received_sample,now()) as Total_jours_Caris,
   
if(!(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'),timestampdiff(day,date_caris_received_sample,date_lab_received_sample),timestampdiff(day,date_caris_received_sample,now())) as days_between_caris_lab,
case 
	when if(!(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00') ,timestampdiff(day,date_caris_received_sample,date_lab_received_sample),timestampdiff(day,date_caris_received_sample,now())) >5 then 'greater than 5'
    when if(!(date_lab_received_sample is null or date_lab_received_sample ='0000-00-00'),timestampdiff(day,date_caris_received_sample,date_lab_received_sample),timestampdiff(day,date_caris_received_sample,now())) <=5 then 'less or equal to 5'
 end as Nbres_jours_Caris,

if(pcr_result=1,if(patient_info is not null,if(is_abandoned=0 and is_dead=0,'no','yes'),''),'') as pos_dead_abandon
,death_date
,abandoned_date
,pcr_result_date,
case 
    when month(date_blood_taken) between 10 and 12 then 'Q1'
    when month(date_blood_taken) between 1 and 3 then 'Q2'
    when month(date_blood_taken) between 4 and 6 then 'Q3'
    else 'Q4'
 End as Quarter,  -- month(date_blood_taken)  as month_blood_taken,
 #year(date_blood_taken) as year_test,
if( month(date_blood_taken) between 10 and 12 ,year(date_blood_taken)+1,year(date_blood_taken) ) as Year
from(
select patient_code,tme.id_patient as id_in_mere ,concat(tme.mother_city_code,'/',tme.mother_hospital_code,'/',tme.mother_code) as Code_mere, date_of_birth, tracking_infant.id_patient as patient_info, date_blood_taken, pcr_result,arv.id_patient as arv, is_dead, is_abandoned, death_date, abandoned_date, pcr_result_date, date_caris_received_sample, date_lab_received_sample,
lookup_testing_specimen_result.name as _result, if(lookup_testing_specimen_result.name="", 'En cours',lookup_testing_specimen_result.name) as Result,lookup_hospital.name as hospital,lookup_network.name as Network,lookup_hospital.office as Office, lookup_testing_gender.name as gender, tracking_regime.start_date, lookup_lab.name as lab from(
    select * from testing_specimen t1 where timestampdiff(day,date_of_birth,date_blood_taken)<=365 and date_blood_taken between '2014-10-01' and '2030-12-31'
    and date_blood_taken=(select min(date_blood_taken) from testing_specimen t2 where t1.id_patient=t2.id_patient)
)test_1
left join patient pa on pa.id=test_1.id_patient
left join testing_mereenfant tme on pa.id= tme.id_patient
left join
(select * from(
    select id_patient from tracking_followup where on_arv=1
    union
    select id_patient from tracking_regime where category='regime_infant_treatment'
    )o
)arv on arv.id_patient=test_1.id_patient
left join tracking_infant on tracking_infant.id_patient=test_1.id_patient
left join lookup_testing_specimen_result on pcr_result=lookup_testing_specimen_result.id
left join lookup_hospital on concat(lookup_hospital.city_code,'/',lookup_hospital.hospital_code)=left(patient_code,8)
left join tracking_regime on tracking_regime.id_patient=test_1.id_patient
left join lookup_testing_gender on lookup_testing_gender.id=gender
left join lookup_lab on lookup_hospital.id_lab=lookup_lab.id
left join lookup_network on lookup_hospital.network=lookup_network.id
where timestampdiff(day,test_1.date_of_birth,test_1.date_blood_taken)<=365 and test_1.date_blood_taken between '2014-10-01' and '2030-12-31' and linked_to_id_patient=0
group by test_1.id_patient) final
'''
