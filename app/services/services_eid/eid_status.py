
class StatusEid():
    
    @classmethod
    def get_filter_title(cls,eid):
        year = eid.Year.unique().tolist()
        quarter = eid.Quarter.unique().tolist()
        office = eid.Office.unique().tolist()
        hospital = eid.hospital.unique().tolist()
        result = eid.Result.unique().tolist()
        tranche_age = eid.tranche_age.unique().tolist()
        liaison_mere = eid.Liaison_mere.unique().tolist()
        return {
            "year_title": year,
            "quarter_title": quarter,
            "office_title": office,
            "hospital_title": hospital,
            "result_title": result,
            "tranche_age_title": tranche_age,
            "liaison_mere_title": liaison_mere
        }
    
    @classmethod
    def pcr_status(cls,eid, year=None,office=None, hospital=None):
        if (year is None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Office==office)&(eid.hospital==hospital)]
        elif(office is None and year is not None and hospital is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.hospital==hospital)]
        elif(hospital is None and year is not None and office is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.Office==office)]
        elif(year is not None and office is None and hospital is None):
            eid_Qi = eid[(eid.Year==year)]
        elif(office is not None and year is None and hospital is None):
            eid_Qi = eid[(eid.Office==office)]
        elif(hospital is not None and year is None and office is None):
            eid_Qi = eid[(eid.hospital==hospital)]
        elif(year is not None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.Office==office)&(eid.hospital==hospital)]
        else:
            eid_Qi = eid
        
        EID = eid_Qi.pivot_table(
            values='Patient_code',
            index="tranche_age",
            columns=["Year", "Quarter"],
            aggfunc=len,
            fill_value = 0, 
        )
        return EID 

    @classmethod
    def positivity_status(cls,eid, year=None,office=None, hospital=None):
        if (year is None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Office==office)&(eid.hospital==hospital)]
        elif(office is None and year is not None and hospital is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.hospital==hospital)]
        elif(hospital is None and year is not None and office is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.Office==office)]
        elif(year is not None and office is None and hospital is None):
            eid_Qi = eid[(eid.Year==year)]
        elif(office is not None and year is None and hospital is None):
            eid_Qi = eid[(eid.Office==office)]
        elif(hospital is not None and year is None and office is None):
            eid_Qi = eid[(eid.hospital==hospital)]
        elif(year is not None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.Office==office)&(eid.hospital==hospital)]
        else:
            eid_Qi = eid
        
        EID = eid_Qi.pivot_table(
            values='Patient_code',
            index="Result",
            columns=["Year", "Quarter"],
            aggfunc=len,
            fill_value = 0, 
        )
        return EID

    @classmethod
    def liaison_status(cls,eid, year=None,office=None, hospital=None):
        if (year is None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Office==office)&(eid.hospital==hospital)]
        elif(office is None and year is not None and hospital is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.hospital==hospital)]
        elif(hospital is None and year is not None and office is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.Office==office)]
        elif(year is not None and office is None and hospital is None):
            eid_Qi = eid[(eid.Year==year)]
        elif(office is not None and year is None and hospital is None):
            eid_Qi = eid[(eid.Office==office)]
        elif(hospital is not None and year is None and office is None):
            eid_Qi = eid[(eid.hospital==hospital)]
        elif(year is not None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Year==year)&(eid.Office==office)&(eid.hospital==hospital)]
        else:
            eid_Qi = eid
        
        EID = eid_Qi.pivot_table(
            values='Patient_code',
            columns="Liaison_mere",
            aggfunc=len,
            fill_value = 0,
        )
        return EID 

