
class StatusEid():

    @classmethod
    def get_filter_title(cls, eid):
        year = eid.Year.drop_duplicates().sort_values().tolist()
        quarter = eid.Quarter.drop_duplicates().sort_values().tolist()
        office = eid.Office.drop_duplicates().sort_values().tolist()
        hospital = eid.hospital.drop_duplicates().sort_values().tolist()
        result = eid.Result.drop_duplicates().sort_values().tolist()
        tranche_age = eid.tranche_age.drop_duplicates().sort_values().tolist()
        liaison_mere = eid.Liaison_mere.drop_duplicates().sort_values().tolist()
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
    def pcr_status_deprecated(cls, eid, year=None, office=None, hospital=None):
        if (year is None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Office == office) & (eid.hospital == hospital)]
        elif (office is None and year is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.hospital == hospital)]
        elif (hospital is None and year is not None and office is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.Office == office)]
        elif (year is not None and office is None and hospital is None):
            eid_Qi = eid[(eid.Year == year)]
        elif (office is not None and year is None and hospital is None):
            eid_Qi = eid[(eid.Office == office)]
        elif (hospital is not None and year is None and office is None):
            eid_Qi = eid[(eid.hospital == hospital)]
        elif (year is not None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (
                eid.Office == office) & (eid.hospital == hospital)]
        else:
            eid_Qi = eid

        EID = eid_Qi.pivot_table(
            values='Patient_code',
            index=["tranche_age", "Year", "Quarter"],
            # columns=["Year", "Quarter"],
            aggfunc=len,
            fill_value=0,
        )
        return EID

    @classmethod
    def pcr_status(cls, eid, year=None, office=None, hospital=None):
        if (year is None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Office == office) & (eid.hospital == hospital)]
        elif (office is None and year is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.hospital == hospital)]
        elif (hospital is None and year is not None and office is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.Office == office)]
        elif (year is not None and office is None and hospital is None):
            eid_Qi = eid[(eid.Year == year)]
        elif (office is not None and year is None and hospital is None):
            eid_Qi = eid[(eid.Office == office)]
        elif (hospital is not None and year is None and office is None):
            eid_Qi = eid[(eid.hospital == hospital)]
        elif (year is not None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (
                eid.Office == office) & (eid.hospital == hospital)]
        else:
            eid_Qi = eid

        EID = eid_Qi.pivot_table(
            values='Patient_code',
            index=["Year", "Quarter", "tranche_age"],
            aggfunc=len,
            fill_value=0,
        ).reset_index().rename_axis(axis=1).rename(columns={'Patient_code': "Total"})
        return EID

    @classmethod
    def positivity_status(cls, eid, year=None, office=None, hospital=None):
        if (year is None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Office == office) & (eid.hospital == hospital)]
        elif (office is None and year is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.hospital == hospital)]
        elif (hospital is None and year is not None and office is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.Office == office)]
        elif (year is not None and office is None and hospital is None):
            eid_Qi = eid[(eid.Year == year)]
        elif (office is not None and year is None and hospital is None):
            eid_Qi = eid[(eid.Office == office)]
        elif (hospital is not None and year is None and office is None):
            eid_Qi = eid[(eid.hospital == hospital)]
        elif (year is not None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (
                eid.Office == office) & (eid.hospital == hospital)]
        else:
            eid_Qi = eid

        EID = eid_Qi.pivot_table(
            values='Patient_code',
            index=["Year", "Quarter", "tranche_age"],
            aggfunc=len,
            fill_value=0,
        ).reset_index().rename_axis(axis=1).rename(columns={'Patient_code': "Total"})
        return EID

    @classmethod
    def liaison_status(cls, eid, year=None, office=None, hospital=None):
        if (year is None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Office == office) & (eid.hospital == hospital)]
        elif (office is None and year is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.hospital == hospital)]
        elif (hospital is None and year is not None and office is not None):
            eid_Qi = eid[(eid.Year == year) & (eid.Office == office)]
        elif (year is not None and office is None and hospital is None):
            eid_Qi = eid[(eid.Year == year)]
        elif (office is not None and year is None and hospital is None):
            eid_Qi = eid[(eid.Office == office)]
        elif (hospital is not None and year is None and office is None):
            eid_Qi = eid[(eid.hospital == hospital)]
        elif (year is not None and office is not None and hospital is not None):
            eid_Qi = eid[(eid.Year == year) & (
                eid.Office == office) & (eid.hospital == hospital)]
        else:
            eid_Qi = eid

        EID = eid_Qi.pivot_table(
            values='Patient_code',
            columns="Liaison_mere",
            aggfunc=len,
            fill_value=0,
        )
        return EID
