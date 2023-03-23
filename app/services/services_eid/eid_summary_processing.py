def pmtctEID_heiPos_ON_ARV(eid, year=None):
    if year == None:
        eid_Qi = eid
    else:
        eid_Qi = eid[(eid['Year'] == year)]


    #Nombre de spécimens par tranche d'âge
    PT = eid_Qi.pivot_table(values='Patient_code', index = 'tranche_age', aggfunc = len, fill_value = 0, margins = True, margins_name ='Total')
    PT.rename(columns = {'Patient_code' : 'Spécimens prélevés'}, inplace = True)

    #Pourcentage
    PTp = round(PT.div(PT.iloc[-1,:], axis = 1), 2)*100
    PTp.rename(columns = {'Spécimens prélevés' : '% Spécimens prélevés'}, inplace = True)

    # Nombre de positif
    eid_positive = eid_Qi[eid_Qi['Result'] == 'ADN VIH-1 Detècté']
    PT2 = eid_positive.pivot_table(values = 'Patient_code', index = 'tranche_age', aggfunc = len, margins = True, margins_name ='Total')
    PT2.rename(columns = {'Patient_code' : 'Enfants positifs'}, inplace = True)

    #Nombre sous ARV
    eid_on_arv = eid_positive[eid_positive['on_arv'] == 'yes']
    PT3 = eid_on_arv.pivot_table(values = 'Patient_code', index = 'tranche_age', aggfunc = len, margins = True, margins_name = 'Total')
    PT3.rename(columns = {'Patient_code' : 'Sous ARV'}, inplace = True)

    #Enfants sans résultat
    eid_no_result = eid_Qi[(eid_Qi['Result'] != 'ADN VIH-1 Non-Detècté') & (eid_Qi['Result'] != 'ADN VIH-1 Detècté')]
    PT4 = eid_no_result.pivot_table(values = 'Patient_code', index = 'tranche_age', aggfunc = len, margins = True, margins_name = 'Total')
    PT4.rename(columns = {'Patient_code' : 'Sans résultat'}, inplace =True)

    #Concaténation des données et tableau final
    EID = PT.merge(PTp, on = 'tranche_age', how = 'left').merge(PT2, on = 'tranche_age', how = 'left').merge(PT3, on = 'tranche_age', how = 'left').merge(PT4, on = 'tranche_age', how = 'left')
    EID.reset_index(inplace = True)
    EID = EID.fillna(0)
    return EID
