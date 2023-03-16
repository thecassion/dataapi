#from ..analysis import datim

def datim_function(datim): 
    return {
        "who_am_i": datim.who_am_i,
        "total_datim": int(datim.total_datim_general),
        "titleI": datim.datim_titleI(),
        "tableI": datim.datim_agyw_prevI().to_dict('split'),
        'total_datimI': int(datim.total_datimI),
        "titleII": datim.datim_titleII(),
        "tableII": datim.datim_agyw_prevII().to_dict('split'),
        'total_datimII': int(datim.total_datimII),
        "titleIII": datim.datim_titleIII(),
        "tableIII": datim.datim_agyw_prevIII().to_dict('split'),
        'total_datimIII': int(datim.total_datimIII),
        "titleIV": datim.datim_titleIV(),
        "tableIV": datim.datim_agyw_prevIV().to_dict('split'),
        'total_datimIV': int(datim.total_datimIV)
    }
