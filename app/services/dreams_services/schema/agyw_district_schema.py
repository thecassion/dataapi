from ..analysis import (
    total_datimI_PAP,
    total_datimII_PAP,
    total_datimIII_PAP,
    total_datimIV_PAP,
    total_datim_general_PAP,
    datimI_PAP,
    datimII_PAP,
    datimIII_PAP,
    datimIV_PAP,
    total_datimI_CAP,
    total_datimII_CAP,
    total_datimIII_CAP,
    total_datimIV_CAP,
    total_datim_general_CAP,
    datimI_CAP,
    datimII_CAP,
    datimIII_CAP,
    datimIV_CAP,
    total_datimI_SM,
    total_datimII_SM,
    total_datimIII_SM,
    total_datimIV_SM,
    total_datim_general_SM,
    datimI_SM,
    datimII_SM,
    datimIII_SM,
    datimIV_SM,
    total_datimI_DESS,
    total_datimII_DESS,
    total_datimIII_DESS,
    total_datimIV_DESS,
    total_datim_general_DESS,
    datimI_DESS,
    datimII_DESS,
    datimIII_DESS,
    datimIV_DESS,
    datim
)


DISTRICT = {
    'PAP': {
        "total_datim": int(total_datim_general_PAP),
        "titleI": datim.datim_titleI(),
        "tableI": datimI_PAP.to_dict('split'),
        'total_datimI': int(total_datimI_PAP),
        "titleII": datim.datim_titleII(),
        "tableII": datimII_PAP.to_dict('split'),
        'total_datimII': int(total_datimII_PAP),
        "titleIII": datim.datim_titleIII(),
        "tableIII": datimIII_PAP.to_dict('split'),
        'total_datimIII': int(total_datimIII_PAP),
        "titleIV": datim.datim_titleIV(),
        "tableIV": datimIV_PAP.to_dict('split'),
        'total_datimIV': int(total_datimIV_PAP)
    },
    'CAP': {
        "total_datim": int(total_datim_general_CAP),
        "titleI": datim.datim_titleI(),
        "tableI": datimI_CAP.to_dict('split'),
        'total_datimI': int(total_datimI_CAP),
        "titleII": datim.datim_titleII(),
        "tableII": datimII_CAP.to_dict('split'),
        'total_datimII': int(total_datimII_CAP),
        "titleIII": datim.datim_titleIII(),
        "tableIII": datimIII_CAP.to_dict('split'),
        'total_datimIII': int(total_datimIII_CAP),
        "titleIV": datim.datim_titleIV(),
        "tableIV": datimIV_CAP.to_dict('split'),
        'total_datimIV': int(total_datimIV_CAP)
    },
    'SM': {
        "total_datim": int(total_datim_general_SM),
        "titleI": datim.datim_titleI(),
        "tableI": datimI_SM.to_dict('split'),
        'total_datimI': int(total_datimI_SM),
        "titleII": datim.datim_titleII(),
        "tableII": datimII_SM.to_dict('split'),
        'total_datimII': int(total_datimII_SM),
        "titleIII": datim.datim_titleIII(),
        "tableIII": datimIII_SM.to_dict('split'),
        'total_datimIII': int(total_datimIII_SM),
        "titleIV": datim.datim_titleIV(),
        "tableIV": datimIV_SM.to_dict('split'),
        'total_datimIV': int(total_datimIV_SM)
    },
    'DESS': {
        "total_datim": int(total_datim_general_DESS),
        "titleI": datim.datim_titleI(),
        "tableI": datimI_DESS.to_dict('split'),
        'total_datimI': int(total_datimI_DESS),
        "titleII": datim.datim_titleII(),
        "tableII": datimII_DESS.to_dict('split'),
        'total_datimII': int(total_datimII_DESS),
        "titleIII": datim.datim_titleIII(),
        "tableIII": datimIII_DESS.to_dict('split'),
        'total_datimIII': int(total_datimIII_DESS),
        "titleIV": datim.datim_titleIV(),
        "tableIV": datimIV_DESS.to_dict('split'),
        'total_datimIV': int(total_datimIV_DESS)
    }
}
