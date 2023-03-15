from .agyw import(
    AgywPrev,
    AgywPrevCommune
)
from ..utils import name_handler

datim = AgywPrev()

datimcommunes = []
for columns in datim.data_mastersheet.commune.unique():
    #globals()[f"ws_{name_handler(columns)}"] = wb.create_sheet(title=AgywPrevCommune(f"{columns}").who_am_i)
    globals()[f"datim_{name_handler(columns)}"] = AgywPrevCommune(f"{columns}")
    # worksheets.append(globals().get(f"ws_{name_handler(columns)}"))
    datimcommunes.append(globals().get(f"datim_{name_handler(columns)}"))

# PAP

total_datimI_PAP = sum([
    datim_PortauPrince.total_datimI,
    datim_Delmas.total_datimI,
    datim_Pétionville.total_datimI,
    datim_Tabarre.total_datimI,
    datim_Gressier.total_datimI,
    datim_Kenscoff.total_datimI,
    datim_Carrefour.total_datimI
])
total_datimII_PAP = sum([
    datim_PortauPrince.total_datimII,
    datim_Delmas.total_datimII,
    datim_Pétionville.total_datimII,
    datim_Tabarre.total_datimII,
    datim_Gressier.total_datimII,
    datim_Kenscoff.total_datimII,
    datim_Carrefour.total_datimII
])
total_datimIII_PAP = sum([
    datim_PortauPrince.total_datimIII,
    datim_Delmas.total_datimIII,
    datim_Pétionville.total_datimIII,
    datim_Tabarre.total_datimIII,
    datim_Gressier.total_datimIII,
    datim_Kenscoff.total_datimIII,
    datim_Carrefour.total_datimIII
])
total_datimIV_PAP = sum([
    datim_PortauPrince.total_datimIV,
    datim_Delmas.total_datimIV,
    datim_Pétionville.total_datimIV,
    datim_Tabarre.total_datimIV,
    datim_Gressier.total_datimIV,
    datim_Kenscoff.total_datimIV,
    datim_Carrefour.total_datimIV
])
total_datim_general_PAP = sum([
    datim_PortauPrince.total_datim_general,
    datim_Delmas.total_datim_general,
    datim_Pétionville.total_datim_general,
    datim_Tabarre.total_datim_general,
    datim_Gressier.total_datim_general,
    datim_Kenscoff.total_datim_general,
    datim_Carrefour.total_datim_general
])

datimI_PAP = datim_PortauPrince.datim_agyw_prevI().set_index("Age")\
    .add(datim_Delmas.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Pétionville.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Tabarre.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Gressier.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Kenscoff.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Carrefour.datim_agyw_prevI().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimII_PAP = datim_PortauPrince.datim_agyw_prevII().set_index("Age")\
    .add(datim_Delmas.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Pétionville.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Tabarre.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Gressier.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Kenscoff.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Carrefour.datim_agyw_prevII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIII_PAP = datim_PortauPrince.datim_agyw_prevIII().set_index("Age")\
    .add(datim_Delmas.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Pétionville.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Tabarre.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Gressier.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Kenscoff.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Carrefour.datim_agyw_prevIII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIV_PAP = datim_PortauPrince.datim_agyw_prevIV().set_index("Age")\
    .add(datim_Delmas.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Pétionville.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Tabarre.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Gressier.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Kenscoff.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Carrefour.datim_agyw_prevIV().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

# CAP
total_datimI_CAP = sum([
    datim_CapHaïtien.total_datimI,
    datim_PlaineduNord.total_datimI,
    datim_Milot.total_datimI,
    datim_Limonade.total_datimI,
    datim_QuartierMorin.total_datimI
])
total_datimII_CAP = sum([
    datim_CapHaïtien.total_datimII,
    datim_PlaineduNord.total_datimII,
    datim_Milot.total_datimII,
    datim_Limonade.total_datimII,
    datim_QuartierMorin.total_datimII
])
total_datimIII_CAP = sum([
    datim_CapHaïtien.total_datimIII,
    datim_PlaineduNord.total_datimIII,
    datim_Milot.total_datimIII,
    datim_Limonade.total_datimIII,
    datim_QuartierMorin.total_datimIII
])
total_datimIV_CAP = sum([
    datim_CapHaïtien.total_datimIV,
    datim_PlaineduNord.total_datimIV,
    datim_Milot.total_datimIV,
    datim_Limonade.total_datimIV,
    datim_QuartierMorin.total_datimIV
])
total_datim_general_CAP = sum([
    datim_CapHaïtien.total_datim_general,
    datim_PlaineduNord.total_datim_general,
    datim_Milot.total_datim_general,
    datim_Limonade.total_datim_general,
    datim_QuartierMorin.total_datim_general
])
datimI_CAP = datim_CapHaïtien.datim_agyw_prevI().set_index("Age")\
    .add(datim_PlaineduNord.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Milot.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Limonade.datim_agyw_prevI().set_index("Age"))\
    .add(datim_QuartierMorin.datim_agyw_prevI().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimII_CAP = datim_CapHaïtien.datim_agyw_prevII().set_index("Age")\
    .add(datim_PlaineduNord.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Milot.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Limonade.datim_agyw_prevII().set_index("Age"))\
    .add(datim_QuartierMorin.datim_agyw_prevII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIII_CAP = datim_CapHaïtien.datim_agyw_prevIII().set_index("Age")\
    .add(datim_PlaineduNord.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Milot.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Limonade.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_QuartierMorin.datim_agyw_prevIII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIV_CAP = datim_CapHaïtien.datim_agyw_prevIV().set_index("Age")\
    .add(datim_PlaineduNord.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Milot.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Limonade.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_QuartierMorin.datim_agyw_prevIV().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

# SM
total_datimI_SM = sum([
    datim_SaintMarc.total_datimI,
    datim_Verrettes.total_datimI,
    datim_La_Chapelle.total_datimI,
    datim_Montrouis.total_datimI,
    datim_Liancourt.total_datimI
])
total_datimII_SM = sum([
    datim_SaintMarc.total_datimII,
    datim_Verrettes.total_datimII,
    datim_La_Chapelle.total_datimII,
    datim_Montrouis.total_datimII,
    datim_Liancourt.total_datimII
])
total_datimIII_SM = sum([
    datim_SaintMarc.total_datimIII,
    datim_Verrettes.total_datimIII,
    datim_La_Chapelle.total_datimIII,
    datim_Montrouis.total_datimIII,
    datim_Liancourt.total_datimIII
])
total_datimIV_SM = sum([
    datim_SaintMarc.total_datimIV,
    datim_Verrettes.total_datimIV,
    datim_La_Chapelle.total_datimIV,
    datim_Montrouis.total_datimIV,
    datim_Liancourt.total_datimIV
])
total_datim_general_SM = sum([
    datim_SaintMarc.total_datim_general,
    datim_Verrettes.total_datim_general,
    datim_La_Chapelle.total_datim_general,
    datim_Montrouis.total_datim_general,
    datim_Liancourt.total_datim_general
])
datimI_SM = datim_SaintMarc.datim_agyw_prevI().set_index("Age")\
    .add(datim_Verrettes.datim_agyw_prevI().set_index("Age"))\
    .add(datim_La_Chapelle.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Montrouis.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Liancourt.datim_agyw_prevI().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimII_SM = datim_SaintMarc.datim_agyw_prevII().set_index("Age")\
    .add(datim_Verrettes.datim_agyw_prevII().set_index("Age"))\
    .add(datim_La_Chapelle.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Montrouis.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Liancourt.datim_agyw_prevII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIII_SM = datim_SaintMarc.datim_agyw_prevIII().set_index("Age")\
    .add(datim_Verrettes.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_La_Chapelle.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Montrouis.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Liancourt.datim_agyw_prevIII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIV_SM = datim_SaintMarc.datim_agyw_prevIV().set_index("Age")\
    .add(datim_Verrettes.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_La_Chapelle.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Montrouis.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Liancourt.datim_agyw_prevIV().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)
# DESS
total_datimI_DESS = sum([
    datim_Dessalines.total_datimI,
    datim_Desdunes.total_datimI,
    datim_Grande_Saline.total_datimI,
    datim_Petite_Rivière_de_lArtibonite.total_datimI
])
total_datimII_DESS = sum([
    datim_Dessalines.total_datimII,
    datim_Desdunes.total_datimII,
    datim_Grande_Saline.total_datimII,
    datim_Petite_Rivière_de_lArtibonite.total_datimII
])
total_datimIII_DESS = sum([
    datim_Dessalines.total_datimIII,
    datim_Desdunes.total_datimIII,
    datim_Grande_Saline.total_datimIII,
    datim_Petite_Rivière_de_lArtibonite.total_datimIII
])
total_datimIV_DESS = sum([
    datim_Dessalines.total_datimIV,
    datim_Desdunes.total_datimIV,
    datim_Grande_Saline.total_datimIV,
    datim_Petite_Rivière_de_lArtibonite.total_datimIV
])
total_datim_general_DESS = sum([
    datim_Dessalines.total_datim_general,
    datim_Desdunes.total_datim_general,
    datim_Grande_Saline.total_datim_general,
    datim_Petite_Rivière_de_lArtibonite.total_datim_general
])
datimI_DESS = datim_Dessalines.datim_agyw_prevI().set_index("Age")\
    .add(datim_Desdunes.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Grande_Saline.datim_agyw_prevI().set_index("Age"))\
    .add(datim_Petite_Rivière_de_lArtibonite.datim_agyw_prevI().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimII_DESS = datim_Dessalines.datim_agyw_prevII().set_index("Age")\
    .add(datim_Desdunes.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Grande_Saline.datim_agyw_prevII().set_index("Age"))\
    .add(datim_Petite_Rivière_de_lArtibonite.datim_agyw_prevII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIII_DESS = datim_Dessalines.datim_agyw_prevIII().set_index("Age")\
    .add(datim_Desdunes.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Grande_Saline.datim_agyw_prevIII().set_index("Age"))\
    .add(datim_Petite_Rivière_de_lArtibonite.datim_agyw_prevIII().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

datimIV_DESS = datim_Dessalines.datim_agyw_prevIV().set_index("Age")\
    .add(datim_Desdunes.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Grande_Saline.datim_agyw_prevIV().set_index("Age"))\
    .add(datim_Petite_Rivière_de_lArtibonite.datim_agyw_prevIV().set_index("Age"))\
    .reset_index().rename_axis(None, axis=1)

