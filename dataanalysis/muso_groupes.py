import pandas as pd
from db.muso_group import MusoGroup
class MusoGroupes:
    def __init__(self, cc_groupes:dict,hi_groupes) -> None:
        self.cc_groupes = cc_groupes
        self.hi_groupes = hi_groupes
        pass
    def groups_not_on_hiv(self):
        return self.hi_groupes
        # return [group for group in self.cc_groupes if  not(group.office==self.hi_groupes.office and group.code==self.hi_groupes.code) ]