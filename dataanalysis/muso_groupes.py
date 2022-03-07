from curses import def_shell_mode
import pandas as pd
from db.muso_group import MusoGroup
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StringType

class MusoGroupes:
    def __init__(self, cc_groupes:dict,hi_groupes) -> None:
        self.cc_groupes = cc_groupes
        self.hi_groupes = hi_groupes
        # self.spark = SparkSession.builder.appName("Caris").master("local").config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

        pass
    def groups_not_on_hiv(self):
        # sp_cc_groupes = self.spark.createDataFrame(self.cc_groupes)
        # sp_hi_groupes = self.spark.createDataFrame(self.hi_groupes)
        # print(sp_cc_groupes.show(4))
        # sp_cc_groupes.createOrReplaceTempView("cc_groupes")
        # sp_hi_groupes.createOrReplaceTempView("hi_groupes")
        # sql = "select cc_groupes.* from cc_groupes left join hi_groupes on cc_groupes.office = hi_groupes.office and cc_groupes.code=hi_groupes.code where hi_groupes.id is null"
        # df = self.spark.sql(sql)
        # print(df.show())
        # return df.toPandas().to_dict("records")
        df = pd.DataFrame(self.cc_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df = df[df["external_id"].isna()]
        return df.to_dict("records")
    def groups_not_on_hiv_df(self):
        df = pd.DataFrame(self.cc_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df = df[df["external_id"].isna()]
        return df
    def groups_on_hiv(self):
        df = pd.DataFrame(self.hi_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df = df[df["external_id"].notna()]
        return df.to_dict("records")
    def groups_duplicated_on_cc(self):
        df = pd.DataFrame(self.cc_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df_duplicated = df[df[["office_name","code"]].duplicated(keep=False)]
        df_duplicated=df_duplicated[df_duplicated["external_id"].isna()]
        return df_duplicated.to_dict("records")
    def groups_duplicated_on_cc_df(self):
        df = pd.DataFrame(self.cc_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df_duplicated = df[df[["office_name","code"]].duplicated(keep=False)]
        df_duplicated=df_duplicated[df_duplicated["external_id"].isna()]
        return df_duplicated
    def max_group_code_by_office(self):
        df = pd.DataFrame(self.cc_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df["code"]=pd.to_numeric(df["code"],errors="coerce")
        df_pivot = df.pivot_table(index=["office_name"],values=["code"],aggfunc=max)
        df_pivot["office_name"] = df_pivot.index
        return df_pivot.to_dict("records")
    def max_group_code_by_office_df(self):
        df = pd.DataFrame(self.cc_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df["code"]=pd.to_numeric(df["code"],errors="coerce")
        df_pivot = df.pivot_table(index=["office_name"],values=["code"],aggfunc=max)
        df_pivot["office_name"] = df_pivot.index
        return df_pivot
    def groupes_cc_with_null_code(self):
        df = pd.DataFrame(self.cc_groupes)
        df["external_id"] = pd.to_numeric(df["external_id"],errors="coerce")
        df["code"]=pd.to_numeric(df["code"],errors="coerce")
        df_null_code = df[df["code"].isna()]
        return df_null_code.to_dict("records")
    def generate_code_for_group_without_code(self):
        group_with_null_code = self.groupes_cc_with_null_code()
        max_group_code_by_office = self.max_group_code_by_office()
        groups = []
        for office_name in max_group_code_by_office:
            i=1
            for group in group_with_null_code:
                if group["office_name"] == office_name["office_name"]:
                    group["code"] = office_name["code"] + i
                    groups.append(group)
                    i=i+1
        return groups

    def generate_code_for_groupes_duplicated_on_cc(self):
        groupes_duplicated_on_cc = self.groups_duplicated_on_cc()
        max_group_code_by_office = self.max_group_code_by_office()
        groups = []
        for office_name in max_group_code_by_office:
            i=1
            for group in groupes_duplicated_on_cc:
                if group["office_name"] == office_name["office_name"]:
                    group["code"] = office_name["code"] + i
                    groups.append(group)
                    i=i+1
        return groups

    def insert_groupes_not_on_hiv(self):
        groups = self.groups_not_on_hiv()
        if len(groups)>0:
            self.insert_groupes_cc_to_hiv(groups)
        else:
            print("no groupes to insert")

    def insert_groupes_without_code_cc(self):
        groupes = self.generate_code_for_group_without_code()
        if len(groupes)>0:
            self.insert_groupes_cc_to_hiv(groupes)
        else:
            print("No groupes without code on cc")

    def insert_groupes_duplicated_on_cc(self):
        groupes = self.generate_code_for_groupes_duplicated_on_cc()
        if len(groupes)>0:
            self.insert_groupes_cc_to_hiv(groupes)
        else:
            print("No groupes duplicated on cc")

    def insert_groupes_cc_to_hiv(self,groupes):
        df_groupes = pd.DataFrame(groupes)
        print(df_groupes.head())
        df_hi_groupes = pd.DataFrame(self.hi_groupes)
        df_hi_groupes = df_hi_groupes[["office","code"]]
        # df_groupes.drop(columns=["office"],inplace=True)
        df_groupes["office"]=df_groupes["office_name"]
        df_groupes["name"]=df_groupes["case_name"]
        df_groupes["localite"]=df_groupes["section"]
        df_groupes["localite_name"]=df_groupes["section_name"]
        df_groupes.drop(columns=["office_name"],inplace=True)
        df_groupes["code"]=pd.to_numeric(df_groupes["code"],errors="coerce")
        print(df_hi_groupes.head())
        df_groupes = pd.merge(df_groupes,df_hi_groupes,on=["office","code"],how="left",suffixes=("","_hi"))
        print(df_groupes.head())
        df_groupes["code_hi"] = pd.to_numeric(df_groupes["code_hi"],errors="coerce")
        df_groupes = df_groupes[df_groupes["code_hi"].isna()]
        __columns = list(pd.DataFrame(self.hi_groupes).columns)
        __columns.remove("id")
        __columns.remove("created_at")
        __columns.remove("updated_at")
        __columns.remove("created_by")
        __columns.remove("updated_by")
        df = df_groupes[__columns]
        df["case_id"]=df_groupes["case_id"]
        muso_group = MusoGroup()
        muso_group.insert_groupes(df)


    def get_hi_groupes_without_case_id(self):
        muso_group = MusoGroup()
        df = pd.DataFrame(muso_group.get_groupes_without_case_id())


    def update_groupes_case_id(self):
        groupes = self.groups_on_hiv()
        try:
            muso_group = MusoGroup()
            muso_group.update_groupes_case_id(groupes)
        except Exception as e:
            print(e)