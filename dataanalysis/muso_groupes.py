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
        df = df[df["extern_id"]==None]
        return df.to_dict("records")
    def groups_not_on_hiv_df(self):
        df = pd.DataFrame(self.cc_groupes)
        df = df[df["extern_id"]==None]
        return df