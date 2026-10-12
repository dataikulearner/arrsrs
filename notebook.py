import dataiku, dataiku.spark as dkuspark
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession.builder.getOrCreate()

# 1. Như code đang đọc
ds = dataiku.Dataset("uds_fac_arrime", ignore_flow=True)
ds.add_read_partitions("2026q1|v2x2")
df = dkuspark.get_dataframe(SQLContext(spark.sparkContext), ds)
print("dss  :", [repr(c) for c in df.columns if "LTV" in c.upper()])

# 2. Đọc thẳng parquet, gộp schema của mọi file
path = ("/Projects/UC_RDS/UC_RDS_DEV/uz_UC_RDS_DEV.db/data_preparation/srs_capital/arrimage/"
        "uds_fac_arrime/dt_closing=2026q1/uds_version=v2x2")
df2 = spark.read.option("mergeSchema", "true").parquet(path)
print("merge:", [repr(c) for c in df2.columns if "LTV" in c.upper()])
