import dataiku
import dataiku.spark as dkuspark
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

spark = SparkSession.builder.getOrCreate()
sql_ctx = SQLContext(spark.sparkContext)

# Cell 1 - partition stda: cd_stage STRING
stda = spark.createDataFrame([("F1", "1", 100.0), ("F2", "2", 200.0)],
                             StructType([StructField("id_fem", StringType()), StructField("cd_stage", StringType()),
                                         StructField("ead", DoubleType())]))
ds = dataiku.Dataset("demo_schema_spark")
ds.set_write_partition("stda")
dkuspark.write_with_schema(ds, stda)
print(ds.read_schema())                 # cd_stage: string

# Cell 2 - partition sgcp: cd_stage LONG (e.g. read from a source where it is numeric)
sgcp = spark.createDataFrame([("F3", 2, 300.0)],
                             StructType([StructField("id_fem", StringType()), StructField("cd_stage", LongType()),
                                         StructField("ead", DoubleType())]))
ds = dataiku.Dataset("demo_schema_spark")
ds.set_write_partition("sgcp")
dkuspark.write_with_schema(ds, sgcp)
print(ds.read_schema())                 # cd_stage: bigint for the WHOLE dataset

# Cell 3 - read stda: the error only appears at the first action (Spark is lazy)
ds = dataiku.Dataset("demo_schema_spark")
ds.add_read_partitions("stda")
df = dkuspark.get_dataframe(sql_ctx, ds)
df.printSchema()                        # cd_stage: long (the dataset schema, not the file's)
df.show()                               # expected: ClassCastException / unsupported conversion

# Cell 4 - fix: cast every partition to ONE fixed schema before writing
SCHEMA = StructType([StructField("id_fem", StringType()), StructField("cd_stage", StringType()),
                     StructField("ead", DoubleType())])

def conform(df):
    """Same columns, same order, same types: missing columns become typed NULL"""
    return df.select(*[(F.col(f.name) if f.name in df.columns else F.lit(None)).cast(f.dataType).alias(f.name)
                       for f in SCHEMA.fields])

for part, df in [("stda", stda), ("sgcp", sgcp)]:
    ds = dataiku.Dataset("demo_schema_spark")
    ds.set_write_partition(part)
    dkuspark.write_with_schema(ds, conform(df))       # same schema every time -> nothing to overwrite

for part in ["stda", "sgcp"]:
    d = dataiku.Dataset("demo_schema_spark"); d.add_read_partitions(part)
    dkuspark.get_dataframe(sql_ctx, d).show()         # both readable


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
path = "/tmp/demo_merge_schema"          # any HDFS path you can write to

# Two files in the SAME folder: the second one has an extra column
spark.createDataFrame([("F1", 100.0)], ["id_fem", "ead"]).write.mode("overwrite").parquet(path)
spark.createDataFrame([("F2", 200.0, 0.8)], ["id_fem", "ead", "tx_ltv_cur"]).write.mode("append").parquet(path)

spark.read.parquet(path).printSchema()
# may show only id_fem, ead: Spark takes the schema of ONE file -> tx_ltv_cur can disappear

spark.read.option("mergeSchema", "true").parquet(path).show()
# id_fem | ead   | tx_ltv_cur
# F1     | 100.0 | null        <- file without the column: NULL
# F2     | 200.0 | 0.8
