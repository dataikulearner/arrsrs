# -*- coding: utf-8 -*-
"""
"""

import dataiku
import dataiku.spark as dkuspark
import pandas as pd
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F

# --------------------------------------------------------------------------
# Parameters
# --------------------------------------------------------------------------
DATASET = "RDS_SRS_CAPITAL.uds_fac_arrime"  # "uds_fac_arrime" if run inside that project
VERSIONS = ["v2x1", "v3x1"]
QUICK_TEST = True  # True: only the 2 latest partitions; False: all (about 110 GB)

# Columns requested in "STDA Business Requirements for DETECT usage on uds_fac_arrime.xlsx"
NEEDED = [
    # measures
    "mt_cal_ead_tot", "mt_cal_rwa_tot", "mt_cal_expo_tot", "nb_maturite_irba",
    "tx_lgd_avant_flex", "cd_flex_type_id_lgd", "tx_lgd_irba_cal_def", "tx_edf_1an_irba",
    # axes
    "cd_pole_pmas", "cd_metier_pmas", "cd_pays_business_cal", "cd_site_comptable",
    "cd_tp_approche", "cd_sous_type_risque", "cd_type_encours_arc", "cd_type_encours",
    "cd_cec_fac_std", "cd_cec_fac_irba", "cd_sous_classe_expo", "cd_ss_cl_expo_std",
    "id_modele_notation",
]
SUM_COLS = ["mt_cal_ead_tot", "mt_cal_rwa_tot", "mt_cal_expo_tot"]
DISTINCT_COLS = ["cd_site_comptable", "cd_pole_pmas", "cd_tp_approche"]

# --------------------------------------------------------------------------
# Spark session
# --------------------------------------------------------------------------
spark = SparkSession.builder.appName("explore_uds_fac_arrime").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)


# --------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------
def list_partitions(dataset_name):
    """Return a pandas DataFrame: dt_closing, uds_version, partition id."""
    parts = dataiku.Dataset(dataset_name).list_partitions()
    rows = [p.split("|") + [p] for p in parts]
    return (pd.DataFrame(rows, columns=["dt_closing", "uds_version", "partition"])
              .sort_values(["uds_version", "dt_closing"])
              .reset_index(drop=True))


def read_partition(dataset_name, partition):
    """Read one partition; lower-case columns; add partition dimensions as columns."""
    dt_closing, uds_version = partition.split("|")
    ds = dataiku.Dataset(dataset_name)
    ds.add_read_partitions(partition)
    df = dkuspark.get_dataframe(sqlContext, ds)
    return (df.toDF(*[c.lower() for c in df.columns])
              .withColumn("dt_closing", F.lit(dt_closing))
              .withColumn("uds_version", F.lit(uds_version)))


def check_columns(df, needed):
    """Return the needed columns that are missing from df."""
    return [c for c in needed if c not in df.columns]


def profile_partition(df, sum_cols, distinct_cols):
    """One-row summary: rows, EAD null / zero / positive, sums, distinct counts."""
    aggs = [F.count("*").alias("n_rows")]
    if "mt_cal_ead_tot" in df.columns:
        ead = F.col("mt_cal_ead_tot").cast("double")
        aggs += [F.sum(ead.isNull().cast("int")).alias("n_ead_null"),
                 F.sum((ead == 0).cast("int")).alias("n_ead_zero"),
                 F.sum((ead > 0).cast("int")).alias("n_ead_pos")]
    aggs += [F.sum(F.col(c).cast("double")).alias("sum_" + c)
             for c in sum_cols if c in df.columns]
    aggs += [F.countDistinct(c).alias("nd_" + c)
             for c in distinct_cols if c in df.columns]
    return df.agg(*aggs).toPandas().iloc[0].to_dict()


# --------------------------------------------------------------------------
# Q1. Partitions available
# --------------------------------------------------------------------------
parts = list_partitions(DATASET)
print(len(parts), "partitions")
grid = (parts.assign(x="x")
             .pivot(index="dt_closing", columns="uds_version", values="x")
             .fillna(""))
print(grid.to_string())

todo = parts[parts.uds_version.isin(VERSIONS)].sort_values(["dt_closing", "uds_version"])
print(todo.groupby("uds_version").dt_closing.agg(["count", "min", "max"]))

if QUICK_TEST:
    todo = todo.tail(2)

# --------------------------------------------------------------------------
# Q2, Q3, Q4. Profile each partition
# --------------------------------------------------------------------------
results = []
for _, p in todo.iterrows():
    df = read_partition(DATASET, p.partition)
    row = {"dt_closing": p.dt_closing,
           "uds_version": p.uds_version,
           "n_cols": len(df.columns),
           "missing": ",".join(check_columns(df, NEEDED))}
    row.update(profile_partition(df, SUM_COLS, DISTINCT_COLS))
    results.append(row)
    print("done", p.partition)

res = pd.DataFrame(results).sort_values(["uds_version", "dt_closing"])
pd.options.display.float_format = "{:,.0f}".format
pd.options.display.width = 250

print("\n=== Q2. Missing requested columns per partition ===")
print(res[["uds_version", "dt_closing", "n_cols", "missing"]].to_string(index=False))

print("\n=== Q3 / Q4. Volume and exposure per partition ===")
print(res.drop(columns=["missing"]).to_string(index=False))

# --------------------------------------------------------------------------
# Q3. Quarter-on-quarter change (%), per version
# --------------------------------------------------------------------------
chg_cols = [c for c in ["n_rows", "n_cols", "n_ead_pos",
                        "sum_mt_cal_ead_tot", "sum_mt_cal_rwa_tot"] if c in res.columns]
pct = (res.set_index(["uds_version", "dt_closing"])[chg_cols]
          .astype(float)
          .groupby(level=0).pct_change().mul(100).round(1))
print("\n=== Q3. Quarter-on-quarter change (%) ===")
print(pct.to_string(float_format="{:,.1f}".format))
