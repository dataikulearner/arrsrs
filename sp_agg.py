# DSS PySpark recipe: SP_agg for SRS Arrimage (CCIRC SP_agg specification)
#   inputs  : RDS_SRS_CAPITAL.uds_fac_arrime, axes_periods_par, folder detect_config
#   output  : SP_agg, partitioned by scope_run (DSS variable "scope_run" = stda | sgcp)
#   one row per (segment x target variable x period x run)
import json
from functools import reduce

import dataiku
import dataiku.spark as dkuspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flows.arrimage import params as ap
from flows.arrimage import pre_processing as pp
from utils.logger import create_logger

logger = create_logger(__name__)

TABLE_PATH = "RDS_SRS_CAPITAL.uds_fac_arrime"
WEIGHT_SUM_EPSILON = 1.0
AGG_FUNCS = {"count": F.count, "avg": F.avg, "min": F.min, "max": F.max, "sum": F.sum}
LITERALS = ["table_path", "period_clone", "period_run_clone", "scope_clone", "id_set_parameters_clone",
            "uds_version_clone"]
KEY = ["seg_axes", "seg_axes_value", "t_variable_clone", "period_clone", "scope_clone", "id_set_parameters_clone"]


def output_columns(run):
    """seg_axes, seg_axes_value, seg_agg* | t_variable_clone, t_agg* | literals"""
    aliases = list(run["target_agg_functions"]) + list(run["segment_vars"]) + list(run["weight_vars"])
    by_num = lambda prefix: sorted((a for a in aliases if a.startswith(prefix)), key=lambda a: int(a.split("agg")[1]))
    return ["seg_axes", "seg_axes_value"] + by_num("seg_") + ["t_variable_clone"] + by_num("t_") + LITERALS


def aggregate_axes(df, axes, targets, run):
    """Rows of one (period, axes): one groupBy per target variable"""
    weight = run["weight_vars"]["t_agg5"]
    rows = []
    for t in targets:
        aggs = [AGG_FUNCS[fn](t).cast("long" if fn == "count" else "double").alias(alias)
                for alias, fn in run["target_agg_functions"].items()]                 # seg_agg1, t_agg1..4
        aggs += [F.sum(col).cast("double").alias(alias) for alias, col in run["segment_vars"].items()]
        aggs += [F.sum(t + "_x_weight").alias("tw_sum"), F.sum(weight).alias("w_sum")]
        t_agg5 = F.lit(None)                    # only for the targets detected on agg5 (rates, maturity)
        if "agg5" in run["target_detection_mapping"].get(t, []):
            t_agg5 = F.when(F.abs("w_sum") > WEIGHT_SUM_EPSILON, F.col("tw_sum") / F.col("w_sum"))
        rows.append(df.groupBy("seg_axes_value", pp.UDS_VERSION_COL).agg(*aggs)
                    .withColumn("t_agg5", t_agg5.cast("double")).drop("tw_sum", "w_sum")
                    .withColumn("seg_axes", F.lit("|".join(axes)))
                    .withColumn("t_variable_clone", F.lit(t)))
    return reduce(DataFrame.unionByName, rows)


def compute_period(spark, run, registry, period, period_run, axes_list, errors):
    """All axes of one (run, period); result cached and materialised (the source is evicted afterwards)"""
    kwargs = {k: run[k] for k in pp.DEFAULTS if k in run}
    kwargs["columns"] = registry
    df = pp.enrich_data_period_arrimage(spark, TABLE_PATH, period, **kwargs)

    targets = [t for t in run["target_variables"] if t in df.columns]
    for t in set(run["target_variables"]) - set(targets):
        logger.warning("{} {}: target {} missing, skipped".format(run["id_set_params"], period, t))
    weight = run["weight_vars"]["t_agg5"]
    df = df.select("*", *[(F.col(t) * F.col(weight)).alias(t + "_x_weight") for t in targets]).cache()

    parts = []
    for axes_str in axes_list:
        axes = axes_str.split("___")
        missing = [a for a in axes if a not in df.columns]
        if missing:
            errors.append("{} {} {}: missing axes {}".format(run["id_set_params"], period, axes_str, missing))
            continue
        try:
            seg_value = F.concat_ws("|", *[F.coalesce(F.trim(F.col(a).cast("string")), F.lit("NULL")) for a in axes])
            parts.append(aggregate_axes(df.withColumn("seg_axes_value", seg_value), axes, targets, run))
        except Exception as exc:                                   # noqa: BLE001 - reported at the end
            errors.append("{} {} {}: {!r}".format(run["id_set_params"], period, axes_str, exc))

    if not parts:
        df.unpersist()
        return None
    res = (reduce(DataFrame.unionByName, parts)
           .withColumnRenamed(pp.UDS_VERSION_COL, "uds_version_clone")
           .withColumn("table_path", F.lit(TABLE_PATH))
           .withColumn("period_clone", F.lit(period))
           .withColumn("period_run_clone", F.lit(period_run))
           .withColumn("scope_clone", F.lit(run["scope_run"]))
           .withColumn("id_set_parameters_clone", F.lit(run["id_set_params"]))
           .select(*output_columns(run))
           .cache())
    rows = dict(res.groupBy("seg_axes").count().collect())        # materialises the cache
    df.unpersist()
    pp.clear_cache()
    for axes_str in axes_list:
        if not rows.get(axes_str.replace("___", "|")):
            errors.append("{} {} {}: no row".format(run["id_set_params"], period, axes_str))
    logger.info("{} {}: {} rows".format(run["id_set_params"], period, sum(rows.values())))
    return res


def read_json(folder, path):
    with folder.get_download_stream(path) as stream:
        return json.load(stream)


# 1. Configuration and scope (DSS variable)
spark = SparkSession.builder.getOrCreate()
folder = dataiku.Folder("detect_config")
params = read_json(folder, "flows/arrimage_params.json")
ap.validate_params(params)
registry = read_json(folder, "column_registry.json")["arrimage"]
scope = dataiku.get_custom_variables().get("scope_run")
if scope not in params[ap.SCOPES_KEY]:
    raise ValueError("DSS variable scope_run = {!r}, expected one of {}".format(scope, sorted(params[ap.SCOPES_KEY])))
target_partition = dataiku.dku_flow_variables.get("DKU_DST_scope_run")
if target_partition not in (None, scope):
    raise ValueError("Building partition {} with scope_run = {}".format(target_partition, scope))

# 2. Tasks of the scope, one source partition per (version, period)
par = dataiku.Dataset("axes_periods_par").get_dataframe()
par = par[par["scope_run_clone"] == scope]
if par.empty:
    raise ValueError("No task for scope {}".format(scope))

results, errors = [], []
try:
    for (version, period, period_run), group in par.groupby(["uds_version_clone", "period_clone", "period_run_clone"]):
        run = ap.resolve_run_params(params, scope, version)
        res = compute_period(spark, run, registry, period, period_run, list(group["axe_clone"]), errors)
        if res is not None:
            results.append(res)

    # 3. Fail if a pair failed or an axes produced no row: nothing written
    if errors:
        raise ValueError("SP_agg {}: {} error(s): {}".format(scope, len(errors), errors))
    sp_agg = reduce(DataFrame.unionByName, results)
    sp_agg = sp_agg.toDF(*[c.lower() for c in sp_agg.columns])
    duplicates = sp_agg.groupBy(*KEY).count().filter("count > 1").limit(5).collect()
    if duplicates:
        raise ValueError("SP_agg {}: duplicate keys {}".format(scope, duplicates))

    # 4. Write the scope_run partition
    dkuspark.write_with_schema(dataiku.Dataset("SP_agg"), sp_agg)
finally:
    for res in results:
        res.unpersist()
    pp.clear_cache()
