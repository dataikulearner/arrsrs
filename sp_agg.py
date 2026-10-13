# DSS PySpark recipe: SP_agg for SRS Arrimage (CCIRC SP_agg specification)
#   inputs  : axes_period_par, folder parameters_folder (uds_fac_arrime is read by the code, not a recipe input)
#   output  : SP_agg, partitioned by scope | period_run | id_set_parameters | period
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

CONFIG_FOLDER = "parameters_folder"
PARAMS_PATH = "flows/arrimage_params.json"     # path inside CONFIG_FOLDER
REGISTRY_PATH = "column_registry.json"          # path inside CONFIG_FOLDER
PAR_DATASET = "axes_period_par"                 # name of the task dataset in the Flow
TABLE_PATH = "uds_fac_arrime"                   # dataset name in this project (DETECT_SRS_ARRIMAGE_DEV)
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
    """Rows of one (period, axes): ONE groupBy for all target variables, then one row per target
    (same rows as one groupBy per target, but one shuffle instead of len(targets))"""
    weight = run["weight_vars"]["t_agg5"]
    aggs = [F.sum(col).cast("double").alias(alias) for alias, col in run["segment_vars"].items()]
    aggs.append(F.sum(weight).alias("w_sum"))
    structs = []
    for i, t in enumerate(targets):
        p = "_{}_".format(i)
        aggs += [AGG_FUNCS[fn](t).cast("long" if fn == "count" else "double").alias(p + alias)
                 for alias, fn in run["target_agg_functions"].items()]                 # seg_agg1, t_agg1..4
        aggs.append(F.sum(t + "_x_weight").alias(p + "tw_sum"))
        t_agg5 = F.lit(None)                    # only for the targets detected on agg5 (rates, maturity)
        if "agg5" in run["target_detection_mapping"].get(t, []):
            t_agg5 = F.when(F.abs("w_sum") > WEIGHT_SUM_EPSILON, F.col(p + "tw_sum") / F.col("w_sum"))
        structs.append(F.struct(F.lit(t).alias("t_variable_clone"),
                                *[F.col(p + alias).alias(alias) for alias in run["target_agg_functions"]],
                                t_agg5.cast("double").alias("t_agg5")))
    return (df.groupBy("seg_axes_value", pp.UDS_VERSION_COL).agg(*aggs)
            .withColumn("_row", F.explode(F.array(*structs)))
            .select("seg_axes_value", pp.UDS_VERSION_COL, *run["segment_vars"], "_row.*")
            .withColumn("seg_axes", F.lit("|".join(axes))))


def compute_period(spark, run, registry, period, period_run, axes_list, errors):
    """All axes of one (run, period); result cached and materialised (the source is evicted afterwards)"""
    kwargs = {k: run[k] for k in pp.DEFAULTS if k in run}
    kwargs["columns"] = registry
    # Columns that must exist: axes, segment and weight variables of this run. A target absent from
    # the partition is skipped with a warning (CCIRC spec: keep only the targets present)
    kwargs["used_columns"] = sorted({a for axes in run["axes_str"].split("/") for a in axes.split("___")}
                                    - {pp.ENTITY_COL}
                                    | set(run["segment_vars"].values()) | set(run["weight_vars"].values()))
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
# Aggregates are small: 200 shuffle partitions per groupBy made tens of thousands of tiny tasks
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "50")
folder = dataiku.Folder(CONFIG_FOLDER)
params = read_json(folder, PARAMS_PATH)
ap.validate_params(params)
registry = read_json(folder, REGISTRY_PATH)["arrimage"]
# Output partition being built: scope | period_run | id_set_parameters | period (one DSS activity per period)
dst = {dim: dataiku.dku_flow_variables.get("DKU_DST_" + dim) for dim in ["scope", "period_run", "period"]}
scope = dst["scope"] or dataiku.get_custom_variables().get("scope_run")
if scope not in params[ap.SCOPES_KEY]:
    raise ValueError("scope = {!r}, expected one of {}".format(scope, sorted(params[ap.SCOPES_KEY])))

# 2. Tasks of this partition: every version of the scope, for the period being built
raw = dataiku.Dataset(PAR_DATASET).get_dataframe()
print("{}: {} rows read, by scope/period_run/version/period:\n{}".format(
    PAR_DATASET, len(raw), raw.groupby(["scope_run_clone", "period_run_clone", "uds_version_clone",
                                        "period_clone"]).size().to_string()))
norm = lambda col: raw[col].astype(str).str.strip()                        # noqa: E731
par = raw.assign(scope_run_clone=norm("scope_run_clone"), uds_version_clone=norm("uds_version_clone"),
                 period_clone=norm("period_clone").str.upper(), period_run_clone=norm("period_run_clone").str.upper())
filters = [("scope_run_clone", scope),
           ("period_clone", dst["period"] and dst["period"].strip().upper()),
           ("period_run_clone", dst["period_run"] and dst["period_run"].strip().upper())]
for col, value in filters:
    if value:
        par = par[par[col] == value]
        if par.empty:
            raise ValueError("{}: no row with {} = {!r} (values: {})".format(
                PAR_DATASET, col, value, sorted(set(norm(col)))))

# Skip the (version, period) without source partition, e.g. stda v3x1 has no 2025q4: that period is
# computed with v2x1 only. Checked here too, so that a stale axes_period_par cannot stop the build
partitions = [str(pid).strip().lower() for pid in pp._list_partitions(TABLE_PATH)]
print("{} partitions listed in {}, e.g. {}".format(len(partitions), TABLE_PATH, sorted(partitions)[-3:]))
if not partitions:
    raise ValueError("No partition listed in {}".format(TABLE_PATH))


def has_partition(version, period):
    versions = pp.versions_of_period(partitions, period)
    if version == pp.LATEST:
        return any(pp.VERSION_RE.match(v) for v in versions)
    return version.lower() in versions


tasks = sorted(set(zip(par["uds_version_clone"], par["period_clone"])))
missing = [(v, p) for v, p in tasks if not has_partition(v, p)]
for version, period in missing:
    print("WARNING {} {}: no partition {}|{} (versions of {}: {})".format(
        scope, version, period.lower(), version, period, sorted(pp.versions_of_period(partitions, period))))
par = par[[(v, p) not in missing for v, p in zip(par["uds_version_clone"], par["period_clone"])]]
if par.empty:
    raise ValueError("No task with a source partition in {} for partition {}".format(PAR_DATASET, dst))
print("Partition {}: {} tasks, versions {}".format(dst, len(par), sorted(set(par["uds_version_clone"]))))

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

    # 4. Write the partition being built
    dkuspark.write_with_schema(dataiku.Dataset("SP_agg"), sp_agg.coalesce(1))   # one file per partition
finally:
    for res in results:
        res.unpersist()
    pp.clear_cache()
