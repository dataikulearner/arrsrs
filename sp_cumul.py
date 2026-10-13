# DSS PySpark recipe: SP_toplines_cumul for SRS Arrimage (CCIRC SP_toplines_cumul specification)
#   inputs  : axes_period_par, folder parameters_folder (uds_fac_arrime is read by the code, not a recipe input)
#   output  : SP_toplines_cumul, one row per (facility x period x version), WIDE: one column per target variable
#   Same enrichment as SP_agg (pre_processing.enrich_data_period_arrimage) => same axis values ("NULL" for empty axes)
#   Rows kept: thresholds_topline, strict ">" (EAD > 5000 or RWA > 5000): negative or NULL exposures are dropped,
#   on purpose (top lines = large positive contributors)
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
PARAMS_PATH = "arrimage_params.json"     # path inside CONFIG_FOLDER
REGISTRY_PATH = "column_registry.json"          # path inside CONFIG_FOLDER
PAR_DATASET = "axes_period_par"                 # same task dataset as SP_agg
OUTPUT_DATASET = "SP_toplines_cumul"
TABLE_PATH = "uds_fac_arrime"
COUNTERPARTY_COL = "id_tiers"                   # counterparty id (id_fem = facility id)
NB_OUTPUT_FILES = 4                             # files per output partition
LITERALS = ["table_path", "period_clone", "period_run_clone", "scope_clone", "id_set_parameters_clone",
            "uds_version_clone"]


def business_axes(run):
    """Union of the axes of axes_str, in order of appearance, entity first"""
    axes = []
    for combination in run["axes_str"].split("/"):
        for axis in combination.split("___"):
            if axis not in axes:
                axes.append(axis)
    return axes


def target_columns(run):
    """Target variables (one column each) + the line materiality column"""
    targets = list(run["target_variables"])
    if run["line_materiality_col"] not in targets:
        targets.append(run["line_materiality_col"])
    return targets


def relevant_columns(run):
    """id_cols + line_fac_col + targets + business axes + literals (CCIRC relevant_cols)"""
    cols = []
    for col in (list(run["line_id_cols"]) + [run["line_fac_col"]] + target_columns(run) + business_axes(run)
                + LITERALS):
        if col not in cols:
            cols.append(col)
    return cols


def topline_process_arrimage(df):
    """Flow specific hook (Spark version of pre_processing.topline_process_arrimage):
    trim string columns and build ctpr_id_generic = id_tiers before the first "_" (get_ctpr_id_generic), NULL if empty"""
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    ctpr = F.col(COUNTERPARTY_COL)
    return df.withColumn("ctpr_id_generic",
                         F.when(ctpr.isNotNull() & (ctpr != ""), F.split(ctpr, "_").getItem(0)))


def toplines_period(spark, run, registry, period, period_run):
    """Top lines of one (run, period), with every relevant column (absent ones as typed NULL)"""
    thresholds = run["thresholds_topline"]
    kwargs = {k: run[k] for k in pp.DEFAULTS if k in run}
    kwargs["columns"] = registry
    # Must exist: axes (segment query of post-processing), threshold variables, counterparty id
    kwargs["used_columns"] = sorted(set(business_axes(run)) - {pp.ENTITY_COL} | set(thresholds) | {COUNTERPARTY_COL})
    df = pp.enrich_data_period_arrimage(spark, TABLE_PATH, period, **kwargs)

    # Filter AFTER enrichment: keep a line if ANY threshold is exceeded (strict ">")
    condition = reduce(lambda a, b: a | b, [F.col(var) > float(thr) for var, thr in thresholds.items()])
    df = topline_process_arrimage(df.filter(condition))

    # Absent columns (older versions, e.g. tx_ltv_cur or lb_raison_sociale): typed NULL, same schema for every period
    targets = target_columns(run)
    for col in relevant_columns(run):
        if col not in df.columns and col not in LITERALS:
            logger.warning("{} {}: column {} absent, NULL".format(run["id_set_params"], period, col))
            df = df.withColumn(col, F.lit(None).cast("double" if col in targets else "string"))
    return (df.withColumnRenamed(pp.UDS_VERSION_COL, "uds_version_clone")
              .withColumn("table_path", F.lit(TABLE_PATH))
              .withColumn("period_clone", F.lit(period))
              .withColumn("period_run_clone", F.lit(period_run))
              .withColumn("scope_clone", F.lit(run["scope_run"]))
              .withColumn("id_set_parameters_clone", F.lit(run["id_set_params"]))
              .select(*relevant_columns(run)))


def read_json(folder, path):
    with folder.get_download_stream(path) as stream:
        return json.load(stream)


# 1. Configuration and partition being built
spark = SparkSession.builder.getOrCreate()
folder = dataiku.Folder(CONFIG_FOLDER)
params = read_json(folder, PARAMS_PATH)
ap.validate_params(params)
registry = read_json(folder, REGISTRY_PATH)["arrimage"]
dst = {dim: dataiku.dku_flow_variables.get("DKU_DST_" + dim) for dim in ["scope", "period_run", "period"]}
scope = dst["scope"] or dataiku.get_custom_variables().get("scope_run")
if scope not in params[ap.SCOPES_KEY]:
    raise ValueError("scope = {!r}, expected one of {}".format(scope, sorted(params[ap.SCOPES_KEY])))

# 2. (version, period) of the scope: same tasks as SP_agg, one per source partition
raw = dataiku.Dataset(PAR_DATASET).get_dataframe()
norm = lambda col: raw[col].astype(str).str.strip()                        # noqa: E731
par = raw.assign(scope_run_clone=norm("scope_run_clone"), uds_version_clone=norm("uds_version_clone"),
                 period_clone=norm("period_clone").str.upper(), period_run_clone=norm("period_run_clone").str.upper())
filters = [("scope_run_clone", scope),
           ("period_clone", dst["period"] and dst["period"].strip().upper()),        # only if partitioned by period
           ("period_run_clone", dst["period_run"] and dst["period_run"].strip().upper())]
for col, value in filters:
    if value:
        par = par[par[col] == value]
        if par.empty:
            raise ValueError("{}: no row with {} = {!r} (values: {})".format(PAR_DATASET, col, value, sorted(set(norm(col)))))

partitions = [str(pid).strip().lower() for pid in pp._list_partitions(TABLE_PATH)]


def has_partition(version, period):
    versions = pp.versions_of_period(partitions, period)
    if version == pp.LATEST:
        return any(pp.VERSION_RE.match(v) for v in versions)
    return version.lower() in versions


tasks = sorted(set(zip(par["uds_version_clone"], par["period_clone"], par["period_run_clone"])))
for version, period, _ in tasks:
    if not has_partition(version, period):
        print("WARNING {} {}: no partition {}|{}, skipped".format(scope, version, period.lower(), version))
tasks = [t for t in tasks if has_partition(t[0], t[1])]
if not tasks:
    raise ValueError("No (version, period) with a source partition for {}".format(dst))
print("{} (version, period) for {}: {}".format(len(tasks), dst, tasks))

# 3. Top lines of every (version, period), stacked ("cumul")
try:
    parts = []
    for version, period, period_run in tasks:
        run = ap.resolve_run_params(params, scope, version)
        parts.append(toplines_period(spark, run, registry, period, period_run))
    columns = [set(p.columns) for p in parts]
    if any(c != columns[0] for c in columns):
        raise ValueError("Top lines of the runs have different columns: {}".format(columns))
    toplines = reduce(DataFrame.unionByName, parts).toDF(*[c.lower() for c in parts[0].columns]).cache()

    # 4. Checks, then write
    counts = toplines.groupBy("uds_version_clone", "period_clone").count().orderBy("uds_version_clone", "period_clone")
    print("Top lines per version and period:\n" + "\n".join(
        "  {} {}: {}".format(r["uds_version_clone"], r["period_clone"], r["count"]) for r in counts.collect()))
    thresholds = params["thresholds_topline"]
    below = toplines.filter(~reduce(lambda a, b: a | b, [F.col(v) > float(t) for v, t in thresholds.items()])).count()
    if below:
        raise ValueError("{} rows pass no threshold of {}".format(below, thresholds))
    dkuspark.write_with_schema(dataiku.Dataset(OUTPUT_DATASET), toplines.coalesce(NB_OUTPUT_FILES))
finally:
    pp.clear_cache()
