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
PARAMS_PATH = "arrimage_params.json"     # path inside CONFIG_FOLDER
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
                 for alias, fn in run["target_agg_functions"].items()]                 # seg_agg1,
