# Pre-processing logic for the SRS Arrimage flow (source: RDS_SRS_CAPITAL.uds_fac_arrime)
#
# Contract (same as the other flows, called by the dispatcher in pre_processing.py):
#   enrich_data_period_arrimage(spark_sess, table_path, period, **kwargs) -> pyspark DataFrame
#   topline_process_arrimage(df) -> pandas DataFrame
#
# No column name is written in this module: source columns, derived axes and derived measures
# come from the "arrimage" entry of the column registry, passed in kwargs["columns"].
#
# table_path is the name of the DSS dataset (e.g. "RDS_SRS_CAPITAL.uds_fac_arrime"), an input of
# the recipe. It is partitioned by dt_closing|uds_version (e.g. "2026q2|v3x1"); partitions are read
# through the DSS API, which also lists the versions available for "latest".
#
# The enriched partition is cached, so that the calls of prepare_agg_period_axe for the different
# axes of the same (period, version) read the source only once.
import json
import re

import pandas as pd

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.logger import create_logger

logger = create_logger(__name__)

FLOW_ID = "arrimage"
ENTITY_COL = "entity"
UDS_VERSION_COL = "uds_version"     # version actually read, kept in the output for audit

# uds_version values look like "v3x1": major version 3, iteration 1
LATEST = "latest"
VERSION_RE = re.compile(r"^v(\d+)x(\d+)$")

# Column registry vocabulary
ROLE_AXIS, ROLE_MEASURE, ROLE_LINE_ID, ROLE_SOURCE = "axis", "measure", "line_id", "source"
ROLES = [ROLE_AXIS, ROLE_MEASURE, ROLE_LINE_ID, ROLE_SOURCE]
TYPE_STRING, TYPE_DOUBLE = "string", "double"
RULE_ISIN, RULE_ALL_NOT_NULL = "isin", "all_not_null"

# Default parameters, overridden by **kwargs (flow params json)
DEFAULTS = {
    # "arrimage" entry of the column registry (mandatory)
    "columns": None,
    # Partition to read
    "uds_version": None,               # mandatory: a version such as "v3x1", or "latest"
    # Root axis: an existing column, or a constant when entity_col is None
    "entity_col": None,
    "entity_value": "SRS_ARRIMAGE",
    # Lines without approach carry no EAD and no RWA (checked on 2026q2)
    "exclude_no_approach": True,
    # Value given to empty axes: concat_ws skips NULLs and would shift the segment id
    "null_axis_value": "NULL",
    # Columns the run uses (axes, targets, segment and weight variables); None = every registry column.
    # Only these must exist in the partition; other registry columns missing from it are added as NULL
    # (the registry is shared by the scopes, and older versions lack some columns, e.g. tx_ltv_cur)
    "used_columns": None,
}


# ==============================================================================
# Column registry (pure Python)
# ==============================================================================

def load_column_registry(path: str, flow_id: str = FLOW_ID) -> dict:
    """Read the entry of a flow in the column registry file
    Args:
        path: str, path of the column registry json
        flow_id: str, the flow entry to return

    Returns:
        dict, to pass as kwargs["columns"]

    Raises:
        ValueError if the flow has no entry
    """
    with open(path) as f:
        registry = json.load(f)
    if flow_id not in registry:
        raise ValueError("column registry {} has no entry for flow '{}'".format(path, flow_id))
    return registry[flow_id]


def _rule_columns(rule: dict) -> list:
    """Source columns referenced by a derivation rule"""
    cols = [rule["then"]] + ([rule["otherwise"]] if rule.get("otherwise") else [])
    if rule["rule"] == RULE_ISIN:
        return cols + [rule["col"]]
    return cols + list(rule["cols"])


def resolve_columns(entry: dict) -> dict:
    """Validate the column registry entry and put its names in lower case
    Args:
        entry: dict, the "arrimage" entry of the column registry

    Returns:
        dict with keys columns, derived_axes, derived_measures, approach_col

    Raises:
        ValueError listing every problem found
    """
    if not entry or not entry.get("columns"):
        raise ValueError("arrimage: 'columns' (column registry entry) is mandatory")
    errors = []
    columns = {name.lower(): dict(spec) for name, spec in entry["columns"].items()}
    for name, spec in columns.items():
        if spec.get("role") not in ROLES:
            errors.append("column '{}': unknown role '{}'".format(name, spec.get("role")))
        if spec.get("type") not in (TYPE_STRING, TYPE_DOUBLE):
            errors.append("column '{}': unknown type '{}'".format(name, spec.get("type")))

    derived = {}
    for group in ("derived_axes", "derived_measures"):
        derived[group] = {}
        for name, rule in entry.get(group, {}).items():
            name, rule = name.lower(), dict(rule)
            if rule.get("rule") not in (RULE_ISIN, RULE_ALL_NOT_NULL):
                errors.append("{} '{}': unknown rule '{}'".format(group, name, rule.get("rule")))
                continue
            needed = ["then", "col", "values"] if rule["rule"] == RULE_ISIN else ["then", "cols"]
            absent = [key for key in needed if not rule.get(key)]
            if absent:
                errors.append("{} '{}': keys {} are missing".format(group, name, absent))
                continue
            for key in ("col", "then", "otherwise"):
                if rule.get(key):
                    rule[key] = rule[key].lower()
            if "cols" in rule:
                rule["cols"] = [col.lower() for col in rule["cols"]]
            if name in columns or name in (ENTITY_COL, UDS_VERSION_COL):
                errors.append("{} '{}': name already used".format(group, name))
            for col in _rule_columns(rule):
                if col not in columns:
                    errors.append("{} '{}': column '{}' is not declared".format(group, name, col))
            derived[group][name] = rule

    approach_col = (entry.get("approach_col") or "").lower()
    if approach_col not in columns:
        errors.append("approach_col '{}' is not declared".format(approach_col))
    if errors:
        raise ValueError("arrimage column registry: " + "; ".join(errors))
    return {"columns": columns, "approach_col": approach_col,
            "derived_axes": derived["derived_axes"], "derived_measures": derived["derived_measures"]}


def _columns_with_role(spec: dict, role: str) -> list:
    return [name for name, col in spec["columns"].items() if col["role"] == role]


def output_axes(spec: dict) -> list:
    """Axes available after enrichment, for the validation of the flow params"""
    return [ENTITY_COL] + _columns_with_role(spec, ROLE_AXIS) + list(spec["derived_axes"])


def output_measures(spec: dict) -> list:
    """Measures available after enrichment, for the validation of the flow params"""
    return _columns_with_role(spec, ROLE_MEASURE) + list(spec["derived_measures"])


def line_id_columns(spec: dict) -> list:
    """Identifier columns kept for the top lines"""
    return _columns_with_role(spec, ROLE_LINE_ID)


# ==============================================================================
# Flow parameters (pure Python)
# ==============================================================================

def resolve_config(kwargs: dict) -> dict:
    """Merge the flow kwargs with the defaults and validate them
    Args:
        kwargs: dict, the specific parameters of the flow; unknown keys are ignored

    Returns:
        dict, with every key of DEFAULTS plus "spec", the validated column registry entry

    Raises:
        ValueError if a mandatory parameter is missing or malformed
    """
    cfg = {key: kwargs.get(key, default) for key, default in DEFAULTS.items()}
    if not cfg["uds_version"]:
        raise ValueError("arrimage: 'uds_version' is mandatory (e.g. 'v3x1')")
    cfg["spec"] = resolve_columns(cfg.pop("columns"))
    if cfg["entity_col"]:
        cfg["entity_col"] = cfg["entity_col"].lower()
    return cfg


def parse_version(version: str) -> tuple:
    """Turn "v3x1" into (3, 1)
    Raises:
        ValueError if the name does not follow the vAxB pattern
    """
    match = VERSION_RE.match(version)
    if not match:
        raise ValueError("arrimage: '{}' is not a vAxB version".format(version))
    return int(match.group(1)), int(match.group(2))


def pick_latest_version(versions: list) -> str:
    """Return the highest version: highest major version, then highest iteration
    Args:
        versions: list of str, the uds_version values available for a quarter; names that do
            not follow the vAxB pattern are ignored

    Raises:
        ValueError if no valid version is given
    """
    valid = [v for v in versions if VERSION_RE.match(v)]
    for ignored in sorted(set(versions) - set(valid)):
        logger.warning("Ignoring uds_version '{}': not a vAxB version".format(ignored))
    if not valid:
        raise ValueError("arrimage: no vAxB version among {}".format(sorted(versions)))
    return max(valid, key=parse_version)


def partition_id(period: str, uds_version: str) -> str:
    """DSS partition id of a period and a version, e.g. ("2026Q2", "v3x1") -> "2026q2|v3x1" """
    if uds_version == LATEST:
        raise ValueError("arrimage: resolve 'latest' before building the partition id")
    return "{}|{}".format(period.lower(), uds_version)


def versions_of_period(partition_ids: list, period: str) -> list:
    """Versions available for a period among DSS partition ids such as "2026q2|v3x1" """
    prefix = period.lower() + "|"
    return [pid[len(prefix):] for pid in partition_ids if pid.startswith(prefix)]


def required_source_columns(cfg: dict) -> list:
    """List the source columns the flow needs, in a stable order and without duplicates"""
    cols = list(cfg["spec"]["columns"])
    if cfg["entity_col"] and cfg["entity_col"] not in cols:
        cols.append(cfg["entity_col"])
    return cols


def needed_source_columns(cfg: dict) -> list:
    """Source columns that must exist: used_columns, expanded through the derivation rules"""
    if not cfg.get("used_columns"):
        return required_source_columns(cfg)
    spec = cfg["spec"]
    rules = dict(spec["derived_axes"], **spec["derived_measures"])
    needed = {spec["approach_col"]} | ({cfg["entity_col"]} if cfg["entity_col"] else set())
    for name in cfg["used_columns"]:
        name = name.lower()
        needed.update(_rule_columns(rules[name]) if name in rules else [name])
    return [col for col in required_source_columns(cfg) if col in needed]


def check_required_columns(columns: list, cfg: dict, context: str):
    """Fail fast when a needed column is absent from the source
    Args:
        columns: list, lower case columns of the source
        cfg: dict, output of resolve_config
        context: str, the partition read, for the error message

    Raises:
        ValueError listing every missing column
    """
    missing = [col for col in needed_source_columns(cfg) if col not in columns]
    if missing:
        raise ValueError("arrimage: columns {} are missing in {}".format(missing, context))


# ==============================================================================
# Spark steps
# ==============================================================================

def _list_partitions(table_path: str) -> list:
    """Partition ids of the DSS dataset"""
    import dataiku
    return dataiku.Dataset(table_path, ignore_flow=True).list_partitions()


def _read_partition(spark_sess: SparkSession, table_path: str, pid: str) -> pyspark.sql.DataFrame:
    """Read one partition of the DSS dataset and put the column names in lower case"""
    import dataiku
    import dataiku.spark as dkuspark
    from pyspark.sql import SQLContext
    logger.info("Reading {} partition {}".format(table_path, pid))
    # ignore_flow: in a recipe, DSS refuses explicit partitions on a Flow input; the partition is chosen here
    dataset = dataiku.Dataset(table_path, ignore_flow=True)
    dataset.add_read_partitions(pid)
    df = dkuspark.get_dataframe(SQLContext(spark_sess.sparkContext), dataset)
    return df.toDF(*[col.lower() for col in df.columns])


def resolve_version(table_path: str, period: str, cfg: dict) -> str:
    """Return the version to read for a period: the configured one, or the latest available"""
    if cfg["uds_version"] != LATEST:
        return cfg["uds_version"]
    versions = versions_of_period(_list_partitions(table_path), period)
    if not versions:
        raise ValueError("arrimage: no partition for period {} in {}".format(period, table_path))
    version = pick_latest_version(versions)
    logger.info("Latest uds_version for period {}: {}".format(period, version))
    return version


# Enriched partitions kept in memory: key -> DataFrame. One entry is enough when the calls are
# ordered by (version, period), as the SP_agg recipe does.
_CACHE = {}
_CACHE_MAX_SIZE = 1


def _cache_key(table_path: str, pid: str, kwargs: dict) -> str:
    settings = {key: kwargs.get(key) for key in DEFAULTS if key != "uds_version"}
    return json.dumps([table_path, pid, settings], sort_keys=True, default=str)


def clear_cache():
    """Release every cached enriched partition. Call it at the end of a recipe."""
    for df in _CACHE.values():
        try:
            df.unpersist()
        except Exception:
            pass
    _CACHE.clear()


def _cache_put(key: str, df):
    while _CACHE and len(_CACHE) >= _CACHE_MAX_SIZE:
        oldest = next(iter(_CACHE))
        try:
            _CACHE.pop(oldest).unpersist()
        except Exception:
            pass
    _CACHE[key] = df.cache()
    return _CACHE[key]


def _clean_string(col_name: str):
    """Cast to string, trim and turn empty strings into NULL"""
    trimmed = F.trim(F.col(col_name).cast("string"))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _select_and_cast(df: pyspark.sql.DataFrame, cfg: dict) -> pyspark.sql.DataFrame:
    """Keep the registry columns only (the source has more than 1,200) with their registry type"""
    columns = cfg["spec"]["columns"]
    select_exprs = []
    absent = [col for col in required_source_columns(cfg) if col not in df.columns]
    if absent:
        logger.warning("arrimage: columns {} not in this partition and not used by the run: NULL".format(absent))
    for col_name in required_source_columns(cfg):
        col_type = columns.get(col_name, {}).get("type", TYPE_STRING)
        if col_name in absent:
            select_exprs.append(F.lit(None).cast(col_type).alias(col_name))
        elif col_type == TYPE_DOUBLE:
            select_exprs.append(F.col(col_name).cast("double").alias(col_name))
        else:
            select_exprs.append(_clean_string(col_name).alias(col_name))
    return df.select(*select_exprs)


def _add_derived(df: pyspark.sql.DataFrame, rules: dict) -> pyspark.sql.DataFrame:
    """Add the columns described by derivation rules"""
    for name, rule in rules.items():
        if rule["rule"] == RULE_ISIN:
            condition = F.col(rule["col"]).isin(rule["values"])
        else:
            condition = F.lit(True)
            for col in rule["cols"]:
                condition = condition & F.col(col).isNotNull()
        value = F.when(condition, F.col(rule["then"]))
        if rule.get("otherwise"):
            value = value.otherwise(F.col(rule["otherwise"]))
        df = df.withColumn(name, value)
    return df


def _add_entity(df: pyspark.sql.DataFrame, cfg: dict) -> pyspark.sql.DataFrame:
    """Add the root axis, from a column or as a constant"""
    if cfg["entity_col"]:
        return df.withColumn(ENTITY_COL, F.col(cfg["entity_col"]))
    return df.withColumn(ENTITY_COL, F.lit(cfg["entity_value"]))


# ==============================================================================
# Entry points called by the dispatcher
# ==============================================================================

def enrich_data_period_arrimage(spark_sess: SparkSession, table_path: str, period: str, **kwargs) \
        -> pyspark.sql.DataFrame:
    """Load and enrich the uds_fac_arrime partition of a period
    Args:
        spark_sess: SparkSession object, used to execute the computation
        table_path: str, name of the DSS dataset, e.g. "RDS_SRS_CAPITAL.uds_fac_arrime"
        period: str, the period of computation, such as "2026Q2"
        **kwargs: dict, specific parameters of the flow, see DEFAULTS; "columns" is the
            "arrimage" entry of the column registry

    Returns:
        pyspark DataFrame with the registry columns, the derived axes and measures, entity, and
        uds_version (the version actually read)
    """
    cfg = resolve_config(kwargs)
    spec = cfg["spec"]
    version = resolve_version(table_path, period, cfg)
    pid = partition_id(period, version)
    key = _cache_key(table_path, pid, kwargs)
    if key in _CACHE:
        logger.info("Using cached enriched partition {} {}".format(table_path, pid))
        return _CACHE[key]

    df = _read_partition(spark_sess, table_path, pid)
    check_required_columns(df.columns, cfg, "{} {}".format(table_path, pid))

    df = _select_and_cast(df, cfg)
    if cfg["exclude_no_approach"]:
        df = df.filter(F.col(spec["approach_col"]).isNotNull())
    df = _add_derived(df, spec["derived_axes"])
    df = _add_derived(df, spec["derived_measures"])
    df = _add_entity(df, cfg)
    df = df.fillna(cfg["null_axis_value"], subset=output_axes(spec))
    df = df.withColumn(UDS_VERSION_COL, F.lit(version))
    logger.info("Enriched arrimage period {} version {}: {} columns".format(period, version, len(df.columns)))
    return _cache_put(key, df)


def topline_process_arrimage(df: pd.DataFrame) -> pd.DataFrame:
    """Further process data for top line purpose
    Args:
        df: pandas DataFrame, the enriched DataFrame after enrich_data_period and top lines filtering

    Returns:
        pandas DataFrame, with whitespace stripped on string columns
    """
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].str.strip()
    return df
