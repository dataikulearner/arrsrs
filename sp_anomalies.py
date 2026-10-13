# DSS Python recipe: SP_anomalies for SRS Arrimage
#   input  : SP_agg_focus_model (partition of the task), folder parameters_folder
#   output : SP_anomalies = SP_agg_focus_model + model_type, anomaly, deviation, observed_value, expected_value,
#            target_variable_agg_name, materiality, period_parsed (one row per segment x period x tagg x model)
#   The detector is the core one, unchanged: models.detect_axes -> _compute_df_temp -> detect_core.
#   It groups by (t_variable, seg_axes, seg_axes_value, period_run, scope, id_set_parameters): stda v2x1 and v3x1
#   are two separate series.
import json

import dataiku
import pandas as pd

from core.constants import ANOMALY, DEVIATION, EXPECTED_VAL, MODEL_TYPE, TARGET_VARIABLE_AGG_NAME, AnomalyType
from core.models import GROUP_COLS, detect_axes          # adjust the module path if models.py lives elsewhere
from flows.arrimage import params as ap

CONFIG_FOLDER = "parameters_folder"
PARAMS_PATH = "arrimage_params.json"
INPUT_DATASET = "SP_agg_focus_model"
OUTPUT_DATASET = "SP_anomalies"
FAIL_ON_SKIPPED_GROUPS = True                    # detect_axes logs and skips a failing series: fail the job instead

with dataiku.Folder(CONFIG_FOLDER).get_download_stream(PARAMS_PATH) as stream:
    params = json.load(stream)
ap.validate_params(params)

# 1. Input of the task
df = dataiku.Dataset(INPUT_DATASET).get_dataframe()
if df.empty:
    raise ValueError("{} is empty for this partition".format(INPUT_DATASET))
scopes = df["scope_clone"].unique()
if len(scopes) != 1:
    raise ValueError("{} holds several scopes: {}".format(INPUT_DATASET, list(scopes)))
scope = scopes[0]
run = ap.resolve_run_params(params, scope, params[ap.SCOPES_KEY][scope][ap.RUNS_KEY][0]["uds_version"])
print("{} rows, scope {}, targets {}, axes {}".format(
    len(df), scope, sorted(df["t_variable_clone"].unique()), sorted(df["seg_axes"].unique())))

# 2. Parameters of the detector, checked before running (detect_axes raises KeyError otherwise)
thresholds_ano = run["thresholds_segment_anomaly"]          # by seg_axes ("|")
thresholds_negli = run["thresholds_negligibility"]          # by target
t_agg_mapping = run["target_detection_mapping"]             # {target: ["agg4"] | ["agg5"]}
missing = {
    "thresholds_segment_anomaly": sorted(set(df["seg_axes"]) - set(thresholds_ano)),
    "thresholds_negligibility": sorted(set(df["t_variable_clone"]) - set(thresholds_negli)),
    "target_detection_mapping": sorted(set(df["t_variable_clone"]) - set(t_agg_mapping)),
}
missing = {k: v for k, v in missing.items() if v}
if missing:
    raise ValueError("Missing keys in the params of scope {}: {}".format(scope, missing))
seg_agg_materiality = run["seg_agg_materiality"]            # "agg4" -> materiality = seg_agg4 * deviation
if "seg_" + seg_agg_materiality not in df.columns:
    raise ValueError("seg_agg_materiality = {!r} but {} has no column seg_{}".format(
        seg_agg_materiality, INPUT_DATASET, seg_agg_materiality))
print("nb_periods={} s_window={} nbs_last={} seg_agg_materiality={}".format(
    run["nb_periods"], run["s_window"], run["nbs_last"], seg_agg_materiality))

# 3. Core detector
df_res = detect_axes(df, thresholds_ano, thresholds_negli, t_agg_mapping, run["segment_vars"], seg_agg_materiality,
                     nb_periods=run["nb_periods"], season_length=run["s_window"], nb_hist_seasons=run["nbs_last"])

# 4. Checks: skipped series, NaN expected values (rows left NORMAL by the core), codes
groups_in = set(map(tuple, df[GROUP_COLS].drop_duplicates().values))
groups_out = set(map(tuple, df_res[GROUP_COLS].drop_duplicates().values))
skipped = sorted(groups_in - groups_out)
if skipped:
    print("WARNING {} series skipped by detect_axes (see the errors above): {}".format(len(skipped), skipped[:20]))
    if FAIL_ON_SKIPPED_GROUPS:
        raise ValueError("{} series failed in detect_axes, nothing written".format(len(skipped)))
nan_expected = df_res[(df_res[ANOMALY] == AnomalyType.NORMAL) & df_res[EXPECTED_VAL].isna()]
print("{} NORMAL rows with a NaN expected value (whole history empty)".format(len(nan_expected)))
print("Anomalies by model and code:\n{}".format(df_res.groupby([MODEL_TYPE, ANOMALY]).size().to_string()))
codes = {AnomalyType.NORMAL, AnomalyType.OUTLIER, AnomalyType.NEW, AnomalyType.VANISH, AnomalyType.NEGLIGIBLE,
         AnomalyType.ERROR}
if not set(df_res[ANOMALY].unique()) <= codes:
    raise ValueError("Unexpected anomaly codes: {}".format(set(df_res[ANOMALY].unique()) - codes))
bad_dev = df_res[(df_res[DEVIATION] < 0) & (df_res[ANOMALY] != AnomalyType.ERROR)]
if len(bad_dev):
    raise ValueError("{} rows with a negative deviation".format(len(bad_dev)))

# 5. Write, in a deterministic order
key = GROUP_COLS + ["period_clone", MODEL_TYPE, TARGET_VARIABLE_AGG_NAME]
df_res = df_res.sort_values(key).reset_index(drop=True)
print("{} rows written".format(len(df_res)))
dataiku.Dataset(OUTPUT_DATASET).write_with_schema(df_res)
