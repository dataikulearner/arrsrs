# DSS Python recipe: SP_agg_focus_model for SRS Arrimage (same logic as the CCIRC recipe)
#   inputs  : SP_agg (all periods of the run), params_par_model_par (one task = scope x t_variable x axe),
#             folder parameters_folder
#   output  : SP_agg_focus_model = SP_agg rows of the task + seg_agg5/6/7 (mean over the last 4 quarters)
#   CCIRC logic kept: segment aggregates over the last nb_last quarters ending at period_run, segments with
#   seg_agg5 <= thresholds_topseg dropped, then fill_na_vanishing_segment (segment present last quarter, absent now)
#   Arrimage: everything per id_set_parameters_clone (stda v2x1 and v3x1 share the partition) and per seg_axes
import json

import dataiku
import pandas as pd

from flows.arrimage import params as ap
from utils.py import fill_na_vanishing_segment, get_last_quarter

CONFIG_FOLDER = "parameters_folder"
PARAMS_PATH = "arrimage_params.json"
SP_AGG_DATASET = "SP_agg"
PAR_DATASET = "params_par_model_par"
OUTPUT_DATASET = "SP_agg_focus_model"
SEGMENT_KEY = ["id_set_parameters_clone", "seg_axes", "seg_axes_value"]
UNIQUE_KEY = SEGMENT_KEY + ["t_variable_clone", "period_clone", "period_run_clone", "scope_clone"]

with dataiku.Folder(CONFIG_FOLDER).get_download_stream(PARAMS_PATH) as stream:
    params = json.load(stream)
ap.validate_params(params)
segment_aggs = params["segment_aggs"]                 # seg_agg5 = mean(seg_agg2), seg_agg6 = mean(seg_agg1), seg_agg7 = mean(seg_agg3)
thresholds_topseg = params["thresholds_topseg"]       # {"seg_agg5": 500}

# 1. Task of this 
# 1. Task of this partition: taken ONLY from the partition being built (no silent fallback to the first task)
flow_vars = dataiku.dku_flow_variables
dst = {d: flow_vars.get("DKU_DST_" + d) for d in ["scope", "period_run", "id_set_parameters", "axe", "t_variable"]}
print("DKU_DST variables: {}".format({k: v for k, v in flow_vars.items() if k.startswith("DKU_DST_")}))
missing_dims = [d for d in ["scope", "period_run", "t_variable"] if not dst[d]]
if missing_dims:
    raise ValueError("No DKU_DST_ variable for {}: {} must be partitioned by these dimensions (same names)".format(
        missing_dims, OUTPUT_DATASET))
scope, t_var = dst["scope"], dst["t_variable"]
axe = dst["axe"] or None
period_run = dst["period_run"].strip().upper()
if scope not in params[ap.SCOPES_KEY]:
    raise ValueError("scope = {!r}, expected one of {}".format(scope, sorted(params[ap.SCOPES_KEY])))
print("Task: scope={} t_variable={} axe={} period_run={}".format(scope, t_var, axe, period_run))

# 2. SP_agg rows of the task
df = dataiku.Dataset(SP_AGG_DATASET).get_dataframe()
df["period_clone"] = df["period_clone"].astype(str).str.strip().str.upper()
df_out = df[(df["scope_clone"] == scope) & (df["t_variable_clone"] == t_var)]
if axe:
    df_out = df_out[df_out["seg_axes"] == axe.replace("___", "|")]
if df_out.empty:
    raise ValueError("SP_agg has no row for scope={} t_variable={} axe={} (t_variables: {})".format(
        scope, t_var, axe, sorted(df.loc[df["scope_clone"] == scope, "t_variable_clone"].unique())))
print("{} SP_agg rows, by version and period:\n{}".format(
    len(df_out), df_out.groupby(["id_set_parameters_clone", "period_clone"]).size().to_string()))

# 3. Segment aggregates over the last nb_last quarters ending at period_run (Q_current, Q-1, Q-2, Q-3)
nb_last = max(spec["nb_last"] for spec in segment_aggs.values())
last_periods = [period_run]
while len(last_periods) < nb_last:
    last_periods.append(get_last_quarter(last_periods[-1]))
recent = df_out[df_out["period_clone"].isin(last_periods)]
df_seg = None
for name, spec in segment_aggs.items():
    agg = (recent[recent["period_clone"].isin(last_periods[:spec["nb_last"]])]
           .groupby(SEGMENT_KEY)[spec["variable"]].agg(spec["function"]).rename(name).reset_index())
    df_seg = agg if df_seg is None else df_seg.merge(agg, on=SEGMENT_KEY, how="outer")

# Segments kept: seg_agg5 > thresholds_topseg (CCIRC "à sortir")
for name, threshold in thresholds_topseg.items():
    df_seg = df_seg[df_seg[name] > threshold]
print("{} segments kept out of {} (thresholds_topseg {})".format(
    len(df_seg), df_out[SEGMENT_KEY].drop_duplicates().shape[0], thresholds_topseg))
df_out = df_out.drop(columns=[c for c in segment_aggs if c in df_out.columns])
df_out = pd.merge(df_out, df_seg, how="inner", on=SEGMENT_KEY)

# 4. Vanishing segments, per version: Q_current vs the previous quarter present in that version
#    (v3x1 has no 2026q1: its previous quarter is 2025Q3). fill_na_vanishing_segment groups by (seg_axes,
#    seg_axes_value) and copies every other column as metadata: called per id_set, and without uds_version_clone,
#    which changes with the period for "latest" (sgcp) and would duplicate the added row
parts = []
for id_set, group in df_out.groupby("id_set_parameters_clone"):
    previous = sorted(p for p in group["period_clone"].unique() if p < period_run)
    if previous:
        version_of = group.groupby("period_clone")["uds_version_clone"].first()
        n_before = len(group)
        group = fill_na_vanishing_segment(group.drop(columns="uds_version_clone"), period_run, previous[-1],
                                          list(segment_aggs))
        group["uds_version_clone"] = group["period_clone"].map(version_of)
        print("{}: {} vanishing segment(s) added at {} (present at {})".format(
            id_set, len(group) - n_before, period_run, previous[-1]))
    parts.append(group)
df_out = pd.concat(parts, ignore_index=True)

# 5. Checks and write
duplicates = df_out.duplicated(UNIQUE_KEY).sum()
if duplicates:
    raise ValueError("{} duplicated rows on {}".format(duplicates, UNIQUE_KEY))
df_out = df_out.sort_values(UNIQUE_KEY).reset_index(drop=True)
print("{} rows written".format(len(df_out)))
dataiku.Dataset(OUTPUT_DATASET).write_with_schema(df_out)
