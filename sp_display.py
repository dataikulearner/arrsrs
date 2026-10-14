# DSS Python recipe: post-processing for SRS Arrimage (same core as CCIRC: core.post_processing.top_anomalies_line_par)
#   inputs  : SP_anomalies (dependency t_variable = Equals: the partition of THIS target), SP_toplines_cumul (all periods),
#             parameters_folder
#   outputs : SP_top_seg, SP_display_seg, SP_display_toplines, partitioned scope|period_run|id_set_parameters|t_variable:
#             one activity = one target; read the outputs with "All partitions" to see every target
#   Arrimage specifics, handled here without changing the core:
#     - one call per (id_set_parameters_clone, target) (stda v2x1 / v3x1 share the partition; the core merges segments
#       and lines on (t_variable, seg_axes, seg_axes_value) only and would mix the two versions)
#     - id_set_parameters_clone relabelled stda / sgcp after the core, version kept in version_clone
#     - to_query of the core replaced by a version that quotes values safely (the legacy file defines to_query_simple
#       twice, and a value containing "'" breaks the query)
#     - t_agg_name_mapping built as {target: first element of target_detection_mapping[target]}
#     - inputs sorted before ranking so that create_ranks (method="first") gives the same ranks at every run
import json

import dataiku
import pandas as pd

import core.post_processing as cpp
from core.post_processing import top_anomalies_line_par
from flows.arrimage import params as ap

CONFIG_FOLDER = "parameters_folder"
PARAMS_PATH = "flows/arrimage_params.json"
SEG_DATASET, LINE_DATASET = "SP_anomalies", "SP_toplines_cumul"
OUT_TOP, OUT_SEG, OUT_LINES = "SP_top_seg", "SP_display_seg", "SP_display_toplines"
FLOW_TYPE = "arrimage"
MODEL_TYPE = "basic"
T_AGG_MEAN_MAPPING = {"agg4": False, "agg5": True}           # agg5 = EAD-weighted average, comparable to a line value
LINE_TOPN_FINAL, LINE_TOPN_RUN, LINE_TOPN_REF = 10, 50, 100
# Line columns the DSS core selects in the final merge on top of line_id_cols (CCIRC display columns):
# added as empty columns when the Arrimage lines do not have them
CORE_EXTRA_LINE_COLS = ["accounting_site_code_post_acc", "lb_site_risque_apres_accr", "pmas_code_post_acc",
                        "business_country", "cd_uds_pmas_metier_ap_acc"]
TOP_SEG_KEY = ["t_variable_clone", "seg_axes", "seg_axes_value", "period_clone", "scope_clone", "id_set_parameters_clone",
               "version_clone"]


def to_query_arrimage(axes, axes_val, flow_type=None):
    """(axis == 'value') & ... with values quoted by repr (a "'" in a value does not break the query)"""
    return " & ".join("({} == {!r})".format(axe, val) for axe, val in zip(axes.split("|"), axes_val.split("|")))


cpp.to_query = to_query_arrimage                               # used by output_top_deviated_lines

with dataiku.Folder(CONFIG_FOLDER).get_download_stream(PARAMS_PATH) as stream:
    params = json.load(stream)
ap.validate_params(params)
runs_by_id = {ap.id_set_params(s, v): ap.resolve_run_params(params, s, v) for s, v in ap.list_runs(params)}
# Dashboard labels, set AFTER the core: id_set_parameters_clone = scope ("stda", "sgcp"), the run version moves to
# version_clone ("v2x1", "v3x1", "latest"). The core must still get one call per arrimage_<scope>_<version>
LABELS = {ap.id_set_params(s, v): (s, v) for s, v in ap.list_runs(params)}


def relabel(df):
    """id_set_parameters_clone arrimage_stda_v2x1 -> stda + version_clone v2x1"""
    if df.empty:
        return df
    unknown = set(df["id_set_parameters_clone"]) - set(LABELS)
    if unknown:
        raise ValueError("Unknown id_set_parameters_clone: {}".format(sorted(unknown)))
    df = df.copy()
    df["version_clone"] = df["id_set_parameters_clone"].map(lambda i: LABELS[i][1])
    df["id_set_parameters_clone"] = df["id_set_parameters_clone"].map(lambda i: LABELS[i][0])
    return df

# 1. Inputs
df_seg = dataiku.Dataset(SEG_DATASET).get_dataframe()
df_line = dataiku.Dataset(LINE_DATASET).get_dataframe()
for name, df_in in [(SEG_DATASET, df_seg), (LINE_DATASET, df_line)]:
    print("{}: {} rows, columns {}".format(name, len(df_in), sorted(df_in.columns)))
    missing_cols = [c for c in ["period_clone", "period_run_clone", "scope_clone", "id_set_parameters_clone"]
                    if c not in df_in.columns]
    if df_in.empty or missing_cols:
        raise ValueError("{} is empty or misses {}: check that it is built for this scope and that the partition "
                         "dependency of the input points to existing partitions".format(name, missing_cols))
period_run = df_seg["period_run_clone"].iloc[0]
scope = df_seg["scope_clone"].iloc[0]
for col in ["period_run_clone", "scope_clone"]:
    if df_seg[col].nunique() != 1:
        raise ValueError("{} holds several {}: {}".format(SEG_DATASET, col, sorted(df_seg[col].unique())))
targets = sorted(df_seg["t_variable_clone"].unique())
dst_target = dataiku.dku_flow_variables.get("DKU_DST_t_variable")         # partition being built (None if not)
if dst_target and targets != [dst_target]:
    raise ValueError("Building t_variable={} but {} holds {}: set the t_variable dependency of {} to Equals".format(
        dst_target, SEG_DATASET, targets, SEG_DATASET))
print("{}: {} rows, scope={} period_run={} versions={} targets={}".format(
    SEG_DATASET, len(df_seg), scope, period_run, sorted(df_seg["id_set_parameters_clone"].unique()), targets))
print("{}: {} rows, periods={}".format(LINE_DATASET, len(df_line), sorted(df_line["period_clone"].unique())))

results, failures = {OUT_TOP: [], OUT_SEG: [], OUT_LINES: []}, []
# One core call per (version, target): the core reads ONE target per call (t_variable_clone.values[0])
for (id_set, target), seg in df_seg.groupby(["id_set_parameters_clone", "t_variable_clone"]):
    run = runs_by_id[id_set]
    axes = sorted({a for combi in seg["seg_axes"].unique() for a in combi.split("|")})
    # passed as in CCIRC, {target: ["agg4"]}: the DSS core takes element [0] itself (a plain string gives "a")
    t_agg_name_mapping = {t: (v if isinstance(v, list) else [v]) for t, v in run["target_detection_mapping"].items()}
    text_mapping = dict({a: a for a in axes}, **run.get("text_mapping", {}))
    thr_seg, thr_line = run["thresholds_segment_materiality"], run["thresholds_line_anomaly"]

    # 2. Checks before the core (it raises KeyError deep inside otherwise)
    missing = [k for k, ok in [("thresholds_segment_materiality.reference", "reference" in thr_seg),
                               ("thresholds_segment_materiality." + target, target in thr_seg),
                               ("thresholds_line_anomaly." + target, target in thr_line),
                               ("thresholds_line_anomaly.count_min", "count_min" in thr_line),
                               ("thresholds_line_anomaly.line_deviation_seg", "line_deviation_seg" in thr_line),
                               ("thresholds_line_anomaly.line_deviation_ref", "line_deviation_ref" in thr_line),
                               ("target_detection_mapping." + target, target in t_agg_name_mapping)] if not ok]
    line = df_line[df_line["id_set_parameters_clone"] == id_set].copy()
    needed = axes + [target, run["line_materiality_col"], run["line_fac_col"]] + list(run["line_id_cols"])
    missing += ["{}.{}".format(LINE_DATASET, c) for c in needed if c not in line.columns]
    print("{}: line_id_cols {} -> non null rows {}".format(id_set, run["line_id_cols"], {
        c: int(line[c].notna().sum()) for c in run["line_id_cols"] if c in line.columns}))
    if missing:
        raise ValueError("{} {}: missing {}".format(id_set, target, missing))
    if line.empty:
        failures.append("{} {}: no top line in {}".format(id_set, target, LINE_DATASET))
        continue
    for c in CORE_EXTRA_LINE_COLS:
        if c not in line.columns:
            line[c] = None
    for a in axes:                                             # same spelling as seg_axes_value ("NULL", codes as text)
        line[a] = line[a].where(line[a].isna(), line[a].astype(str))

    # 3. Core post-processing, inputs in a fixed order (deterministic ranks)
    seg = seg.sort_values(["seg_axes", "seg_axes_value", "period_clone", "model_type"]).reset_index(drop=True)
    line = line.sort_values(["period_clone", run["line_fac_col"], run["line_materiality_col"]]).reset_index(drop=True)
    out = top_anomalies_line_par(
        seg, line, period_run, scope, run["line_materiality_col"], run["line_fac_col"], thr_line, thr_seg, FLOW_TYPE,
        id_set, list(run["line_id_cols"]), t_agg_name_mapping, T_AGG_MEAN_MAPPING, run.get("segment_filters", {}),
        run.get("segment_conditions", {}), text_mapping, MODEL_TYPE,
        line_topn_final=LINE_TOPN_FINAL, line_topn_run=LINE_TOPN_RUN, line_topn_ref=LINE_TOPN_REF)
    if out is None:                                            # the core logs the exception and returns None
        failures.append("{} {}: top_anomalies_line_par failed, see the errors above".format(id_set, target))
        continue
    df_top, df_disp_seg, df_disp_lines = out
    print("{} {}: {} top segments, {} display rows, {} display line rows".format(
        id_set, target, len(df_top), len(df_disp_seg), len(df_disp_lines)))
    results[OUT_TOP].append(df_top)
    results[OUT_SEG].append(df_disp_seg)
    results[OUT_LINES].append(df_disp_lines)

if failures:
    raise ValueError("Post-processing {}: {}".format(scope, failures))

# 4. Checks and write
df_top = relabel(pd.concat(results[OUT_TOP], ignore_index=True))
if df_top.empty:
    raise ValueError("No top segment for {} {} (see the core warnings above)".format(scope, period_run))
print("{} columns: {}".format(OUT_TOP, sorted(df_top.columns)))
if [c for c in TOP_SEG_KEY if c not in df_top.columns]:
    raise ValueError("{} misses {}".format(OUT_TOP, [c for c in TOP_SEG_KEY if c not in df_top.columns]))
dup = df_top.duplicated(TOP_SEG_KEY).sum()
if dup:
    raise ValueError("{} duplicated segments in {} on {}".format(dup, OUT_TOP, TOP_SEG_KEY))
outputs = {
    OUT_TOP: df_top.sort_values(TOP_SEG_KEY),
    OUT_SEG: relabel(pd.concat(results[OUT_SEG], ignore_index=True)),
    OUT_LINES: relabel(pd.concat(results[OUT_LINES], ignore_index=True)),
}
for name in [OUT_SEG, OUT_LINES]:
    key = [c for c in ["id_set_parameters_clone", "version_clone", "t_variable_clone", "segment_id", "period_clone",
                       "index"] if c in outputs[name].columns]
    outputs[name] = outputs[name].sort_values(key)
outputs[OUT_LINES] = outputs[OUT_LINES][sorted(outputs[OUT_LINES].columns)]
for name, df in outputs.items():
    print("{}: {} rows, by version and target:\n{}".format(
        name, len(df), df.groupby(["id_set_parameters_clone", "version_clone", "t_variable_clone"]).size().to_string()))
    dataiku.Dataset(name).write_with_schema(df.reset_index(drop=True))
