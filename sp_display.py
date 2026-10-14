import json

import dataiku
import pandas as pd

import core.post_processing as cpp
from core.post_processing import top_anomalies_line_par
from flows.arrimage import params as ap

CONFIG_FOLDER = "parameters_folder"
PARAMS_PATH = "arrimage_params.json"
SEG_DATASET, LINE_DATASET = "SP_anomalies", "SP_toplines_cumul"
OUT_TOP, OUT_SEG, OUT_LINES = "SP_top_seg", "SP_display_seg", "SP_display_toplines"
FLOW_TYPE = "arrimage"
MODEL_TYPE = "basic"
T_AGG_MEAN_MAPPING = {"agg4": False, "agg5": True}           # agg5 = EAD-weighted average, comparable to a line value
LINE_TOPN_FINAL, LINE_TOPN_RUN, LINE_TOPN_REF = 10, 50, 100
TOP_SEG_KEY = ["t_variable_clone", "seg_axes", "seg_axes_value", "period_clone", "scope_clone", "id_set_parameters_clone"]


def to_query_arrimage(axes, axes_val, flow_type=None):
    """(axis == 'value') & ... with values quoted by repr (a "'" in a value does not break the query)"""
    return " & ".join("({} == {!r})".format(axe, val) for axe, val in zip(axes.split("|"), axes_val.split("|")))


cpp.to_query = to_query_arrimage                               # used by output_top_deviated_lines

with dataiku.Folder(CONFIG_FOLDER).get_download_stream(PARAMS_PATH) as stream:
    params = json.load(stream)
ap.validate_params(params)
runs_by_id = {ap.id_set_params(s, v): ap.resolve_run_params(params, s, v) for s, v in ap.list_runs(params)}

# 1. Inputs
df_seg = dataiku.Dataset(SEG_DATASET).get_dataframe()
df_line = dataiku.Dataset(LINE_DATASET).get_dataframe()
if df_seg.empty:
    raise ValueError("{} is empty for this partition".format(SEG_DATASET))
period_run = df_seg["period_run_clone"].iloc[0]
scope = df_seg["scope_clone"].iloc[0]
target = df_seg["t_variable_clone"].iloc[0]
for col, value in [("period_run_clone", period_run), ("scope_clone", scope), ("t_variable_clone", target)]:
    if df_seg[col].nunique() != 1:
        raise ValueError("{} holds several {}: {}".format(SEG_DATASET, col, sorted(df_seg[col].unique())))
print("{}: {} rows, scope={} period_run={} target={} versions={}".format(
    SEG_DATASET, len(df_seg), scope, period_run, target, sorted(df_seg["id_set_parameters_clone"].unique())))
print("{}: {} rows, periods={}".format(LINE_DATASET, len(df_line), sorted(df_line["period_clone"].unique())))

results, failures = {OUT_TOP: [], OUT_SEG: [], OUT_LINES: []}, []
for id_set, seg in df_seg.groupby("id_set_parameters_clone"):
    run = runs_by_id[id_set]
    axes = sorted({a for combi in seg["seg_axes"].unique() for a in combi.split("|")})
    t_agg_name_mapping = {t: (v[0] if isinstance(v, list) else v) for t, v in run["target_detection_mapping"].items()}
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
    if missing:
        raise ValueError("{}: missing {}".format(id_set, missing))
    if line.empty:
        failures.append("{}: no top line in {}".format(id_set, LINE_DATASET))
        continue
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
        failures.append("{}: top_anomalies_line_par failed, see the errors above".format(id_set))
        continue
    df_top, df_disp_seg, df_disp_lines = out
    print("{}: {} top segments, {} display rows, {} display line rows".format(
        id_set, len(df_top), len(df_disp_seg), len(df_disp_lines)))
    results[OUT_TOP].append(df_top)
    results[OUT_SEG].append(df_disp_seg)
    results[OUT_LINES].append(df_disp_lines)

if failures:
    raise ValueError("Post-processing {} {}: {}".format(scope, target, failures))

# 4. Checks and write
df_top = pd.concat(results[OUT_TOP], ignore_index=True)
dup = df_top.duplicated(TOP_SEG_KEY).sum()
if dup:
    raise ValueError("{} duplicated segments in {} on {}".format(dup, OUT_TOP, TOP_SEG_KEY))
outputs = {
    OUT_TOP: df_top.sort_values(TOP_SEG_KEY),
    OUT_SEG: pd.concat(results[OUT_SEG], ignore_index=True),
    OUT_LINES: pd.concat(results[OUT_LINES], ignore_index=True),
}
for name in [OUT_SEG, OUT_LINES]:
    key = [c for c in ["id_set_parameters_clone", "segment_id", "period_clone", "index"] if c in outputs[name].columns]
    outputs[name] = outputs[name].sort_values(key)
outputs[OUT_LINES] = outputs[OUT_LINES][sorted(outputs[OUT_LINES].columns)]
for name, df in outputs.items():
    print("{}: {} rows".format(name, len(df)))
    dataiku.Dataset(name).write_with_schema(df.reset_index(drop=True))
