# DSS Python recipe: params_par_model_par for SRS Arrimage
#   input  : folder parameters_folder (flows/arrimage_params.json)
#   output : params_par_model_par, one row per modeling task = (scope_clone, t_variable_clone, axe_clone)
#            stda: 6 targets x 13 axes = 78 rows, sgcp: 8 targets x 8 axes = 64 rows
import json

import dataiku
import pandas as pd

from flows.arrimage import params as ap

with dataiku.Folder("parameters_folder").get_download_stream("arrimage_params.json") as stream:
    params = json.load(stream)
ap.validate_params(params)

target_scope = dataiku.dku_flow_variables.get("DKU_DST_scope")       # if the dataset is partitioned by scope
rows = []
for scope, block in params[ap.SCOPES_KEY].items():
    if target_scope is not None and scope != target_scope:
        continue
    # Thresholds are read by seg_axes ("|") and by target: fail now rather than in the detector (KeyError)
    missing_axes = [a for a in block["axes_str"].split("/")
                    if a.replace("___", "|") not in block["thresholds_segment_anomaly"]]
    missing_targets = [t for t in block["target_variables"] if t not in block["thresholds_negligibility"]]
    if missing_axes or missing_targets:
        raise ValueError("{}: no thresholds_segment_anomaly for {}, no thresholds_negligibility for {}".format(
            scope, missing_axes, missing_targets))
    for t_var in block["target_variables"]:
        for axe in block["axes_str"].split("/"):
            rows.append({"scope_clone": scope, "t_variable_clone": t_var, "axe_clone": axe})

par = pd.DataFrame(rows)
if par.duplicated().any():
    raise ValueError("Duplicated tasks: {}".format(par[par.duplicated()].to_dict("records")))
print(par.groupby("scope_clone").size())
dataiku.Dataset("params_par_model_par").write_with_schema(par)
