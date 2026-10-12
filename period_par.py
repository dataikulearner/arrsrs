# DSS Python recipe: axes_periods_par for SRS Arrimage
#   input  : folder detect_config (flows/arrimage_params.json)
#   output : axes_periods_par, one row per (scope, version, period, axes) = one SP_agg task
import json

import dataiku
import pandas as pd

from flows.arrimage import params as ap

with dataiku.Folder("detect_config").get_download_stream("flows/arrimage_params.json") as stream:
    params = json.load(stream)
ap.validate_params(params)

rows = []
for scope, version in ap.list_runs(params):                   # (stda, v2x1), (stda, v3x1), (sgcp, latest)
    run = ap.resolve_run_params(params, scope, version)
    periods = sorted(set(run["periods_str"].split("/")), reverse=True)[:run["nb_past_quarters"]]
    for period in periods:
        for axe in run["axes_str"].split("/"):
            rows.append({"scope_run_clone": scope, "uds_version_clone": version,
                         "id_set_params_clone": run["id_set_params"], "period_run_clone": periods[0],
                         "axe_clone": axe, "period_clone": period})

par = pd.DataFrame(rows)
print(par.groupby(["scope_run_clone", "uds_version_clone"]).size())
dataiku.Dataset("axes_periods_par").write_with_schema(par)
