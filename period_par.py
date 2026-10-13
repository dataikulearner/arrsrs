# DSS Python recipe: axes_periods_par for SRS Arrimage
#   input  : folder parameters_folder (flows/arrimage_params.json); uds_fac_arrime is listed by the code
#   output : axes_periods_par, partitioned by scope: one row per (scope, version, period, axes) = one SP_agg task
# Python recipe (no Spark): pre_processing is not imported, it needs pyspark
import json
import re

import dataiku
import pandas as pd

from flows.arrimage import params as ap

TABLE_PATH = "uds_fac_arrime"
LATEST = "latest"
VERSION_RE = re.compile(r"^v(\d+)x(\d+)$")

with dataiku.Folder("parameters_folder").get_download_stream("flows/arrimage_params.json") as stream:
    params = json.load(stream)
ap.validate_params(params)

# Partition ids of the source, e.g. "2026q2|v3x1" (ignore_flow: the dataset is not an input of the recipe)
partitions = [str(pid).strip().lower() for pid in dataiku.Dataset(TABLE_PATH, ignore_flow=True).list_partitions()]
print("{} partitions listed in {}".format(len(partitions), TABLE_PATH))


def exists(period, version):
    """The partition to read exists: the fixed version, or any vAxB version for latest"""
    prefix = period.strip().lower() + "|"
    versions = [pid[len(prefix):] for pid in partitions if pid.startswith(prefix)]
    if version == LATEST:
        return any(VERSION_RE.match(v) for v in versions)
    return version.lower() in versions


# Partition being built (axes_periods_par is partitioned by scope): only the runs of that scope
target_scope = dataiku.dku_flow_variables.get("DKU_DST_scope")
if target_scope is not None and target_scope not in params[ap.SCOPES_KEY]:
    raise ValueError("Partition scope = {!r}, expected one of {}".format(target_scope, sorted(params[ap.SCOPES_KEY])))

rows = []
for scope, version in ap.list_runs(params):     # (stda, v2x1), (stda, v3x1), (sgcp, latest)
    if target_scope is not None and scope != target_scope:
        continue
    run = ap.resolve_run_params(params, scope, version)
    candidates = sorted({p.strip().upper() for p in run["periods_str"].split("/")}, reverse=True)
    missing = [p for p in candidates if not exists(p, version)]
    if missing:
        print("WARNING {} {}: no partition for {}, skipped".format(scope, version, missing))
    periods = [p for p in candidates if exists(p, version)][:run["nb_past_quarters"]]
    if not periods:
        raise ValueError("{} {}: no partition for any period of periods_str".format(scope, version))
    for period in periods:
        for axe in run["axes_str"].split("/"):
            rows.append({"scope_run_clone": scope, "uds_version_clone": version,
                         "id_set_params_clone": run["id_set_params"], "period_run_clone": periods[0],
                         "axe_clone": axe, "period_clone": period})

par = pd.DataFrame(rows)
print(par.groupby(["scope_run_clone", "uds_version_clone"]).size())
dataiku.Dataset("axes_periods_par").write_with_schema(par)
