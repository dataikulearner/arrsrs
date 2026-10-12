# Flow parameters of SRS Arrimage: one file, one block per scope.
#
# In this flow a scope is a client use case (stda, sgcp) with its own axes, targets, thresholds
# and uds_version runs. It is not a line filter like the CCIRC scopes (US, Fortis, Japan...):
# every scope reads all the lines of uds_fac_arrime.
#
# resolve_run_params() turns (scope, uds_version) into a flat dict with the same shape as the
# CCIRC params, so that the core (pre_processing, models, post_processing) is called unchanged.
import json

SCOPES_KEY = "scopes"
RUNS_KEY = "runs"
SCOPE_ONLY_KEYS = ["label", RUNS_KEY]

# Keys every scope must define, on top of the common keys of the file
SCOPE_REQUIRED_KEYS = [
    "axes_str", "target_variables", "target_detection_mapping", "thresholds_segment_anomaly",
    "thresholds_segment_materiality", "thresholds_line_anomaly", "thresholds_negligibility",
]
RUN_REQUIRED_KEYS = ["uds_version", "periods_str", "nb_past_quarters"]


def load_params(path: str) -> dict:
    """Read the Arrimage params file and validate its structure"""
    with open(path) as f:
        params = json.load(f)
    validate_params(params)
    return params


def validate_params(params: dict):
    """Check the scopes and their runs; raise listing every problem found"""
    errors = []
    scopes = params.get(SCOPES_KEY) or {}
    if not scopes:
        errors.append("no scope defined")
    for scope, block in scopes.items():
        for key in SCOPE_REQUIRED_KEYS:
            if key not in block:
                errors.append("scope '{}': '{}' is missing".format(scope, key))
        runs = block.get(RUNS_KEY) or []
        if not runs:
            errors.append("scope '{}': no run defined".format(scope))
        versions = [run.get("uds_version") for run in runs]
        if len(versions) != len(set(versions)):
            errors.append("scope '{}': uds_version defined twice".format(scope))
        for run in runs:
            for key in RUN_REQUIRED_KEYS:
                if key not in run:
                    errors.append("scope '{}', run {}: '{}' is missing".format(scope, run.get("uds_version"), key))
        overlap = [k for k in block if k in params and k != SCOPES_KEY]
        if overlap:
            errors.append("scope '{}': keys {} are also defined at file level".format(scope, overlap))
    if errors:
        raise ValueError("arrimage params: " + "; ".join(errors))


def list_runs(params: dict) -> list:
    """All (scope, uds_version) pairs to compute"""
    return [(scope, run["uds_version"])
            for scope, block in params[SCOPES_KEY].items() for run in block[RUNS_KEY]]


def id_set_params(scope: str, uds_version: str) -> str:
    """Identifier written in every output row, e.g. arrimage_stda_v2x1"""
    return "arrimage_{}_{}".format(scope, uds_version)


def resolve_run_params(params: dict, scope: str, uds_version: str) -> dict:
    """Flat params for one scope and one version, in the shape the core expects
    Args:
        params: dict, content of arrimage_params.json
        scope: str, e.g. "stda"
        uds_version: str, one of the versions of the scope's runs, e.g. "v2x1" or "latest"

    Returns:
        dict with the common keys, the scope keys and the run keys; "scopes" holds the
        current scope only, without line filter

    Raises:
        ValueError for an unknown scope or version
    """
    scopes = params[SCOPES_KEY]
    if scope not in scopes:
        raise ValueError("arrimage params: unknown scope '{}' (known: {})".format(scope, sorted(scopes)))
    block = scopes[scope]
    runs = {run["uds_version"]: run for run in block[RUNS_KEY]}
    if uds_version not in runs:
        raise ValueError("arrimage params: scope '{}' has no run for '{}' (known: {})".format(
            scope, uds_version, sorted(runs)))

    flat = {k: v for k, v in params.items() if k != SCOPES_KEY}
    flat.update({k: v for k, v in block.items() if k not in SCOPE_ONLY_KEYS})
    flat.update(runs[uds_version])
    flat["scope_run"] = scope
    flat["id_set_params"] = id_set_params(scope, uds_version)
    # The scope selects a configuration, not lines: no filter
    flat[SCOPES_KEY] = {scope: {"label": block.get("label", scope), "filters": {}}}
    return flat
