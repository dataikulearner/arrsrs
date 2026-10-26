# DSS notebook: recompute the expected value of the basic model and compare it with DETECT
#   basic model: expected(q) = median of the observed values of the up to NB_LAST previous rows of the series
#   series = t_variable x seg_axes x seg_axes_value x period_run x scope x id_set_parameters (one version)
#   Usage: 1) global check (share of rows that match)  2) detail of one segment, with the quarters of each window
import numpy as np
import pandas as pd

NB_LAST = 4                                              # params "nb_last"
SERIES = ["t_variable_clone", "seg_axes", "seg_axes_value", "period_run_clone", "scope_clone",
          "id_set_parameters_clone"]
REL_TOL = 1e-9


def recompute_expected(df, nb_last=NB_LAST):
    """Adds window_periods, window_values, expected_recomputed (basic model rows only, sorted by series and period)"""
    df = df[df["model_type"] == "basic"].sort_values(SERIES + ["period_clone"]).reset_index(drop=True)
    windows_p, windows_v, medians = [], [], []
    for _, s in df.groupby(SERIES, sort=False):
        periods, values = s["period_clone"].tolist(), s["observed_value"].tolist()
        for i in range(len(s)):
            p = periods[max(0, i - nb_last):i]
            v = [x for x in values[max(0, i - nb_last):i] if pd.notna(x)]
            windows_p.append(p)
            windows_v.append(v)
            medians.append(float(np.median(v)) if v else np.nan)
    df["window_periods"], df["window_values"], df["expected_recomputed"] = windows_p, windows_v, medians
    exp, rec = df["expected_value"], df["expected_recomputed"]
    df["match"] = (exp.isna() & rec.isna()) | np.isclose(exp, rec, rtol=REL_TOL, atol=1e-6)
    return df


def segment_detail(res, scope, target, seg_axes_value, seg_axes="entity", version=None):
    """One segment: each quarter with its window, recomputed and DETECT expected values"""
    m = ((res["scope_clone"] == scope) & (res["t_variable_clone"] == target)
         & (res["seg_axes"] == seg_axes) & (res["seg_axes_value"] == seg_axes_value))
    if version:
        m &= res["id_set_parameters_clone"] == version
    cols = ["id_set_parameters_clone", "period_clone", "observed_value", "window_periods",
            "expected_recomputed", "expected_value", "deviation", "anomaly", "match"]
    return res.loc[m, cols]


if __name__ == "__main__":
    import dataiku
    sp = dataiku.Dataset("SP_anomalies").get_dataframe()
    res = recompute_expected(sp)

    # 1. Global check
    print("rows checked: {}, match: {:.2%}".format(len(res), res["match"].mean()))
    print(res.groupby(["id_set_parameters_clone", "t_variable_clone"])["match"].mean().unstack().round(4))
    print(res.loc[~res["match"], SERIES + ["period_clone", "observed_value", "expected_value",
                                           "expected_recomputed", "window_periods"]].head(10).to_string())

    # 2. One segment, e.g. the client's question
    pd.set_option("display.width", 250)
    print(segment_detail(res, "sgcp", "mt_cal_rwa_tot", "PMA_08").to_string())
