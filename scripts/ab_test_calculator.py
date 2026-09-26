"""
ab_test_calculator.py

Calculates statistical significance for an A/B test stored in the
`user_sessions` table (see project schema) and writes the result into
`ab_test_history`.

Metrics tested:
  - time_spent   (continuous)  -> Welch's t-test
  - page_views   (continuous)  -> Welch's t-test
  - conversion   (categorical) -> Chi-square test of independence

Also computes:
  - Minimum Detectable Effect (MDE) achieved by the current sample size
    for the conversion metric, given standard alpha/power assumptions.
  - A simple recommendation: "ROLL_OUT" / "DO_NOT_ROLL_OUT" / "INCONCLUSIVE"

Run manually:
    python scripts/ab_test_calculator.py

Run inside Airflow container:
    docker exec -it airflow python /opt/airflow/scripts/ab_test_calculator.py
"""

import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats
import psycopg2
from psycopg2.extras import Json

# ---------------------------------------------------------------------------
# Config — pulled from environment, falling back to docker-compose defaults
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "postgres_data"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "my_dataset"),
    "user": os.getenv("PG_USER", "data_user"),
    "password": os.getenv("PG_PASSWORD", "data_password"),
}

ALPHA = 0.05          # significance level
POWER = 0.80          # desired statistical power for MDE calculation


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_sessions(conn) -> pd.DataFrame:
    query = """
        SELECT user_id, group_type, page_views, time_spent, conversion,
               device, location
        FROM user_sessions
    """
    return pd.read_sql(query, conn)


def welch_t_test(group_a: pd.Series, group_b: pd.Series) -> dict:
    """Two-sided Welch's t-test (does not assume equal variances)."""
    stat, p_value = stats.ttest_ind(group_a, group_b, equal_var=False)
    return {
        "mean_a": float(group_a.mean()),
        "mean_b": float(group_b.mean()),
        "std_a": float(group_a.std()),
        "std_b": float(group_b.std()),
        "t_stat": float(stat),
        "p_value": float(p_value),
        "significant": bool(p_value < ALPHA),
    }


def chi_square_conversion(df: pd.DataFrame) -> dict:
    """Chi-square test of independence between group_type and conversion."""
    contingency = pd.crosstab(df["group_type"], df["conversion"])
    chi2, p_value, dof, expected = stats.chi2_contingency(contingency)

    conv_rate = (
        df.groupby("group_type")["conversion"]
        .apply(lambda s: (s == "Yes").mean())
    )

    return {
        "conversion_rate_a": float(conv_rate.get("A", np.nan)),
        "conversion_rate_b": float(conv_rate.get("B", np.nan)),
        "chi2_stat": float(chi2),
        "p_value": float(p_value),
        "significant": bool(p_value < ALPHA),
        "contingency_table": contingency.to_dict(),
    }


def minimum_detectable_effect(n_a: int, n_b: int, baseline_rate: float,
                               alpha: float = ALPHA, power: float = POWER) -> float:
    """
    Approximate MDE (absolute, in percentage points) for a two-proportion
    z-test, given the current sample sizes and a baseline conversion rate.
    """
    if n_a == 0 or n_b == 0 or baseline_rate in (0, 1):
        return float("nan")

    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_power = stats.norm.ppf(power)
    n_avg = 2 / (1 / n_a + 1 / n_b)

    mde = (z_alpha + z_power) * np.sqrt(2 * baseline_rate * (1 - baseline_rate) / n_avg)
    return float(mde)


def build_recommendation(time_result: dict, views_result: dict,
                          conv_result: dict) -> str:
    """
    Simple, transparent decision rule:
      - If conversion is significant AND directionally positive for B -> ROLL_OUT
      - If conversion is significant AND negative for B -> DO_NOT_ROLL_OUT
      - Otherwise -> INCONCLUSIVE
    """
    if not conv_result["significant"]:
        return "INCONCLUSIVE"

    lift = conv_result["conversion_rate_b"] - conv_result["conversion_rate_a"]
    return "ROLL_OUT" if lift > 0 else "DO_NOT_ROLL_OUT"


def save_results(conn, n_a: int, n_b: int, time_result: dict,
                  views_result: dict, conv_result: dict, mde: float,
                  recommendation: str):
    insert_query = """
        INSERT INTO ab_test_history (
            analysis_date, group_a_users, group_b_users,
            group_a_avg_time, group_b_avg_time,
            group_a_conversion_pct, group_b_conversion_pct,
            time_p_value, views_p_value, conversion_p_value,
            recommendation, raw_results
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    raw_results = {
        "time_spent": time_result,
        "page_views": views_result,
        "conversion": conv_result,
        "mde_conversion_pp": mde,
        "alpha": ALPHA,
        "power": POWER,
    }

    with conn.cursor() as cur:
        cur.execute(
            insert_query,
            (
                datetime.now(),
                n_a,
                n_b,
                time_result["mean_a"],
                time_result["mean_b"],
                conv_result["conversion_rate_a"] * 100,
                conv_result["conversion_rate_b"] * 100,
                time_result["p_value"],
                views_result["p_value"],
                conv_result["p_value"],
                recommendation,
                Json(raw_results),
            ),
        )
    conn.commit()

def srm_check(n_a: int, n_b: int, expected_ratio: float = 0.5) -> dict:
    """Sample Ratio Mismatch check."""
    total = n_a + n_b
    expected_a = total * expected_ratio
    expected_b = total * (1 - expected_ratio)
    chi2, p_value = stats.chisquare(f_obs=[n_a, n_b], f_exp=[expected_a, expected_b])
    return {
        "n_a": n_a,
        "n_b": n_b,
        "observed_ratio": round(n_a / total, 4),
        "expected_ratio": expected_ratio,
        "chi2_stat": round(float(chi2), 4),
        "p_value": round(float(p_value), 4),
        "srm_detected": bool(p_value < 0.001),
    }

def run_analysis():
    conn = get_connection()
    try:
        df = load_sessions(conn)


        group_a = df[df["group_type"] == "A"]
        group_b = df[df["group_type"] == "B"]

        # SRM check
        srm = srm_check(len(group_a), len(group_b))
        print(json.dumps({"srm_check": srm}, indent=2))

        time_result = welch_t_test(group_a["time_spent"], group_b["time_spent"])
        views_result = welch_t_test(group_a["page_views"], group_b["page_views"])
        conv_result = chi_square_conversion(df)

        mde = minimum_detectable_effect(
            n_a=len(group_a),
            n_b=len(group_b),
            baseline_rate=conv_result["conversion_rate_a"],
        )

        recommendation = build_recommendation(time_result, views_result, conv_result)

        save_results(
            conn, len(group_a), len(group_b),
            time_result, views_result, conv_result, mde, recommendation,
        )

        report = {
            "sample_size": {"group_a": len(group_a), "group_b": len(group_b)},
            "time_spent": time_result,
            "page_views": views_result,
            "conversion": conv_result,
            "mde_conversion_pp": round(mde * 100, 2) if not np.isnan(mde) else None,
            "recommendation": recommendation,
        }
        print(json.dumps(report, indent=2, default=str))
        return report

    finally:
        conn.close()


if __name__ == "__main__":
    run_analysis()