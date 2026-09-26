"""
financial_impact.py

Calculates illustrative financial impact of the A/B test result
under three traffic scenarios (pessimistic / base / optimistic).

IMPORTANT: This is NOT a result from the dataset. It is an illustrative
calculation based on assumed business parameters.
"""

import pandas as pd
from sqlalchemy import create_engine

# Assumptions (illustrative)
CONVERSION_A = 0.0540  # from ab_test_history
CONVERSION_B = 0.1407  # from ab_test_history
AOV_GBP = 50           # assumed average order value

SCENARIOS = {
    "pessimistic": 50_000,
    "base":       100_000,
    "optimistic": 500_000,
}

def calculate_scenario(traffic: int, conv_a: float, conv_b: float, aov: float) -> dict:
    orders_a = traffic * conv_a
    orders_b = traffic * conv_b
    revenue_a = orders_a * aov
    revenue_b = orders_b * aov
    return {
        "traffic": traffic,
        "orders_a": round(orders_a),
        "orders_b": round(orders_b),
        "revenue_a_gbp": round(revenue_a),
        "revenue_b_gbp": round(revenue_b),
        "additional_monthly_gbp": round(revenue_b - revenue_a),
        "additional_annual_gbp": round((revenue_b - revenue_a) * 12),
    }

def main():
    print("=" * 70)
    print("ILLUSTRATIVE FINANCIAL IMPACT OF A/B TEST")
    print("=" * 70)
    print(f"Assumptions: AOV = £{AOV_GBP}, conversion A = {CONVERSION_A:.2%}, B = {CONVERSION_B:.2%}")
    print()
    
    results = []
    for name, traffic in SCENARIOS.items():
        r = calculate_scenario(traffic, CONVERSION_A, CONVERSION_B, AOV_GBP)
        r["scenario"] = name
        results.append(r)
    
    df = pd.DataFrame(results)
    print(df.to_string(index=False))
    print()
    print("⚠️  These figures are illustrative and based on assumed parameters.")
    
    # Save to PostgreSQL
    engine = create_engine('postgresql://data_user:data_password@postgres_data:5432/my_dataset')
    df.to_sql('financial_impact', engine, if_exists='replace', index=False)
    print("✅ Results saved to financial_impact table")

if __name__ == "__main__":
    main()