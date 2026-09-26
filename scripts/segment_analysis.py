"""
segment_analysis.py

Breaks down the A/B test effect by device and location.
Helps identify whether the treatment effect is uniform or concentrated in a segment.
"""

import pandas as pd
from sqlalchemy import create_engine
from scipy import stats

def analyze_segments(df: pd.DataFrame, segment_col: str) -> pd.DataFrame:
    results = []
    for segment in df[segment_col].dropna().unique():
        subset = df[df[segment_col] == segment]
        a = subset[subset.group_type == 'A']
        b = subset[subset.group_type == 'B']
        
        if len(a) == 0 or len(b) == 0:
            continue
        
        conv_a = (a['conversion'] == 'Yes').mean()
        conv_b = (b['conversion'] == 'Yes').mean()
        lift_pp = (conv_b - conv_a) * 100
        
        # Chi-square for this segment
        contingency = pd.crosstab(subset['group_type'], subset['conversion'])
        if contingency.shape == (2, 2):
            chi2, p_val, _, _ = stats.chi2_contingency(contingency)
        else:
            chi2, p_val = None, None
        
        results.append({
            'segment': segment,
            'n_a': len(a),
            'n_b': len(b),
            'conv_a_pct': round(conv_a * 100, 2),
            'conv_b_pct': round(conv_b * 100, 2),
            'lift_pp': round(lift_pp, 2),
            'p_value': round(p_val, 6) if p_val is not None else None,
            'significant': bool(p_val < 0.05) if p_val is not None else None,
        })
    return pd.DataFrame(results)

def main():
    engine = create_engine('postgresql://data_user:data_password@postgres_data:5432/my_dataset')
    df = pd.read_sql('SELECT * FROM user_sessions', engine)
    
    print("=" * 80)
    print("SEGMENT ANALYSIS: A/B TEST EFFECT BY DEVICE AND LOCATION")
    print("=" * 80)
    
    for col in ['device', 'location']:
        print(f"\n📱 Segment: {col.upper()}")
        print("-" * 80)
        result = analyze_segments(df, col)
        print(result.to_string(index=False))
    
    # Save to PostgreSQL
    device_result = analyze_segments(df, 'device')
    device_result['segment_type'] = 'device'
    location_result = analyze_segments(df, 'location')
    location_result['segment_type'] = 'location'
    
    combined = pd.concat([device_result, location_result], ignore_index=True)
    combined.to_sql('segment_analysis', engine, if_exists='replace', index=False)
    print("\n✅ Results saved to segment_analysis table")

if __name__ == "__main__":
    main()