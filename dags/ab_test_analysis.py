"""
DAG for automated A/B Testing Analysis
"""
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_ab_test_data(**context):
    engine = create_engine('postgresql://data_user:data_password@postgres_data:5432/my_dataset')
    query = "SELECT user_id, group_type, page_views, time_spent, conversion, device, location FROM user_sessions WHERE group_type IN ('A', 'B')"
    df = pd.read_sql(query, engine)
    logger.info(f"✅ Extracted {len(df)} records")
    context['ti'].xcom_push(key='ab_data', value=df.to_json())
    return len(df)

def calculate_statistical_significance(**context):
    from scipy import stats
    ti = context['ti']
    df_json = ti.xcom_pull(key='ab_data', task_ids='extract_ab_test_data')
    df = pd.read_json(df_json)
    group_a = df[df['group_type'] == 'A']
    group_b = df[df['group_type'] == 'B']
    t_stat_time, p_val_time = stats.ttest_ind(group_a['time_spent'], group_b['time_spent'], equal_var=False)
    t_stat_views, p_val_views = stats.ttest_ind(group_a['page_views'], group_b['page_views'], equal_var=False)
    conversion_yes = [(group_a['conversion'] == 'Yes').sum(), (group_b['conversion'] == 'Yes').sum()]
    conversion_no = [(group_a['conversion'] == 'No').sum(), (group_b['conversion'] == 'No').sum()]
    contingency_table = pd.DataFrame([conversion_yes, conversion_no])
    chi2, p_val_chi2, dof, expected = stats.chi2_contingency(contingency_table)
    conv_rate_a = (group_a['conversion'] == 'Yes').mean()
    conv_rate_b = (group_b['conversion'] == 'Yes').mean()
    recommendation = "INCONCLUSIVE"
    if p_val_chi2 < 0.05:
        recommendation = "ROLL_OUT" if conv_rate_b > conv_rate_a else "DO_NOT_ROLL_OUT"
    results = {
        'timestamp': datetime.now().isoformat(),
        'group_a_users': len(group_a),
        'group_b_users': len(group_b),
        'time_p_value': float(p_val_time),
        'views_p_value': float(p_val_views),
        'conversion_p_value': float(p_val_chi2),
        'conv_rate_a': float(conv_rate_a),
        'conv_rate_b': float(conv_rate_b),
        'recommendation': recommendation
    }
    logger.info(f"📊 Analysis complete: {recommendation}")
    context['ti'].xcom_push(key='ab_results', value=results)
    return results

def save_analysis_results(**context):
    from sqlalchemy import text
    engine = create_engine('postgresql://data_user:data_password@postgres_data:5432/my_dataset')
    ti = context['ti']
    results = ti.xcom_pull(key='ab_results', task_ids='calculate_statistical_significance')
    insert_query = """
    INSERT INTO ab_test_history (
        analysis_date, group_a_users, group_b_users,
        group_a_avg_time, group_b_avg_time,
        group_a_conversion_pct, group_b_conversion_pct,
        time_p_value, views_p_value, conversion_p_value,
        recommendation, raw_results
    ) VALUES (
        :analysis_date, :group_a_users, :group_b_users,
        :group_a_avg_time, :group_b_avg_time,
        :group_a_conversion_pct, :group_b_conversion_pct,
        :time_p_value, :views_p_value, :conversion_p_value,
        :recommendation, :raw_results
    )
    """
    with engine.connect() as conn:
        conn.execute(text(insert_query), {
            'analysis_date': results['timestamp'],
            'group_a_users': results['group_a_users'],
            'group_b_users': results['group_b_users'],
            'group_a_avg_time': 0,
            'group_b_avg_time': 0,
            'group_a_conversion_pct': results['conv_rate_a'] * 100,
            'group_b_conversion_pct': results['conv_rate_b'] * 100,
            'time_p_value': results['time_p_value'],
            'views_p_value': results['views_p_value'],
            'conversion_p_value': results['conversion_p_value'],
            'recommendation': results['recommendation'],
            'raw_results': json.dumps(results)
        })
        conn.commit()
    logger.info("✅ Results saved to ab_test_history")

with DAG(
    'ab_test_analysis',
    default_args=default_args,
    description='Automated A/B testing analysis',
    schedule_interval='0 8 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['ab_testing', 'analytics'],
) as dag:
    start = DummyOperator(task_id='start')
    extract = PythonOperator(task_id='extract_ab_test_data', python_callable=extract_ab_test_data, provide_context=True)
    analyze = PythonOperator(task_id='calculate_statistical_significance', python_callable=calculate_statistical_significance, provide_context=True)
    save = PythonOperator(task_id='save_analysis_results', python_callable=save_analysis_results, provide_context=True)
    end = DummyOperator(task_id='end')
    start >> extract >> analyze >> save >> end