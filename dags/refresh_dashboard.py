"""
DAG for automated company data refresh.
Data source: CSV file mounted into the container at /opt/airflow/data/company/data.csv
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def update_data():
    """Load CSV into PostgreSQL"""
    # Path to CSV inside the container (mounted from host)
    csv_path = '/opt/airflow/data/company/data.csv'

    # Connect to PostgreSQL
    engine = create_engine('postgresql://data_user:data_password@postgres_data:5432/my_dataset')

    try:
        # Load CSV
        df = pd.read_csv(csv_path)
        logger.info(f"📊 Loaded {len(df)} rows from CSV")

        # Save to PostgreSQL
        df.to_sql('company_data', engine, if_exists='replace', index=False)
        logger.info(f"✅ Data updated: {len(df)} records")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise

with DAG(
    'refresh_company_dashboard',
    default_args=default_args,
    description='Daily company data refresh from CSV',
    schedule_interval='0 9 * * *',  # Daily at 9:00 AM
    catchup=False,
    max_active_runs=1,
    tags=['company', 'etl'],
) as dag:

    update_task = PythonOperator(
        task_id='update_data',
        python_callable=update_data
    )

    update_task