from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from extraction import run_extraction # here we import  the function from our etl script
from transformation import run_transformation
from loading import run_loading




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 27),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG(
    'real_estate_dag',
    default_args=default_args,
    description='My first etl code'
)

extraction = PythonOperator(
    task_id='extraction_layer',
    python_callable=run_extraction,
    dag=dag,
)

transformation = PythonOperator(
    task_id='transformation_layer',
    python_callable=run_transformation,
    dag=dag,
)
loading = PythonOperator(
    task_id='loading_layer',
    python_callable=run_loading,
    dag=dag,
)

extraction >> transformation >> loading