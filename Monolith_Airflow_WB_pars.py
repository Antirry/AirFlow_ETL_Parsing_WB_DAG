from airflow import DAG
from airflow.operators.python import PythonOperator
from OLD_VERSION_parts_dag_for_WB_pars.Object_Сlass_call import names_cloud
from pendulum import datetime, duration

from OLD_VERSION_Monolith_parts_dag_for_WB_pars.Google_load import load_CSV_to_bq

# print(load_CSV_to_bq(**names_cloud))

default_args = {
    'retries': 1,
    'retry_delay': duration(minutes=5),
}

dag = DAG(
    'CSV_to_bigquery_loader',
    default_args=default_args,
    description='A DAG to load CSV data into BigQuery',
    start_date=datetime(2024, 2, 9, tz='Europe/Moscow'),
    schedule_interval='0 19 * * *',  # At 19:00 every day
    catchup=False
)

load_json_to_bq_task = PythonOperator(
        task_id='load_CSV_to_bq',
        python_callable=load_CSV_to_bq,
        op_kwargs=names_cloud,
        provide_context=True,  # добавлено, если функции нужен контекст исполнения
        dag=dag
    )

    # If there are more tasks, define them here and set up dependencies as needed
    # For example:
    # task1 >> load_json_to_bq_task >> task3

# Adjust the dataset_id, table_id, and data according to your needs
