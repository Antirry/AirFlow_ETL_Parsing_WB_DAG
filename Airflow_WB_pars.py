from airflow import DAG
from airflow.operators.python import PythonOperator
from NEW_VERSION_parts_dag_for_WB_pars.Google_load import load_CSV_to_bq
from NEW_VERSION_parts_dag_for_WB_pars.Object_Сlass_call import (
    _convert_res_API,
    _get_data_API,
    _names_cloud,
)
from pendulum import datetime, duration

# print(load_CSV_to_bq(**names_cloud))


def Create_Task(task_id: str, func_name: callable, op_args:list[any]) -> None:
    return PythonOperator(
        task_id=task_id,
        python_callable=func_name,
        op_args=op_args,
        provide_context=True,
    )


default_args = {
    'retries': 1,
    'retry_delay': duration(minutes=5),
}


with DAG(
    'CSV_to_bigquery_loader',
    default_args=default_args,
    description='A DAG to load CSV data into BigQuery',
    start_date=datetime(2024, 2, 9, tz='Europe/Moscow'),
    schedule_interval='0 19 * * *',  # At 19:00 every day
    catchup=False
) as dag:

    get_data_API = Create_Task('get_data_API',
                                _get_data_API,
                                ['Config.ini', 'Default', ['data', 'products']]
                            )

    convert_res_API = Create_Task('convert_output_API',
                                _convert_res_API,
                                [['id', 'priceU', 'feedbacks']]
                            )

    names_cloud = Create_Task('Read_Config_Cloud',
                            _names_cloud,
                            ['Config.ini', 'GoogleCloud']
                        )

    load_json_to_bq_task = PythonOperator(
            task_id='load_CSV_to_bq',
            python_callable=load_CSV_to_bq,
            provide_context=True,  # добавлено, если функции нужен контекст исполнения
        )

    # If there are more tasks, define them here and set up dependencies as needed
    # For example:
    # task1 >> load_json_to_bq_task >> task3

# Adjust the dataset_id, table_id, and data according to your needs

get_data_API >> convert_res_API >> names_cloud >> load_json_to_bq_task
