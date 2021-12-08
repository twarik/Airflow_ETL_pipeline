from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from dhs_etl import extract1,extract2,extract3,transform,load

args = {
    'owner': 'twarik',
    'email': ['mtwarikharouna@gmail.com'],
}

with DAG(
    dag_id="hence_dag",
    default_args=args,
    description='ETL pipeline to load data from different sources into a Data warehouse',
    start_date=days_ago(0),
    schedule_interval="@once",
    tags=["hence"]
) as dag:
    extract_task1 = PythonOperator(task_id="Extract_from_csv_file",
                                    python_callable=extract1,
                                    provide_context=True)

    extract_task2 = PythonOperator(task_id="Extract_from_geojson",
                                    python_callable=extract2,
                                    provide_context=True)

    extract_task3 = PythonOperator(task_id="Extract_from_dhs_API",
                                    python_callable=extract3,
                                    provide_context=True)

    transform_task = PythonOperator(task_id="transform",
                                    python_callable=transform,
                                    provide_context=True)

    load_task = PythonOperator(task_id="load",
                                    python_callable=load,
                                    provide_context=True)

    visualise_task = BashOperator(task_id='visualize',
                                    bash_command='streamlit run /home/twarik/Desktop/airflow/files/app.py')

    [extract_task1, extract_task2, extract_task3] >> transform_task >> load_task >> visualise_task
