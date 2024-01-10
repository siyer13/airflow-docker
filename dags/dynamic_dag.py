import os
import yaml

import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.operators.python import PythonOperator


my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.yaml")
with open(configuration_file_path) as yaml_file:
    configuration = yaml.safe_load(yaml_file)
    print(configuration)

def print_name():
    """Print the Airflow context and ds variable from the context."""
    print('siyer')

with DAG(
    dag_id="siyer_test_dag",
    start_date=datetime.datetime(2024, 1, 10),
    schedule="@daily",
):
    EmptyOperator(task_id="empty")



    @task(task_id="print_the_context")
    def print_context():
        """Print the Airflow context and ds variable from the context."""
        print(configuration)
        print('hello')
        return "Whatever you return gets printed in the logs"

    run_this = print_context()

    for task in configuration['tasks']:
        print(task['name'])
        run_this_too = PythonOperator(task_id=task['name'], python_callable=print_name)
        