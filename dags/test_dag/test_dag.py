from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
  'owner': 'brian',
  'depends_on_past': False,
  'start_date': datetime(2021, 5, 25),
  'email': [''],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='test_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
)

def print_start():
    print('Start')
    return 'Logs'

def print_end():
    print('End')
    return 'Logs'


start = PythonOperator(
    task_id='test_start',
    python_callable=print_start,
)

end = PythonOperator(
    task_id='test_end',
    python_callable=print_end,
)

start >> end