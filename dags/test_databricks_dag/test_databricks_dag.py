from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperatorfrom 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 25)
}

dag = DAG(
    dag_id='test_dag',
    default_args=args,
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
    dag=dag
)

end = PythonOperator(
    task_id='test_end',
    python_callable=print_end,
    dag=dag
)

#######
## Create Databricks connection separately in the UI
## 
## Grab Databricks details from the ui on job / run / cluster
#######

job_id = 10

test_run_job = DatabricksRunNowOperator(
            task_id="databricks_run_now",
            databricks_conn_id="databricks_default",
            job_id=str(job_id)
        )