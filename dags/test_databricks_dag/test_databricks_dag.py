from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperatorfrom 
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
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

job_id = [10, 11, 12]
job_list = []

with TaskGroup(group_id=f'group1') as tg1:
    
    for id in job_id:
        job_list.append(
            DatabricksRunNowOperator(
                task_id="databricks_run_now"+str(id),
                databricks_conn_id="databricks_default",
                job_id=str(id),
                dag=dag
            )
        )

for item in job_list:
    start >> item
    item >> end 