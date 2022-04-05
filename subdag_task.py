from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag

default_args={ 'start_date': datetime(2022,2,24)}
dag = DAG(
    dag_id='parallel_dag',
    schedule_interval='@daily',
    default_args=default_args,
        catchup=False,
)

task1 = BashOperator(
                task_id='task1',
                bash_command='sleep 3',
                dag=dag
)

processing = SubDagOperator(
                task_id='parallel_subdag',
                subdag=subdag_parallel_dag('parallel_dag','parallel_subdag',default_args),
                dag=dag
)

task4 = BashOperator(
                task_id='task4',
                bash_command='sleep 3',
                dag=dag
)

# task1 >> [task 2.task 3] >> task 4
task1 >> processing >> task4
