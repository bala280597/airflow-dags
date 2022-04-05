from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args={ 'start_date': datetime(2022,2,24)}
with DAG('taskgroup',schedule_interval='@daily',default_args=default_args,catchup=False):
	task1 = BashOperator(
			task_id='task1',
			bash_command='sleep 3'
	)

	with TaskGroup('processing_task') as processing_task:
				task2 = BashOperator(
				task_id='task2',
				bash_command='sleep 3'
				)

				task3 = BashOperator(
				task_id='task3',
				bash_command='sleep 3'
				)

	task4 = BashOperator(
			task_id='task4',
			bash_command='sleep 3'
	)

# task1 >> [task 2,task 3] >> task 4
task1 >> processing_task >> task4
