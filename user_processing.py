from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
from pandas import json_normalize
from airflow.operators.bash import BashOperator

def _processing_user(ti):
  users = ti.xcom_pull(task_ids=['extracting_user'])

  if not len(users) or  'results' not in users[0]:
     raise ValueError('User is empty')
  user = users[0]['results'][0]
  processed_user=json_normalize({
    'firstname': user['name']['first'],
    'lastname': user['name']['last'],
    'country': user['location']['country'],
    'username': user['login']['username'],
    'password': user['login']['password'],
    'email': user['email']
  })
  processed_user.to_csv('/tmp/processed_user.csv',index=None,header=False)

default_args={ 'start_date': datetime(2022,2,24)}
dag = DAG(
    dag_id='user_processing-1',
    schedule_interval='@daily',
    default_args=default_args,
        catchup=False,
)

create_table_sqlite_task = SqliteOperator(
    task_id='create_table_sqlite',
    sqlite_conn_id='db_sqlite',
    sql=r"""
    CREATE TABLE users(
                email TEXT NOT NULL PRIMARY KEY,
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country Text NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL
    );
    """,
    dag=dag,
)

is_available = HttpSensor(
                          task_id='is_api_available',
                          http_conn_id='user_api',
                          endpoint='api/',
                          dag=dag,
)

extracting_user= SimpleHttpOperator(
                         task_id='extracting_user',
                         http_conn_id='user_api',
                         endpoint='api/',
                         method='GET',
                         response_filter= lambda response: json.loads(response.text),
                         log_response=True,
                          dag=dag,
)

processing_user = PythonOperator(
                  task_id='processing_user',
                  python_callable=_processing_user,
                  dag=dag,
)

storing_user = BashOperator(
        task_id='storing_user',
        bash_command=""" echo -e ".separator ","\n.import /tmp/processed_user.csv users" | sqlite3 /root/airflow/airflow.db """,
        dag=dag,
)

create_table_sqlite_task >> is_available >> extracting_user >> processing_user >> storing_user
				 
