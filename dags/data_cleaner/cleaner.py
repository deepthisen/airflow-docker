import pandas as pd
import numpy as np
from datetime import timedelta
import os

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

dag_path = os.getcwd()

def data_cleaner():

    hotel_data = pd.read_csv(dag_path+'/raw_data/hotel_bookings.csv')
    cleaned_data = hotel_data.fillna(0)

    cleaned_data.to_csv(dag_path+'/processed_data/processed_hotel_bookings.csv',index=False)

def data_cleaned_msg():
    print('Done')

#Write default arguments that will be explicitly passed to each task's constructor
default_args = {'owner':'deepthisen'}

#We'll need an object that instantiates pipelines dynamically.
data_cleaning_dag = DAG(
    'data_cleaning_dag', #name of the dag displayed in airflow user interface (UI)
    default_args=default_args,#
    schedule_interval=timedelta(days=30), #The interval at which the dag should run
    start_date=airflow.utils.dates.days_ago(2), #The date from which the dag should start running
    catchup=False)

# Write the tasks
clean_data = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaner,
    dag=data_cleaning_dag)

message = PythonOperator(
    task_id='cleaned_data_message',
    python_callable=data_cleaned_msg,
    dag = data_cleaning_dag)

clean_data >> message
'''


from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'timeout': 300,
}

dag = DAG(
    'docker_ex8',
    default_args=default_args,
    description='Run Docker Container Example',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

run_docker_task = DockerOperator(
    task_id='run_docker_container',
    image='deepthisen/test:latest',
    force_pull =True,

    #command='your_command_inside_container',
    api_version='auto',  # Adjust based on your Docker version
    #volumes=['/path/on/host:/path/in/container'],d
    dag=dag,
    #command = ['python3','print_pund.py']

)

'''