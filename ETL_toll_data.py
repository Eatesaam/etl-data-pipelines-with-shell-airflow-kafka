# import the libraries

from datetime import timedelta, date
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Muhammad Eatesaam',
    'start_date': days_ago(0),
    'email': ['eatesaam@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the unzip_data task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='wget  https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz \
          tar -xvzf tolldata.tgz -C  /home/project/airflow/dags/finalassignment',
    dag=dag,
)

# define the extract_data_from_csv task
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command= 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# define the extract_data_from_tsv task
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut --output-delimeter="," -f5-7 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)

# define the extract_data_from_fixed_width task
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut --output-delimeter="," -c59-61,63-67 payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

# define the consolidate_data task
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# define the transform_data task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tar -xzf tolldata.tgz',
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data