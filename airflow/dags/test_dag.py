from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'amira',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def test_task():
    from datetime import datetime  # Import inside the function
    print("=" * 50)
    print("✅ Airflow is working correctly!")
    print("=" * 50)
    print("Hello from TidalLine project!")
    print(f"Current time: {datetime.now()}")
    print("=" * 50)
    return "Task completed successfully!"

dag = DAG(
    'test_airflow_connection',
    default_args=default_args,
    description='Test DAG to verify Airflow is working',
    schedule_interval=None,  # Manually triggered only
    catchup=False,
    tags=['test', 'tidaline']
)

# Task 1: Print current date
print_date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

# Task 2: Test Python function
test_python = PythonOperator(
    task_id='test_python',
    python_callable=test_task,
    dag=dag
)

# Task 3: Test SFTP connection (to verify our connections work)
test_sftp = BashOperator(
    task_id='test_sftp',
    bash_command='sshpass -p "batchpass" ssh -o StrictHostKeyChecking=no batch@sftp "ls -la /home/batch/data"',
    dag=dag
)

# Task 4: Test PostgreSQL connection
test_postgres = BashOperator(
    task_id='test_postgres',
    bash_command='PGPASSWORD=postgres psql -h postgres -U postgres -d maritime_logistics -c "SELECT COUNT(*) FROM seismic_events;"',
    dag=dag
)

# Task 5: Test HDFS connection (using itvdelab)
test_hdfs = BashOperator(
    task_id='test_hdfs',
    bash_command='ssh itversity@itvdelab "hdfs dfs -ls /"',
    dag=dag
)

# Define task dependencies
print_date >> test_python >> [test_sftp, test_postgres, test_hdfs]