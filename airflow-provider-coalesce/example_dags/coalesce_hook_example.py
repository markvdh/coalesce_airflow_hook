from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from hooks import coalesce_hook

def start_coalesce_job(env_id,job_id):
    # initialize the Coalesce hook with 2 parameters:
    # 1 = Coalesce connection (should have the token in the password field)
    # 2 = the Snowflake connection, or "OAuth" for OAuth
    #    hook = CoalesceAPI("coalesce_default","OAuth")
    hook = coalesce_hook("coalesce_default","snowflake_default")
    hook.startRun(env_id, job_id)

default_args = {
    'start_date': datetime.strptime('2023-05-24 00:00:00', '%Y-%m-%d %H:%M:%S'),
    'retries': 0,
    'retry_delay': timedelta(minutes=5.0)
}

with DAG('coalesce_hook_example', tags=["coalesce"], default_args=default_args, schedule_interval=None) as dag:
    run_dim_refresh = PythonOperator(
        task_id="run_job",
        # enter the Coalesce environment ID and optionally a job ID
        # NOTE: the entire environment will refresh if no job ID is provided
        op_kwargs={'env_id': 65, 'job_id': 145 },
        python_callable=start_coalesce_job 
    )