from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import TaskInstance

default_args = {
    'owner': 'Akshit',
    'start_date': datetime(2023, 1, 1),
}

def fail_task():
    raise ValueError("This task is deliberately failing.")

def fail_check(**context):
    ti = context['ti']
    task_instance = ti.get_task_instance()

    failed_upstream_tasks = []
    for task in task_instance.task.get_flat_relatives(upstream=True):
        upstream_ti = TaskInstance(task=task, execution_date=ti.execution_date)
        if upstream_ti.current_state() == "failed":
            failed_upstream_tasks.append(upstream_ti.task_id)
    print(failed_upstream_tasks)
    if failed_upstream_tasks:
        raise ValueError(f"Failed upstream tasks: {failed_upstream_tasks}")

with DAG(
    'trigger_rules_POC',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
) as dag:
    
    start = EmptyOperator(
        task_id='start',
    )

    fail = PythonOperator(
        task_id='fail_task',
        python_callable=fail_task,
    )

    intermediate = EmptyOperator(
        task_id='intermediate',
        trigger_rule='all_done',  # This task runs regardless of the success or failure of previous tasks
    )

    fail_check = PythonOperator(
        task_id = "fail_check",
        python_callable=fail_check,
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed',  # This task runs only if all upstream tasks have succeeded
    )

    start >> fail >> intermediate >> fail_check >> end
