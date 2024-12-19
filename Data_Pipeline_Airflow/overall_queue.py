from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.python import BranchPythonOperator

default_args={
    'owner':'Bibek Rawat',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=45)
}

# Define the DAG
with DAG(dag_id = "algo_trigger_all_queue",
        start_date = datetime(2024, 1, 1),
        schedule_interval = None,
        default_args = default_args,
        tags = ['AllQueue','Queue','Algo',],
        catchup = True,
        description = 'Trigger All Queue Script'
        ) as dag:
    start = DummyOperator(task_id='start')
    algo_gsc_queue_task = GlueJobOperator(
        task_id="algo_gsc_queue_tk",
        job_name="algo_gsc_queue"
    )
    algo_ga_queue_task = GlueJobOperator(
        task_id="algo_ga4_queue_tk",
        job_name="algo_ga4_queue"
    )
    algo_pytrends_queue_task = GlueJobOperator(
        task_id="algo_pytrends_populate_queue_tk",
        job_name="algo_pytrends_populate_queue"
    )
    populate_search_voluem_queue_task = TriggerDagRunOperator(
        task_id='search_volume_queue_tk',
        trigger_dag_id='search_volume_queue', 
    )
    populate_sev_queue_task = TriggerDagRunOperator(
        task_id='populate_sev_queue_data_tk',
        trigger_dag_id='populate_sev_queue_data',
    )
    populate_sov_queue_task = TriggerDagRunOperator(
        task_id='populate_sov_queue_data_tk',
        trigger_dag_id='populate_sov_queue_data',
    )
    end = DummyOperator(task_id='end')


# Add dependencies if required
start >> [algo_gsc_queue_task,algo_ga_queue_task,algo_pytrends_queue_task,
          populate_search_voluem_queue_task,populate_sev_queue_task,populate_sov_queue_task] >> end 


