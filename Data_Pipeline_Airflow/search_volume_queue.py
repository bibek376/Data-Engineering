import json
import pandas as pd
from airflow import DAG
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import calendar
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


default_args={
    'owner':'Bibek Rawat',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=45)
}

def date_time() -> json:
    """Function To calculate Previous month First and Last Data "YYYY-MM-DD" Format
    """
    try:
        # Get the current date
        current_date = datetime.now().date()
        # Subtract one month
        one_month_ago = current_date - relativedelta(months=1)

        #print("Current date:", current_date)
        #print("Date one month ago:", one_month_ago)

        year=str(one_month_ago).split('-')[0]
        month=str(one_month_ago).split('-')[1]

        # Get the first and last day of the month
        first, last = calendar.monthrange(int(year), int(month))

        # Create datetime objects for the first and last day of the month
        first_day = datetime(int(year), int(month), 1)
        last_day = datetime(int(year), int(month), last)

        print(f"date_from: {first_day}, date_to: {last_day}")

        return {
            'date_from': first_day.strftime('%Y-%m-%d'),
            'date_to': last_day.strftime('%Y-%m-%d')
        }

    except Exception as e:
        print(f"Got unexcepted error:", {e})


def populate_queue_table(**kwargs) -> None:
    """Function to populate queue table with data from flightdeck table"""
    try:
        ti = kwargs["ti"]

        # Retrieve date_from and date_to from XCom
        date_info = ti.xcom_pull(task_ids='calculate_date_time_tk')

        # Validate the returned data
        if not date_info:
            raise ValueError("No data received from calculate_date_time_tk task via XCom")

        date_from = date_info.get('date_from')
        date_to = date_info.get('date_to')

        if not date_from or not date_to:
            raise ValueError("Invalid date information received from XCom")

        print(f"date_from: {date_from}, date_to: {date_to}")

        mssql_hook = MsSqlHook(mssql_conn_id='internaltoolset_mssqls')

        sql_queue_stmt = f"""INSERT INTO  algo.AlgoSearchVolumeQueue (
                                client_id, date_from, date_to, run_date, completion_date,
                                country_code, search_engine_id, search_category, frequency, status_code)
                             SELECT
                                p.ClientId as client_id,
                                CAST('{date_from}' as date) as date_from,
                                CAST('{date_to}' as date) as date_to,
                                CAST(GETDATE() AS DATE) as run_date,
                                null as completion_date,
                                w.countrycode as country_code,
                                w.searchengineID as search_engine_id,
                                'LocalSearchVolume' as search_category,
                                'M' as frequency,
                                0 as status_code
                             FROM  algo.Projects p
                             INNER JOIN  web.websitesearchengines w
                             ON p.ClientId = w.client_id
                             WHERE p.IsActive = 1;"""

        print("Executing SQL statement:", sql_queue_stmt)
        mssql_hook.run(sql=sql_queue_stmt)

    except Exception as e:
        print(f"Got unexpected error: {e}")

# Define the DAG
with DAG(dag_id = "search_volume_queue",
        start_date = datetime(2024, 1, 1),
        schedule_interval = None,
        default_args = default_args,
        tags = ['SearchVolumeQueue', 'Python'],
        catchup = True,
        description = 'Populate Queue Table'
        ) as dag:
        # Add the date_time function as a PythonOperator
        calculate_date_time = PythonOperator(
            task_id = "calculate_date_time_tk",
            python_callable = date_time
        )
        # A task to read the data from a FD db to populate
        read_queue_data = PythonOperator(
            task_id = "populate_queue_table_tk",
            python_callable = populate_queue_table
        )

calculate_date_time >> read_queue_data

