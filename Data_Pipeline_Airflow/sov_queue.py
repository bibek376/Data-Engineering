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
    'email_on_failure': True,
    'email_on_retry': False,
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
        date_info = ti.xcom_pull(task_ids='populate_queue_table.calculate_date_time_tk')
        date_from = date_info['date_from']
        date_to = date_info['date_to']

        mssql_hook = MsSqlHook(mssql_conn_id='internaltoolset_mssqls')

        is_brand_values = [1, 0, None]
        for is_brand in is_brand_values:
            # Handle None for is_brand and use 'NULL' in SQL if is_brand is None
            is_brand_sql = 'NULL' if is_brand is None else int(is_brand)

            sql_queue_stmt = f"""INSERT INTO  algo.AlgoSovQueue (
                                    client_id, date_from, date_to, domain_id, run_date, 
                                    completion_date, is_brand, country_code, search_engine_id, 
                                    frequency, status_code)
                                    SELECT
                                        p.ClientId as client_id ,
                                        CAST('{date_from}' AS DATE) as date_from,
                                        CAST('{date_to}' AS DATE) as date_to,
                                        ul.DomainId as domain_id,
                                        CAST(GETDATE() AS DATE) as run_date,
                                        NULL as completion_date,
                                        {is_brand_sql} as is_brand,
                                        w.countrycode AS country_code,
                                        w.searchengineID as search_engine_id,
                                        'M' as frequency,
                                        0 as status_code
                                    FROM
                                         algo.Projects p
                                    INNER JOIN dbo.tbl_clients tc
                                    On
                                        p.ClientId = tc.client_id
                                    INNER JOIN web.UrlList ul 
                                    ON
                                        ul.UrlId = tc.UrlId
                                    INNER JOIN web.websitesearchengines w 
                                    ON
                                        w.client_id = tc.client_id
                                    WHERE
                                        p.IsActive = 1
                                    """
            
            mssql_hook.run(sql=sql_queue_stmt)

    except Exception as e:
        print(f"Got unexpected error: {e}")


# Define the DAG
with DAG(dag_id = "populate_sov_queue_data",
        start_date = datetime(2024, 1, 1),
        schedule_interval = None,
        default_args = default_args,
        tags = ['SOVQueue', 'Python'],
        catchup = True,
        description = 'Populate Queue Table'
        ) as dag:

    # First process_data TaskGroup
    with TaskGroup(
        group_id = "populate_queue_table",
        ui_color = "#28a745",
        ui_fgcolor = "#ffffff",
        tooltip = "This task group performs: queue table populate and make the data ready for procedure call",
    ) as t_populate_queue_table:

        # # Add the date_time function as a PythonOperator
        calculate_date_time = PythonOperator(
            task_id = "calculate_date_time_tk",
            python_callable = date_time
        )
        # A task to read the data from a FD db to populate
        read_queue_data = PythonOperator(
            task_id = "populate_queue_table_tk",
            python_callable = populate_queue_table
        )

        # Set the dependencies within the TaskGroup
        calculate_date_time >>  read_queue_data 

    # Define the dependencies
    # t_populate_queue_table >> t_populate_final_sev_data >> t_trigger_bronze_script >> t_trigger_silver_script
    t_populate_queue_table
