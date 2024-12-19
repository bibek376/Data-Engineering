import requests
from requests.exceptions import HTTPError, RequestException
from airflow.providers.trino.hooks.trino import TrinoHook
import json
import pandas as pd
from airflow import DAG
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import calendar
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.python import BranchPythonOperator
from requests.exceptions import HTTPError
import time
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.exceptions import AirflowException
from trino.exceptions import TrinoUserError


default_args={
    'owner':'Bibek Rawat',
    'email': ["bibekrawat123@gmail.com"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=45)
}


def populate_sov_queue_table() -> None:
    """Function to populate queue table with data from flightdeck table"""
    try:

        mssql_hook = MsSqlHook(mssql_conn_id='internaltoolset_mssqls')

        is_brand_values = [1, 0, None]
        for is_brand in is_brand_values:
            # Handle None for is_brand and use 'NULL' in SQL if is_brand is None
            is_brand_sql = 'NULL' if is_brand is None else int(is_brand)

            sql_queue_stmt = f"""INSERT INTO algo.AlgoSovQueue (
                                    client_id, date_from, date_to, domain_id, run_date, 
                                    completion_date, is_brand, country_code, search_engine_id, 
                                    frequency, status_code)
                                    SELECT
                                        p.ClientId as client_id ,
                                        CAST({Variable.get('ALGO_RESET_DATE_FROM')} as date) as date_from,
                                        CAST({Variable.get('ALGO_RESET_DATE_TO')} as date) as date_to,
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
                                    AND p.ClientId = {Variable.get('ALGO_RESET_CLIENT_ID')};
                                    """
            
            mssql_hook.run(sql=sql_queue_stmt)

    except Exception as e:
        print(f"Got unexpected error: {e}")

def populate_sev_queue_table() -> None:
    """Function to populate queue table with data from flightdeck table"""
    try:

        mssql_hook = MsSqlHook(mssql_conn_id='internaltoolset_mssqls')

        sev = [1,3,5,10]
        for sev in sev:
            sql_queue_stmt = f"""INSERT
                                INTO
                                algo.AlgoSevQueue (client_id,
                                client_url_id,
                                competitor_url_id,
                                url,
                                sev,
                                date_from,
                                date_to,
                                run_date,
                                completion_date,
                                country_code,
                                search_engine_id,
                                frequency,
                                status_code)
                           SELECT DISTINCT 
                                p.ClientId as client_id ,
                                tc.UrlId AS client_url_id,
                                tcc.domainID AS competitor_url_id,
                                x.Url as url,
                                {sev} as sev,
                                CAST({Variable.get('ALGO_RESET_DATE_FROM')} as date) as date_from,
                                CAST({Variable.get('ALGO_RESET_DATE_TO')} as date) as date_to,
                                CAST(GETDATE() AS DATE) as run_date,
                                null as completion_date,
                                w.countrycode AS country_code,
                                w.searchengineID AS search_engine_id,
                                'M' as frequency,
                                0 as status_code
                            FROM
                                 algo.Projects p
                            INNER JOIN web.websitesearchengines w 
                            ON
                                w.client_id = p.ClientId
                            INNER JOIN dbo.tbl_clients tc
                            ON
                                tc.client_id = p.ClientId
                            INNER JOIN dbo.tbl_competitors tcc 
                            ON
                                p.ClientId = tcc.client_id
                            INNER JOIN web.UrlList x
                            ON
                                x.UrlId = tcc.domainID
                            WHERE
                                p.IsActive = 1
                            AND p.ClientId = {Variable.get('ALGO_RESET_CLIENT_ID')};
                            UNION ALL
                            SELECT DISTINCT 
                                p.ClientId as client_id ,
                                tc.UrlId AS client_url_id,
                                tc.UrlId AS competitor_url_id,
                                x.Url as url,
                                {sev} as sev,
                                CAST({Variable.get('ALGO_RESET_DATE_FROM')} as date) as date_from,
                                CAST({Variable.get('ALGO_RESET_DATE_TO')} as date) as date_to,
                                CAST(GETDATE() AS DATE) as run_date,
                                null as completion_date,
                                w.countrycode AS country_code,
                                w.searchengineID AS search_engine_id,
                                'M' as frequency,
                                0 as status_code
                            FROM
                                 algo.Projects p
                            INNER JOIN web.websitesearchengines w 
                            ON
                                w.client_id = p.ClientId
                            INNER JOIN dbo.tbl_clients tc
                            ON
                                tc.client_id = p.ClientId
                            INNER JOIN web.UrlList x
                            ON
                                x.UrlId = tc.UrlId 
                            WHERE
                                p.IsActive = 1
                            AND p.ClientId = {Variable.get('ALGO_RESET_CLIENT_ID')};"""
            
            mssql_hook.run(sql=sql_queue_stmt)


    except Exception as e:
        print(f"Got unexcepted error:", {e})


def populate_search_voluem_queue_table() -> None:
    """Function to populate queue table with data from flightdeck table"""
    try:
        mssql_hook = MsSqlHook(mssql_conn_id='internaltoolset_mssqls')

        sql_queue_stmt = f"""INSERT
                                    INTO
                                     algo.AlgoSearchVolumeQueue (client_id,
                                    date_from,
                                    date_to,
                                    run_date,
                                    completion_date,
                                    country_code,
                                    search_engine_id,
                                    search_category,
                                    frequency,
                                    status_code)                                
                                SELECT
                                    p.ClientId as client_id,
                                    CAST({Variable.get('ALGO_RESET_DATE_FROM')} as date) as date_from,
                                    CAST({Variable.get('ALGO_RESET_DATE_TO')} as date) as date_to,
                                    CAST(GETDATE() AS DATE) as run_date,
                                    null as completion_date,
                                    w.countrycode as country_code,
                                    w.searchengineID as search_engine_id,
                                    'LocalSearchVolume' as search_category,
                                    'M' as frequency,
                                    0 as status_code
                                FROM
                                     algo.Projects p
                                INNER JOIN  web.websitesearchengines w
                                ON
                                    p.ClientId = w.client_id
                                WHERE p.IsActive = 1
                                    AND p.ClientId = {Variable.get('ALGO_RESET_CLIENT_ID')};"""
        
        mssql_hook.run(sql=sql_queue_stmt)

    except Exception as e:
        print(f"Got unexcepted error:", {e})


# Define the DAG
with DAG(dag_id = "reset_queue_data",
        start_date = datetime(2024, 1, 1),
        schedule_interval = None,
        default_args = default_args,
        tags = ['ResetGSCQueue','DateFrom','ClientID','Python'],
        catchup = True,
        description = 'Populate Queue Table'
        ) as dag:
        start = DummyOperator(task_id='start')
        # Add the date_time function as a GlueJobOperator
        algo_gsc_queue_reset_date_task = GlueJobOperator(
            task_id="algo_gsc_queue_reset_date_tk",
            job_name="algo_gsc_queue_reset_date",
            script_args={"--algo_reset_client_id": f"{Variable.get('ALGO_RESET_CLIENT_ID')}",
                        "--algo_reset_date_from": f"{Variable.get('ALGO_RESET_DATE_FROM')}"}
        )
        algo_ga_queue_reset_date_task = GlueJobOperator(
            task_id="algo_ga_queue_reset_date_tk",
            job_name="algo_ga_queue_reset_date",
            script_args={"--algo_reset_client_id": f"{Variable.get('ALGO_RESET_CLIENT_ID')}",
                        "--algo_reset_date_from": f"{Variable.get('ALGO_RESET_DATE_FROM')}",
                        "--algo_reset_date_to" : f"{Variable.get('ALGO_RESET_DATE_TO')}"}
        )
        algo_pytrends_queue_reset_date_task = GlueJobOperator(
            task_id="algo_pytrends_queue_reset_date_tk",
            job_name="algo_pytrends_queue_reset_date",
            script_args={"--algo_reset_client_id": f"{Variable.get('ALGO_RESET_CLIENT_ID')}",
                        "--algo_reset_date_from": f"{Variable.get('ALGO_RESET_DATE_FROM')}"}
        )
        populate_search_voluem_queue_table_tk = PythonOperator(
            task_id = "algo_search_volume_queue_reset_date_tk",
            python_callable = populate_search_voluem_queue_table
        )        
        populate_sev_queue_table_task = PythonOperator(
            task_id = "populate_sev_queue_reset_date_tk",
            python_callable = populate_sev_queue_table
        )        
        populate_sov_queue_table_task = PythonOperator(
            task_id = "populate_sov_queue_reset_date_tk",
            python_callable = populate_sov_queue_table
        )        
        end = DummyOperator(task_id='end')


start >> [algo_gsc_queue_reset_date_task,algo_ga_queue_reset_date_task,algo_pytrends_queue_reset_date_task,
          populate_search_voluem_queue_table_tk,populate_sev_queue_table_task,populate_sov_queue_table_task] >> end 




