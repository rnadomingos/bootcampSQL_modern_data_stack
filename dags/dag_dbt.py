# dbtc_DfNtbbEsiU5y791mBIiTOmA7aPD36OOuT-2EEHsG-KZYxPRHDY
from airflow.decorators import dag, task
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

DBT_CLOUD_CONN_ID = "dbt-con"
JOB_ID = "70471823477385"

@dag(
    start_date=datetime(2025,6,23),
    schedule="@daily",
    catchup=False,
)

def running_dbt_cloud():
    rodar_dbt = DbtCloudRunJobOperator(
        task_id="rodar_dbt",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=60,
        timeout=360
    )

