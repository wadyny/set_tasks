from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


with DAG(
    dag_id="dwh_main_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["airline_dwh"],
) as dag:
    process_raw_to_staging = SnowflakeOperator(
        task_id="process_raw_to_staging",
        sql=[
            "USE WAREHOUSE COMPUTE_WH;",
            "CALL AIRLINE_DWH.STAGING.sp_process_raw_to_staging();",
        ],
        snowflake_conn_id="AIRFLOW_CONN_SNOWFLAKE_DEFAULT",
    )

    process_staging_to_analytics = SnowflakeOperator(
        task_id="process_staging_to_analytics",
        sql=[
            "USE WAREHOUSE COMPUTE_WH;",
            "CALL AIRLINE_DWH.ANALYTICS.sp_process_staging_to_analytics();",
        ],
        snowflake_conn_id="AIRFLOW_CONN_SNOWFLAKE_DEFAULT",
    )

    process_raw_to_staging >> process_staging_to_analytics