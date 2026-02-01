from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="orders_city_count_5min_report",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",   # every 5 minutes
    catchup=False,
    tags=["hive", "report", "city"]
) as dag:

    city_wise_order_count = HiveOperator(
        task_id="city_wise_order_count_txt",
        hive_cli_conn_id="hive_conn",
        hql="""
        INSERT OVERWRITE LOCAL DIRECTORY
        '/tmp/orders_city_count_{{ ds_nodash }}_{{ ts_nodash }}'
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        SELECT
            city,
            count(order_id) as total_orders
        FROM project.orders_silver
        WHERE cast(event_time as timestamp) >=
              current_timestamp() - INTERVAL 120 MINUTES
        GROUP BY city;
        """
    )

    city_wise_order_count
