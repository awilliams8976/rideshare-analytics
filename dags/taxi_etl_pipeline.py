import datetime
import os
import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

def verify_link(
    date
):
    """
    Verify if link to yellow taxi parquet files from the NYC Taxi & Limousine Commission
    (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is valid.

    :param date: Date to retrieve yellow taxi parquet file
    :return: date
    """
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    date_str = date.date().strftime("%Y-%m")
    file = "yellow_tripdata_%s.parquet" % date_str

    request = requests.get(url+file, stream=True)

    if request.status_code < 400:
        return date
    else:
        return None


with DAG(
    dag_id="tax_etl_pipeline",
    description="Pipeline that extracts, transforms, and loads taxi trip data to a data model in PostgreSQL.",
    start_date=datetime(year=2022,month=1,day=1),
    schedule_interval=None,
    catchup=True
) as dag:
    
    start = dag.start_date.date()
    dates = [date.date() for date in pd.date_range(start,datetime.today(),freq="MS").tolist() if verify_link(date) != None]

    if dag.catchup:
        dates = dates
    else:
        dates = [max(dates)]

    spark_script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "etl"))
    extract_tasks = []

    start = EmptyOperator(
        task_id="start_pipeline"
    )

    for date in dates:
        extract = SparkSubmitOperator(
            task_id="extract_taxi_data_%s" % date.strftime("%Y-%m"),
            conn_id="spark",
            application=os.path.join(spark_script_dir,"extract.py"),
            application_args=["%s" % date]
        )

        extract_tasks.append(extract)

    transform = SparkSubmitOperator(
        task_id="transform_taxi_data",
        conn_id="spark",
        application=os.path.join(spark_script_dir, "transform.py")
    )

    load = SparkSubmitOperator(
        task_id="load_taxi_data",
        conn_id="spark",
        application=os.path.join(spark_script_dir, "load.py")
    )

    end = EmptyOperator(
        task_id="end_pipeline"
    )

    start >> extract_tasks >> transform >> load >> end
