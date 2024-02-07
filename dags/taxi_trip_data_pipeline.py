import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract import extract_taxi_data,extract_zone_data
from etl.transform import transform_data
from etl.load import load_data
from etl.utils import retrieve_load_dates
from functools import reduce
from pyspark.sql import DataFrame,SparkSession




def extract(date:datetime,path:str):

    spark = (SparkSession.builder
             .master("local")
             .config("spark.driver.memory", "15g")
             .appName("Extract yellow_tripdata_%s.parquet" % date.strftime("%Y-%m"))
             .getOrCreate())

    zone_df = extract_zone_data(spark_session=spark)
    taxi_df = extract_taxi_data(spark_session=spark,year=date.year,month=date.month)
    taxi_df.write.csv(os.path.join(path,"yellow_taxi_trip_data"),mode="overwrite",header=True)
    zone_df.write.csv(os.path.join(path,"zone_data"),mode="overwrite",header=True)


with DAG(
    dag_id="taxi_trip_data_pipeline_dag",
    description="Pipeline that extracts, transforms, and loads taxi trip data to a data model in PostgreSQL.",
    start_date=datetime(year=2022,month=1,day=1),
    schedule_interval=None,
    catchup=False
) as dag:

    date_range = retrieve_load_dates(dag.start_date)
    processed_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","data/processed/"))

    for date_ in date_range:

        raw_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data/raw/%s" % date_))

        extract = PythonOperator(
            task_id="extract yellow_tripdata_%s.parquet" % date_.strftime("%Y-%m"),
            python_callable=extract,
            op_kwargs={
                "date":date_
            }
        )
