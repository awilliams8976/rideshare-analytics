import datetime
import os
import pandas as pd
from functools import reduce
from pyspark import SparkFiles
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import lit


def extract_taxi_data(
    spark_session: SparkSession,
    year: int,
    month: int
):
    """
    Downloads yellow taxi data for a specific year and month from the NYC Taxi & Limousine Commission
    (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to a PySpark DataFrame.

    :param spark_session: Apache Spark Session
    :param year: Year from which the file is to be downloaded
    :param month: Month (number) from which the file is to be downloaded
    :return: PySpark DataFrame
    """

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    date_str = datetime.date(year,month,1).strftime("%Y-%m")
    yellow_file = "yellow_tripdata_%s.parquet" % date_str
    yellow_data_url = url+yellow_file

    spark_session.sparkContext.addFile(yellow_data_url)

    df = spark_session.read.parquet("file://"+SparkFiles.get(yellow_file))
    df = df.dropDuplicates()
    df = df.withColumn("file_name",lit(yellow_file))

    return df


def extract_zone_data(
    spark_session: SparkSession
):
    """
    Downloads taxi zone data from the NYC Taxi & Limousine Commission
    (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to a PySpark DataFrame.

    :param spark_session: Apache Spark Session
    :return: PySpark DataFrame
    """

    url = "https://d37ci6vzurychx.cloudfront.net/misc/"
    zone_file = "taxi+_zone_lookup.csv"
    zone_url = url+zone_file

    spark_session.sparkContext.addFile(zone_url)

    df = spark_session.read.csv("file://"+SparkFiles.get(zone_file),header=True)
    df = df.dropDuplicates()

    return df


if __name__ == "__main__":

    spark = (SparkSession.builder
             .master("local")
             .config("spark.driver.memory", "15g")
             .appName("Extract yellow taxi data")
             .getOrCreate())

    zone_df = extract_zone_data(spark_session=spark)
    date_list = pd.date_range("2022-01-01","2022-01-31",freq="MS").tolist()
    taxi_dfs = []

    for date in date_list:
        df_ = extract_taxi_data(spark_session=spark,year=date.year,month=date.month)
        taxi_dfs.append(df_)

    path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","data/raw/"))

    taxi_df = reduce(DataFrame.union,taxi_dfs)
    taxi_df.write.csv(os.path.join(path,"yellow_taxi_trip_data"),mode="overwrite",header=True)
    zone_df.write.csv(os.path.join(path,"zone_data"),mode="overwrite",header=True)
