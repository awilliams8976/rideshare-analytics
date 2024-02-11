import datetime
import logging
import os
import pandas as pd
import requests
import sys
from functools import reduce
from pyspark import SparkFiles
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import lit

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


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
    log.info("""------------------------- TAXI DATA URL CREATED -------------------------""")

    spark_session.sparkContext.addFile(yellow_data_url)
    log.info("""------------------------- TAXI FILE ADDED TO SPARK FILE SYSTEM -------------------------""")

    df = spark_session.read.parquet("file://"+SparkFiles.get(yellow_file))
    log.info("""------------------------- TAXI DATAFRAME CREATED -------------------------""")
    df = df.dropDuplicates()
    log.info("""------------------------- DUPLICATES DROPPED FROM TAXI DATAFRAME -------------------------""")

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
    log.info("""------------------------- ZONE DATA URL CREATED -------------------------""")

    spark_session.sparkContext.addFile(zone_url)
    log.info("""------------------------- ZONE FILE ADDED TO SPARK FILE SYSTEM -------------------------""")

    df = spark_session.read.csv("file://"+SparkFiles.get(zone_file),header=True)
    log.info("""------------------------- ZONE DATAFRAME CREATED -------------------------""")
    df = df.dropDuplicates()
    log.info("""------------------------- DUPLICATES DROPPED FROM TAXI DATAFRAME -------------------------""")

    return df


def retrieve_load_dates(
    start_date: datetime.date,
    end_date: datetime.date = datetime.datetime.utcnow().replace(day=1).date()
):
    """
    Generate a list of dates and times of active yellow taxi parquet files from the NYC Taxi & Limousine Commission
    (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

    :param start_date: Start date to retrieve yellow taxi parquet files
    :param end_date: End date to retrieve yellow taxi parquet files
    :return: list
    """
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    date_range = pd.date_range(start_date, end_date, freq="MS").tolist()
    log.info("""------------------------- DATE RANGE CREATED -------------------------""")

    file_range = []

    for date in date_range:
        date_str = date.strftime("%Y-%m")
        file = "yellow_tripdata_%s.parquet" % date_str

        request = requests.get(url+file, stream=True)
        log.info("""------------------------- REQUEST TO %S MADE -------------------------""" % (url+file))

        if request.status_code < 400:
            file_range.append(date)
            log.info("""------------------------- FILE DATE %s APPENDED TO DATE LIST -------------------------""" % date)
        else:
            log.info("""------------------------- FILE % DOES NOT EXIST -------------------------""" % file)

    return file_range


if __name__ == "__main__":
    start = datetime.datetime.today()
    log.info("""------------------------- START SCRIPT -------------------------""")

    date = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    log.info("""------------------------- PARAM %s FOUND -------------------------""" % date)
    
    spark = (SparkSession.builder
             .master("local")
             .config("spark.driver.memory", "15g")
             .appName("Extract yellow taxi data")
             .getOrCreate())
    log.info("""------------------------- SPARK SESSION CREATED -------------------------""")
    
    zone_df = extract_zone_data(spark_session=spark)
    log.info("""------------------------- ZONE DATAFRAME CREATED -------------------------""")

    # date_list = retrieve_load_dates(datetime.date(2023,10,1))
    # log.info("""------------------------- CREATED DATE RANGE LIST: %s -------------------------""" % date_list)

    # taxi_dfs = [extract_taxi_data(spark_session=spark,year=date_.year,month=date_.month) for date_ in date_list]
    # log.info("""------------------------- TAXI DATAFRAMES LIST CREATED -------------------------""")

    taxi_df = extract_taxi_data(spark_session=spark,year=date.year,month=date.month)
    log.info("""------------------------- TAXI DATAFRAME CREATED -------------------------""")

    taxi_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..","data/raw/%s" % date.strftime("%Y-%m")))
    zone_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..","data/zone"))
    log.info("""------------------------- OUTPUT FILES PATH -------------------------""")

    # taxi_df = reduce(DataFrame.union,taxi_dfs)
    # log.info("""------------------------- UNIONED TAXI DATAFRAMES -------------------------""")
    
    taxi_df.write.csv(taxi_path,mode="overwrite",header=True)
    log.info("""------------------------- TAXI DATAFRAME WRITTEN TO PATH -------------------------""")
    
    zone_df.write.csv(zone_path,mode="overwrite",header=True)
    log.info("""------------------------- ZONE DATAFRAME WRITTEN TO PATH -------------------------""")

    end = datetime.datetime.today()
    log.info("""------------------------- ELAPSED TIME: %s SECONDS -------------------------""" % (end-start).seconds)
    log.info("""------------------------- END SCRIPT -------------------------""")
