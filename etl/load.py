import datetime
import glob
import logging
import os
from airflow.hooks.base import BaseHook
from configparser import ConfigParser
from functools import reduce
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def load_data(
    taxi_dict: dict
):
    """
    Loads yellow taxi dimension tables and fact table DataFrames to postgreSQL database.

    :param taxi_dict: Dictionary of taxi dimension tables and fact table PySpark DataFrames
    :return: str
    """

    hook = BaseHook.get_connection("taxi_db")
    log.info("""------------------------- %s HOOK ESTABLISHED -------------------------""" % hook.conn_id.upper())

    config = {
        "host": hook.host,
        "database": hook.schema,
        "login": hook.login,
        "password": hook.password,
        "port": hook.port,
        "url":"jdbc:postgresql://%s:%s/%s" % (hook.host, hook.port, hook.schema),
        "driver":"org.postgresql.Driver"
    }
    log.info("""------------------------- DATABASE CONFIG DICTIONARY CREATED: %s -------------------------""" % config)

    for k,v in taxi_dict.items():
        log.info("%s ROWS TO LOAD TO %s.%s TABLE FROM %s DATAFRAME ATTEMPT" % (v.count(), config["database"].upper(), k.upper(), k.upper()))
        df_writer = DataFrameWriter(v)
        log.info("""------------------------- %s DATAFRAME WRITTEN -------------------------""" % k.upper())

        df_writer.jdbc(url=config["url"],
                       table=k,
                       mode="overwrite",
                       properties={"user":config["login"],"password": config["password"],"driver": config["driver"]}
                       )
        log.info("%s ROWS LOADED TO %s.%s TABLE FROM %s DATAFRAME SUCCESSFULLY" % (v.count(), config["database"].upper(), k.upper(), k.upper()))

    return log.info("ALL ROWS SUCCESSFULLY LOADED TO %s DATABASE IN POSTGRESQL" % config["database"].upper())


if __name__ == "__main__":
    start = datetime.datetime.today()
    log.info("""------------------------- START SCRIPT -------------------------""")

    spark = (SparkSession.builder
             .master("local")
             .config("spark.driver.memory", "15g")
             .appName("Load yellow taxi data to PostgreSQL")
             .getOrCreate())
    log.info("""------------------------- SPARK SESSION CREATED -------------------------""")

    ext = "*.csv"

    dm_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data/processed"))
    log.info("""------------------------- DATA MODEL FILES PATHS -------------------------""")

    df_dict = {
        "vendor_dim":reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                             for file in glob.glob(os.path.join(path,"vendor_dim",ext))]),
        "datetime_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                for file in glob.glob(os.path.join(path,"datetime_dim",ext))]),
        "store_fwd_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                 for file in glob.glob(os.path.join(path,"store_fwd_dim",ext))]),
        "passenger_count_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                       for file in glob.glob(os.path.join(path,"passenger_count_dim",ext))]),
        "trip_distance_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                     for file in glob.glob(os.path.join(path,"trip_distance_dim",ext))]),
        "rate_code_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                 for file in glob.glob(os.path.join(path,"rate_code_dim",ext))]),
        "pu_location_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                   for file in glob.glob(os.path.join(path,"pu_location_dim",ext))]),
        "do_location_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                   for file in glob.glob(os.path.join(path,"do_location_dim",ext))]),
        "payment_type_dim": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                                    for file in glob.glob(os.path.join(path,"payment_type_dim",ext))]),
        "taxi_fact": reduce(DataFrame.union,[spark.read.csv(file,header=True) for path,dir_,files in os.walk(dm_path)
                                             for file in glob.glob(os.path.join(path,"taxi_fact",ext))])
    }
    log.info("""------------------------- DATA MODEL DATAFRAMES CREATED -------------------------""")

    load_data(df_dict)
    log.info("""------------------------- DATA MODEL DATAFRAMES WRITTEN TO DATABASE -------------------------""")
    end = datetime.datetime.today()
    log.info("""------------------------- ELAPSED TIME: %s SECONDS -------------------------""" % (end-start).seconds)
    log.info("""------------------------- END SCRIPT -------------------------""")
    