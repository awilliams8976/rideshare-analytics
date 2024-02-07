import os
from configparser import ConfigParser
from extract import extract_taxi_data, extract_zone_data
from pyspark.sql import DataFrameWriter, SparkSession
from transform import transform_data


def load_data(
    taxi_dict: dict
):
    """
    Loads yellow taxi dimension tables and fact table DataFrames to postgreSQL database.

    :param taxi_dict: Dictionary of taxi dimension tables and fact table PySpark DataFrames
    :return: str
    """

    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","database.ini"))
    parser = ConfigParser()
    parser.read(config_path)

    config = {}
    if parser.has_section("postgresql"):
        params = parser.items("postgresql")
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception("Section %s not found in the %s file" % ("postgresql", config_path))

    config["url"] = "jdbc:postgresql://%s:5432/%s" % (config["host"], config["database"])
    config["driver"] = "org.postgresql.Driver"
    properties = {"user": config["user"], "password": config["password"], "driver": config["driver"]}

    for k,v in taxi_dict.items():
        print("%s rows to load to %s.%s table from %s DataFrame attempt" % (v.count(), config["database"], k, k))
        df_writer = DataFrameWriter(v)

        df_writer.jdbc(url=config["url"], table=k, mode="overwrite", properties=properties)
        print("%s rows to loaded to %s.%s table from %s DataFrame successfully" % (v.count(), config["database"], k, k))

    return "All rows successfully loaded to %s database in PostgreSQL" % config["database"]


if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local")
             .config("spark.driver.memory", "15g")
             .appName("Load yellow taxi data to PostgreSQL")
             .getOrCreate())

    taxi_data = extract_taxi_data(spark_session=spark, year=2022, month=1)
    zone_data = extract_zone_data(spark_session=spark)
    df_dict = transform_data(taxi_data, zone_data)

    load_data(df_dict)
