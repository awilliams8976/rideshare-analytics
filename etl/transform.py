import datetime
import glob
import logging
import os
import pyspark.sql.functions as f
import uuid
from functools import reduce
from itertools import chain
from pyspark.sql import DataFrame, SparkSession, types, Window

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def transform_data(
        taxi_df: DataFrame,
        zone_df: DataFrame
):
    """
    Performs data transformations on the yellow taxi DataFrame. Also prepares the data to be
    loaded into a data warehouse by returning a dictionary of dimensions and one fact table.

    :param taxi_df: Yellow taxi PySpark DataFrame
    :param zone_df: Taxi zone lookup PySpark DataFrame
    :return: dict
    """

    uuid_udf = f.udf(lambda: str(uuid.uuid4()), types.StringType())

    rename_cols_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id"
    }
    taxi_df = taxi_df.withColumnsRenamed(rename_cols_map)
    log.info("""------------------------- RENAME COLUMNS -------------------------""")

    cast_cols_map = {
        "vendor_id": taxi_df["vendor_id"].cast("int"),
        "passenger_count": taxi_df["passenger_count"].cast("int"),
        "rate_code": taxi_df["rate_code"].cast("int"),
        "pu_location_id": taxi_df["pu_location_id"].cast("int"),
        "do_location_id": taxi_df["do_location_id"].cast("int"),
        "payment_type": taxi_df["payment_type"].cast("int"),
        "trip_distance": taxi_df["trip_distance"].cast("float"),
        "fare_amount": taxi_df["fare_amount"].cast("float"),
        "extra": taxi_df["extra"].cast("float"),
        "mta_tax": taxi_df["mta_tax"].cast("float"),
        "tip_amount": taxi_df["tip_amount"].cast("float"),
        "tolls_amount": taxi_df["tolls_amount"].cast("float"),
        "improvement_surcharge": taxi_df["improvement_surcharge"].cast("float"),
        "total_amount": taxi_df["total_amount"].cast("float"),
        "congestion_surcharge": taxi_df["congestion_surcharge"].cast("float"),
        "airport_fee": taxi_df["airport_fee"].cast("float")
    }
    taxi_df = taxi_df.withColumns(cast_cols_map)
    log.info("""------------------------- CAST COLUMNS -------------------------""")

    vendor_name = {
        1: "Creative Mobile Technologies, LLC",
        2: "VeriFone Inc."
    }
    vendor_dim = taxi_df.select("vendor_id").distinct().sort("vendor_id")
    vendor_dim = vendor_dim.withColumnRenamed("vendor_id","vendor_code")
    # vendor_dim = vendor_dim.withColumn(
    #     "vendor_id",
    #     f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    vendor_dim = vendor_dim.withColumn("vendor_id",uuid_udf())
    vendor_map_expr = f.create_map([f.lit(x) for x in chain(*vendor_name.items())])
    vendor_dim = vendor_dim.withColumn("vendor_name", vendor_map_expr[f.col("vendor_code")])
    vendor_dim = vendor_dim.select(["vendor_id", "vendor_code", "vendor_name"])
    log.info("""------------------------- CREATED VENDOR DIM DATAFRAME -------------------------""")

    datetime_dim = taxi_df \
        .select(["pickup_datetime", "dropoff_datetime"]) \
        .distinct() \
        .sort(["pickup_datetime", "dropoff_datetime"])

    datetime_dim_cols = {
        "datetime_id": uuid_udf(),
        "pickup_datetime": datetime_dim["pickup_datetime"],
        "pickup_year": f.year(datetime_dim["pickup_datetime"]),
        "pickup_month": f.month(datetime_dim["pickup_datetime"]),
        "pickup_day": f.day(datetime_dim["pickup_datetime"]),
        "pickup_hour": f.hour(datetime_dim["pickup_datetime"]),
        "pickup_minute": f.minute(datetime_dim["pickup_datetime"]),
        "pickup_second": f.second(datetime_dim["pickup_datetime"]),
        "pickup_weekday": f.weekday(datetime_dim["pickup_datetime"]),
        "pickup_day_of_month": f.dayofmonth(datetime_dim["pickup_datetime"]),
        "pickup_day_of_year": f.dayofyear(datetime_dim["pickup_datetime"]),
        "pickup_week_of_year": f.weekofyear(datetime_dim["pickup_datetime"]),
        "dropoff_datetime": datetime_dim["dropoff_datetime"],
        "dropoff_year": f.year(datetime_dim["dropoff_datetime"]),
        "dropoff_month": f.month(datetime_dim["dropoff_datetime"]),
        "dropoff_day": f.day(datetime_dim["dropoff_datetime"]),
        "dropoff_hour": f.hour(datetime_dim["dropoff_datetime"]),
        "dropoff_minute": f.minute(datetime_dim["dropoff_datetime"]),
        "dropoff_second": f.second(datetime_dim["dropoff_datetime"]),
        "dropoff_weekday": f.weekday(datetime_dim["dropoff_datetime"]),
        "dropoff_day_of_month": f.dayofmonth(datetime_dim["dropoff_datetime"]),
        "dropoff_day_of_year": f.dayofyear(datetime_dim["dropoff_datetime"]),
        "dropoff_week_of_year": f.weekofyear(datetime_dim["dropoff_datetime"])
    }
    datetime_dim = datetime_dim.withColumns(datetime_dim_cols)
    datetime_dim = datetime_dim.select([
        "datetime_id",
        "pickup_datetime",
        "pickup_year",
        "pickup_month",
        "pickup_day",
        "pickup_hour",
        "pickup_minute",
        "pickup_second",
        "pickup_weekday",
        "pickup_day_of_month",
        "pickup_day_of_year",
        "pickup_week_of_year",
        "dropoff_datetime",
        "dropoff_year",
        "dropoff_month",
        "dropoff_day",
        "dropoff_hour",
        "dropoff_minute",
        "dropoff_second",
        "dropoff_weekday",
        "dropoff_day_of_month",
        "dropoff_day_of_year",
        "dropoff_week_of_year"
    ])
    log.info("""------------------------- CREATED DATE DIM DATAFRAME -------------------------""")

    passenger_count_dim = taxi_df.select("passenger_count").distinct().sort("passenger_count")
    passenger_count_dim = passenger_count_dim.withColumn("passenger_count_id",uuid_udf())
    passenger_count_dim = passenger_count_dim.select(["passenger_count_id", "passenger_count"])

    trip_distance_dim = taxi_df.select("trip_distance").distinct().sort("trip_distance")
    trip_distance_dim = trip_distance_dim.withColumn("trip_distance_id",uuid_udf())
    trip_distance_dim = trip_distance_dim.select(["trip_distance_id", "trip_distance"])
    log.info("""------------------------- CREATED PASSENGER COUNT DIM DATAFRAME -------------------------""")

    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }
    rate_code_dim = taxi_df.select("rate_code").distinct().sort("rate_code")
    rate_code_dim = rate_code_dim.withColumn("rate_code_id",uuid_udf())
    rate_code_map_expr = f.create_map([f.lit(x) for x in chain(*rate_code_type.items())])
    rate_code_dim = rate_code_dim.withColumn("rate_code_name", rate_code_map_expr[f.col("rate_code")])
    rate_code_dim = rate_code_dim.select(["rate_code_id", "rate_code", "rate_code_name"])
    log.info("""------------------------- CREATED RATE CODE DIM DATAFRAME -------------------------""")

    store_and_fwd_name = {
        "N": "not a store and forward trip",
        "Y": "store and forward trip"
    }
    store_fwd_dim = taxi_df.select("store_and_fwd_flag").distinct().sort("store_and_fwd_flag")
    store_fwd_dim = store_fwd_dim.withColumn("store_and_fwd_id",uuid_udf())
    store_fwd_map_expr = f.create_map([f.lit(x) for x in chain(*store_and_fwd_name.items())])
    store_fwd_dim = store_fwd_dim.withColumn("store_and_fwd_name", store_fwd_map_expr[f.col("store_and_fwd_flag")])
    store_fwd_dim = store_fwd_dim.select(["store_and_fwd_id", "store_and_fwd_flag", "store_and_fwd_name"])
    log.info("""------------------------- CREATED STORE & FWD DIM DATAFRAME -------------------------""")

    rename_location_cols_map = {
        "pu_location_id": "pu_location_code",
        "do_location_id": "do_location_code",
        "Borough": "borough",
        "Zone": "zone"
    }
    pu_location_dim = taxi_df.select("pu_location_id")
    pu_location_dim = pu_location_dim \
        .join(zone_df, pu_location_dim["pu_location_id"] == zone_df["LocationId"], how="left") \
        .drop(zone_df["LocationId"]) \
        .distinct() \
        .sort("pu_location_id")
    pu_location_dim = pu_location_dim.withColumnsRenamed(rename_location_cols_map)
    pu_location_dim = pu_location_dim.withColumn("pu_location_id",uuid_udf())
    pu_location_dim = pu_location_dim.select(["pu_location_id","pu_location_code","borough","zone","service_zone"])
    log.info("""------------------------- CREATED PICKUP LOCATION DIM DATAFRAME -------------------------""")

    do_location_dim = taxi_df.select("do_location_id")
    do_location_dim = do_location_dim \
        .join(zone_df, do_location_dim["do_location_id"] == zone_df["LocationId"], how="left") \
        .drop(zone_df["LocationId"]) \
        .distinct() \
        .sort("do_location_id")
    do_location_dim = do_location_dim.withColumnsRenamed(rename_location_cols_map)
    do_location_dim = do_location_dim.withColumn("do_location_id",uuid_udf())
    do_location_dim = do_location_dim.select(["do_location_id","do_location_code","borough","zone","service_zone"])
    log.info("""------------------------- CREATED DROP OFF LOCATION DIM DATAFRAME -------------------------""")

    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    payment_type_dim = taxi_df.select("payment_type").distinct().sort("payment_type")
    payment_type_dim = payment_type_dim.withColumnRenamed("payment_type", "payment_code")
    payment_type_map_expr = f.create_map([f.lit(x) for x in chain(*payment_type_name.items())])
    payment_type_dim = payment_type_dim.withColumn("payment_type_name", payment_type_map_expr[f.col("payment_code")])
    payment_type_dim = payment_type_dim.withColumn("payment_id",uuid_udf())
    payment_type_dim = payment_type_dim.select(["payment_id", "payment_code", "payment_type_name"])
    log.info("""------------------------- CREATED PAYMENT TYPE DIM DATAFRAME -------------------------""")

    taxi_fact = taxi_df \
        .withColumn("trip_id",uuid_udf()) \
        .join(vendor_dim, taxi_df["vendor_id"] == vendor_dim["vendor_code"]) \
        .join(passenger_count_dim, "passenger_count") \
        .join(store_fwd_dim, "store_and_fwd_flag") \
        .join(trip_distance_dim,"trip_distance") \
        .join(rate_code_dim, "rate_code") \
        .join(pu_location_dim, taxi_df["pu_location_id"] == pu_location_dim["pu_location_code"]) \
        .join(do_location_dim, taxi_df["do_location_id"] == do_location_dim["do_location_code"]) \
        .join(datetime_dim, ["pickup_datetime", "dropoff_datetime"]) \
        .join(payment_type_dim, taxi_df["payment_type"] == payment_type_dim["payment_code"]) \
        .sort("datetime_id") \
        .select(["trip_id",vendor_dim["vendor_id"],"datetime_id","passenger_count_id","trip_distance_id","rate_code_id",
                 "store_and_fwd_id",pu_location_dim["pu_location_id"],do_location_dim["do_location_id"],"payment_id",
                 "fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount"])
    log.info("""------------------------- CREATED TAXI FACT DATAFRAME -------------------------""")

    return {
        "vendor_dim":vendor_dim,
        "datetime_dim": datetime_dim,
        "store_fwd_dim": store_fwd_dim,
        "passenger_count_dim": passenger_count_dim,
        "trip_distance_dim": trip_distance_dim,
        "rate_code_dim": rate_code_dim,
        "pu_location_dim": pu_location_dim,
        "do_location_dim": do_location_dim,
        "payment_type_dim": payment_type_dim,
        "taxi_fact": taxi_fact
    }


if __name__ == "__main__":
    start = datetime.datetime.today()
    log.info("""------------------------- START SCRIPT -------------------------""")

    spark = (SparkSession.builder
             .master("local")
             .config("spark.driver.memory", "15g")
             .appName("Transform yellow taxi data")
             .getOrCreate())
    log.info("""------------------------- SPARK SESSION CREATED -------------------------""")

    ext = "*.csv"

    taxi_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data/raw"))
    zone_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data/zone"))
    log.info("""------------------------- TAXI & ZONE FILES PATHS -------------------------""")

    taxi_files = [file for path,dir_,files in os.walk(taxi_path) for file in glob.glob(os.path.join(path,ext))]
    # noinspection PyRedeclaration
    zone_files = [file for path,dir_,files in os.walk(zone_path) for file in glob.glob(os.path.join(path,ext))]
    log.info("""------------------------- TAXI & ZONE FILE LISTS -------------------------""")

    taxi_df_ = reduce(DataFrame.union,[spark.read.csv(path=file,header=True) for file in taxi_files])
    log.info("""------------------------- UNIONED TAXI DATAFRAMES -------------------------""")
    zone_df_ = reduce(DataFrame.union,[spark.read.csv(path=file,header=True) for file in zone_files])
    log.info("""------------------------- UNIONED ZONE DATAFRAMES -------------------------""")
    df = transform_data(taxi_df_, zone_df_)
    log.info("""------------------------- DATA MODEL DATAFRAME CREATED -------------------------""")

    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data/processed/"))
    log.info("""------------------------- OUTPUT FILES PATHS -------------------------""")

    df["vendor_dim"].write.csv(os.path.join(path, "vendor_dim"), mode="overwrite", header=True)
    log.info("""------------------------- VENDOR DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["datetime_dim"].write.csv(os.path.join(path, "datetime_dim"), mode="overwrite", header=True)
    log.info("""------------------------- DATE DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["store_fwd_dim"].write.csv(os.path.join(path, "store_fwd_dim"), mode="overwrite", header=True)
    log.info("""------------------------- STORE & FWD DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["passenger_count_dim"].write.csv(os.path.join(path, "passenger_count_dim"), mode="overwrite", header=True)
    log.info("""------------------------- PASSENGER COUNT DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["trip_distance_dim"].write.csv(os.path.join(path, "trip_distance_dim"), mode="overwrite", header=True)
    log.info("""------------------------- TRIP DISTANCE DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["rate_code_dim"].write.csv(os.path.join(path, "rate_code_dim"), mode="overwrite", header=True)
    log.info("""------------------------- RATE CODE DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["pu_location_dim"].write.csv(os.path.join(path,"pu_location_dim"),mode="overwrite",header=True)
    log.info("""------------------------- PICKUP LOCATION DIM WRITTEN TO PATH -------------------------""")

    df["do_location_dim"].write.csv(os.path.join(path, "do_location_dim"), mode="overwrite", header=True)
    log.info("""------------------------- DROP OFF LOCATION DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["payment_type_dim"].write.csv(os.path.join(path, "payment_type_dim"), mode="overwrite", header=True)
    log.info("""------------------------- PAYMENT TYPE DIM DATAFRAME WRITTEN TO PATH -------------------------""")

    df["taxi_fact"].write.csv(os.path.join(path, "taxi_fact"), mode="overwrite", header=True)
    log.info("""------------------------- TAXI FACT DATAFRAME WRITTEN TO PATH -------------------------""")
    end = datetime.datetime.today()
    log.info("""------------------------- ELAPSED TIME: %s SECONDS -------------------------""" % (end-start).seconds)
    log.info("""------------------------- END SCRIPT -------------------------""")
