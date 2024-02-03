import os
import pyspark.sql.functions as f
from extract import extract_taxi_data, extract_zone_data
from itertools import chain
from pyspark.sql import DataFrame, SparkSession, Window


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

    rename_cols_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id"
    }
    taxi_df = taxi_df.withColumnsRenamed(rename_cols_map)

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

    vendor_name = {
        1: "Creative Mobile Technologies, LLC",
        2: "VeriFone Inc."
    }
    vendor_dim = taxi_df.select("vendor_id").distinct().sort("vendor_id")
    vendor_dim = vendor_dim.withColumnRenamed("vendor_id","vendor_code")
    vendor_dim = vendor_dim.withColumn(
        "vendor_id",
        f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    vendor_map_expr = f.create_map([f.lit(x) for x in chain(*vendor_name.items())])
    vendor_dim = vendor_dim.withColumn("vendor_name", vendor_map_expr[f.col("vendor_code")])
    vendor_dim = vendor_dim.select(["vendor_id", "vendor_code", "vendor_name"])

    date_dim = taxi_df \
        .select(["pickup_datetime", "dropoff_datetime"]) \
        .distinct() \
        .sort(["pickup_datetime", "dropoff_datetime"])

    date_dim_cols = {
        "datetime_id": f.row_number().over(Window.orderBy(f.monotonically_increasing_id())),
        "pickup_datetime": date_dim["pickup_datetime"],
        "pickup_year": f.year(date_dim["pickup_datetime"]),
        "pickup_month": f.month(date_dim["pickup_datetime"]),
        "pickup_day": f.day(date_dim["pickup_datetime"]),
        "pickup_hour": f.hour(date_dim["pickup_datetime"]),
        "pickup_minute": f.minute(date_dim["pickup_datetime"]),
        "pickup_second": f.second(date_dim["pickup_datetime"]),
        "pickup_weekday": f.weekday(date_dim["pickup_datetime"]),
        "pickup_day_of_month": f.dayofmonth(date_dim["pickup_datetime"]),
        "pickup_day_of_year": f.dayofyear(date_dim["pickup_datetime"]),
        "pickup_week_of_year": f.weekofyear(date_dim["pickup_datetime"]),
        "dropoff_datetime": date_dim["dropoff_datetime"],
        "dropoff_year": f.year(date_dim["dropoff_datetime"]),
        "dropoff_month": f.month(date_dim["dropoff_datetime"]),
        "dropoff_day": f.day(date_dim["dropoff_datetime"]),
        "dropoff_hour": f.hour(date_dim["dropoff_datetime"]),
        "dropoff_minute": f.minute(date_dim["dropoff_datetime"]),
        "dropoff_second": f.second(date_dim["dropoff_datetime"]),
        "dropoff_weekday": f.weekday(date_dim["dropoff_datetime"]),
        "dropoff_day_of_month": f.dayofmonth(date_dim["dropoff_datetime"]),
        "dropoff_day_of_year": f.dayofyear(date_dim["dropoff_datetime"]),
        "dropoff_week_of_year": f.weekofyear(date_dim["dropoff_datetime"])
    }
    date_dim = date_dim.withColumns(date_dim_cols)
    date_dim = date_dim.select([
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

    passenger_count_dim = taxi_df.select("passenger_count").distinct().sort("passenger_count")
    passenger_count_dim = passenger_count_dim.withColumn(
        "passenger_count_id",
        f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    passenger_count_dim = passenger_count_dim.select(["passenger_count_id", "passenger_count"])

    trip_distance_dim = taxi_df.select("trip_distance").distinct().sort("trip_distance")
    trip_distance_dim = trip_distance_dim.withColumn(
        "trip_distance_id",
        f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    trip_distance_dim = trip_distance_dim.select(["trip_distance_id", "trip_distance"])

    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }
    rate_code_dim = taxi_df.select("rate_code").distinct().sort("rate_code")
    rate_code_dim = rate_code_dim.withColumn(
        "rate_code_id",
        f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    rate_code_map_expr = f.create_map([f.lit(x) for x in chain(*rate_code_type.items())])
    rate_code_dim = rate_code_dim.withColumn("rate_code_name", rate_code_map_expr[f.col("rate_code")])
    rate_code_dim = rate_code_dim.select(["rate_code_id", "rate_code", "rate_code_name"])

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
    pu_location_dim = pu_location_dim.withColumn(
        "pu_location_id",
        f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    pu_location_dim = pu_location_dim.select(["pu_location_id","pu_location_code","borough","zone","service_zone"])

    do_location_dim = taxi_df.select("do_location_id")
    do_location_dim = do_location_dim \
        .join(zone_df, do_location_dim["do_location_id"] == zone_df["LocationId"], how="left") \
        .drop(zone_df["LocationId"]) \
        .distinct() \
        .sort("do_location_id")
    do_location_dim = do_location_dim.withColumnsRenamed(rename_location_cols_map)
    do_location_dim = do_location_dim.withColumn(
        "do_location_id",
        f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    do_location_dim = do_location_dim.select(["do_location_id","do_location_code","borough","zone","service_zone"])

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
    payment_type_dim = payment_type_dim.withColumn(
        "payment_id",
        f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    payment_type_dim = payment_type_dim.select(["payment_id", "payment_code", "payment_type_name"])

    fact_table = taxi_df \
        .join(vendor_dim, taxi_df["vendor_id"] == vendor_dim["vendor_code"]) \
        .join(passenger_count_dim, "passenger_count") \
        .join(trip_distance_dim,"trip_distance") \
        .join(rate_code_dim, "rate_code") \
        .join(pu_location_dim, taxi_df["pu_location_id"] == pu_location_dim["pu_location_code"]) \
        .join(do_location_dim, taxi_df["do_location_id"] == do_location_dim["do_location_code"]) \
        .join(date_dim, ["pickup_datetime", "dropoff_datetime"]) \
        .join(payment_type_dim, taxi_df["payment_type"] == payment_type_dim["payment_code"]) \
        .select([vendor_dim["vendor_id"],"datetime_id","passenger_count_id","trip_distance_id","rate_code_id",
                 "store_and_fwd_flag",pu_location_dim["pu_location_id"],do_location_dim["do_location_id"],"payment_id",
                 "fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount"])

    return {
        "vendor_dim":vendor_dim,
        "date_dim": date_dim,
        "passenger_count_dim": passenger_count_dim,
        "trip_distance_dim": trip_distance_dim,
        "rate_code_dim": rate_code_dim,
        "pu_location_dim": pu_location_dim,
        "do_location_dim": do_location_dim,
        "payment_type_dim": payment_type_dim,
        "fact_table": fact_table
    }


if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local")
             .config("spark.driver.memory", "15g")
             .appName("Extract yellow taxi data")
             .getOrCreate())

    taxi_data = extract_taxi_data(spark_session=spark, year=2022, month=1)
    zone_data = extract_zone_data(spark_session=spark)
    df = transform_data(taxi_data, zone_data)

    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data/processed/"))

    df["vendor_dim"].write.csv(os.path.join(path, "vendor_dim"), mode="overwrite", header=True)
    df["date_dim"].write.csv(os.path.join(path, "date_dim"), mode="overwrite", header=True)
    df["passenger_count_dim"].write.csv(os.path.join(path, "passenger_count_dim"), mode="overwrite", header=True)
    df["trip_distance_dim"].write.csv(os.path.join(path, "trip_distance_dim"), mode="overwrite", header=True)
    df["rate_code_dim"].write.csv(os.path.join(path, "rate_code_dim"), mode="overwrite", header=True)
    df["pu_location_dim"].write.csv(os.path.join(path,"pu_location_dim"),mode="overwrite",header=True)
    df["do_location_dim"].write.csv(os.path.join(path, "do_location_dim"), mode="overwrite", header=True)
    df["payment_type_dim"].write.csv(os.path.join(path, "payment_type_dim"), mode="overwrite", header=True)
    df["fact_table"].write.csv(os.path.join(path, "fact_table"), mode="overwrite", header=True)
