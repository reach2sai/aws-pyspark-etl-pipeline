"""PySpark shipment ETL with simple validation and delivery metrics."""
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def transform(df):
    dates = (df.withColumn("event_ts", F.to_timestamp("event_timestamp"))
               .withColumn("shipped_on", F.to_date("ship_date"))
               .withColumn("delivered_on", F.to_date("delivery_date"))
               .withColumn("promised_on", F.to_date("promised_date")))
    valid = dates.filter(
        F.col("shipment_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("shipped_on").isNotNull()
        & (F.col("delivered_on").isNull() | (F.col("delivered_on") >= F.col("shipped_on")))
    )
    window = Window.partitionBy("shipment_id").orderBy(F.col("event_ts").desc())
    latest = valid.withColumn("row_num", F.row_number().over(window)).filter("row_num = 1").drop("row_num")
    return (latest.withColumn("transit_days", F.datediff("delivered_on", "shipped_on"))
                  .withColumn("on_time", F.when(
                      (F.col("status") == "DELIVERED") & F.col("delivered_on").isNotNull()
                      & F.col("promised_on").isNotNull(),
                      F.col("delivered_on") <= F.col("promised_on")
                  ).otherwise(F.lit(None).cast("boolean"))))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/shipments.csv")
    parser.add_argument("--output", default="output/shipments")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("ShipmentETL").getOrCreate()
    try:
        source = spark.read.option("header", True).csv(args.input)
        curated = transform(source)
        curated.write.mode("overwrite").parquet(args.output)
        curated.select("shipment_id", "status", "transit_days", "on_time").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
