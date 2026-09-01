import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, current_timestamp, when
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

spark = SparkSession.builder \
    .appName("OmniRoute-GPS-Stream") \
    .getOrCreate()

PROJECT_ID = spark.conf.get("spark.driver.project", None) or "project-f8c7a8ef-e885-466f-954"
KAFKA_BOOTSTRAP = "10.142.0.2:9092"
BQ_TABLE = f"{PROJECT_ID}.omniroute_dwh.fact_safety_violations"
BQ_TEMP_BUCKET = f"omniroute-scripts-{PROJECT_ID}"

# Load restricted zones (small static reference data) and broadcast it
zones_path = f"gs://{BQ_TEMP_BUCKET}/reference/restricted_zones.json"
zones_raw = spark.sparkContext.textFile(zones_path).collect()
zones = json.loads("".join(zones_raw))
broadcast_zones = spark.sparkContext.broadcast(zones)

def check_zone(lat, lon):
    if lat is None or lon is None:
        return (False, None)
    for z in broadcast_zones.value:
        if z["min_lat"] <= lat <= z["max_lat"] and z["min_long"] <= lon <= z["max_long"]:
            return (True, z["zone_name"])
    return (False, None)

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType as SType, StructField, BooleanType, StringType

zone_check_schema = SType([
    StructField("is_restricted_zone", BooleanType(), True),
    StructField("zone_name", StringType(), True)
])
zone_check_udf = udf(check_zone, zone_check_schema)

gps_schema = StructType() \
    .add("event_type", StringType()) \
    .add("vehicle_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("speed_kmph", DoubleType()) \
    .add("timestamp", StringType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "fleet.gps.events") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), gps_schema).alias("data")
).select("data.*") \
 .withColumn("event_ts", to_timestamp(col("timestamp"))) \
 .withColumn("is_overspeeding", col("speed_kmph") > 110) \
 .withColumn("zone_check", zone_check_udf(col("latitude"), col("longitude"))) \
 .withColumn("is_restricted_zone", col("zone_check.is_restricted_zone")) \
 .withColumn("zone_name", col("zone_check.zone_name")) \
 .drop("zone_check") \
 .withColumn("is_violation", col("is_overspeeding") | col("is_restricted_zone")) \
 .withColumn("ingestion_time", current_timestamp())

# Only write actual violations to the fact table (keeps the table meaningful,
# matches BRD's "flagged event" framing rather than logging every GPS ping)
violations = parsed.filter(col("is_violation") == True) \
    .select(
        col("vehicle_id").alias("vin"),
        "driver_id", "speed_kmph", "latitude", "longitude", "event_ts",
        "is_overspeeding", "is_restricted_zone", "zone_name", "is_violation",
        "ingestion_time"
    ).withColumnRenamed("event_ts", "event_timestamp")

def write_to_bq(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.write \
        .format("bigquery") \
        .option("table", BQ_TABLE) \
        .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
        .mode("append") \
        .save()
    print(f"Batch {batch_id}: wrote {batch_df.count()} violation rows to BigQuery")

query = violations.writeStream \
    .foreachBatch(write_to_bq) \
    .outputMode("update") \
    .start()

query.awaitTermination()
