from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, lead, lit, when
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("OmniRoute-SCD2-AssetHistory").getOrCreate()

PROJECT_ID = "project-f8c7a8ef-e885-466f-954"
BQ_TABLE = f"{PROJECT_ID}.omniroute_dwh.asset_history_scd2"
BQ_TEMP_BUCKET = f"omniroute-scripts-{PROJECT_ID}"
RAW_PATH = f"gs://{BQ_TEMP_BUCKET}/raw-data/vehicle_assignment.csv"

raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_PATH)
raw = raw.withColumn("assignment_date", col("assignment_date").cast(DateType()))

window_conflict = Window.partitionBy("vin", "assignment_date").orderBy(col("daily_rate").desc())
deduped = raw.withColumn("rn", row_number().over(window_conflict)) \
    .filter(col("rn") == 1).drop("rn")

window_scd = Window.partitionBy("vin").orderBy("assignment_date")
scd2 = deduped.withColumn("start_date", col("assignment_date")) \
    .withColumn("end_date", lead("assignment_date").over(window_scd))

scd2_final = scd2.withColumn(
    "status",
    when(col("end_date").isNull(), lit("IN-TRANSIT")).otherwise(lit("ARCHIVED"))
).select("vin", "driver_id", "daily_rate", "start_date", "end_date", "status")

scd2_final.show(50, truncate=False)

scd2_final.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
    .mode("overwrite") \
    .save()

print(f"SCD2 Asset History written to {BQ_TABLE}")
spark.stop()
