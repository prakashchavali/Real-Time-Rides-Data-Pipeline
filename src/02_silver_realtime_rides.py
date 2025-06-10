# Databricks notebook source
from pyspark.sql.functions import from_json, col, lit, current_timestamp, round, abs, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType, IntegerType, BooleanType
import math

# COMMAND ----------

# Define paths for your Delta tables and checkpoints.
# Use standard DBFS paths or mounted ADLS Gen2 paths.
# Ensure consistency with Bronze and Gold notebooks.
BRONZE_OUTPUT_PATH = "dbfs:/delta/rides_bronze" # Should match the path from your Bronze notebook
SILVER_CHECKPOINT_PATH = "dbfs:/checkpoints/rides_silver"
SILVER_OUTPUT_PATH = "dbfs:/delta/rides_silver"

# Clean up previous run's checkpoint and data for a fresh start.
# This is useful for development but should be used with caution in production.
print(f"Attempting to remove previous checkpoint: {SILVER_CHECKPOINT_PATH}")
dbutils.fs.rm(SILVER_CHECKPOINT_PATH, True)
print(f"Attempting to remove previous Delta table: {SILVER_OUTPUT_PATH}")
dbutils.fs.rm(SILVER_OUTPUT_PATH, True)
print("Previous Silver paths cleared (if they existed).")


# 1. Define the schema of your JSON ride data
ride_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("passenger_id", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("fare", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("event_timestamp", TimestampType(), True) # Added for watermarking
])

# COMMAND ----------

# 2. Read from Bronze Delta table as a stream
# The 'raw_json_data' column was written by the Bronze layer.
print(f"Starting to read from Bronze Delta table at: {BRONZE_OUTPUT_PATH}")
df_silver = (
    spark.readStream
    .format("delta")
    .load(BRONZE_OUTPUT_PATH)
    # Deserialize the 'value' column (our JSON message)
    .withColumn("value_str", col("raw_json_data").cast("string"))
    .withColumn("data", from_json(col("value_str"), ride_schema))
    .filter(col("data").isNotNull()) # Filter out malformed JSON records
    .select("data.*", "timestamp") # Select all fields from parsed data and Kafka timestamp (if needed)
)


# COMMAND ----------

# 3. Data Cleaning and Derived Fields
print("Applying data cleaning and deriving new fields...")
df_silver_cleaned = df_silver.select(
    col("ride_id"),
    col("driver_id"),
    col("passenger_id"),
    col("start_time"),
    col("end_time"),
    col("fare"),
    col("status"),
    col("event_timestamp"), # Keep this for watermarking and event-time processing
    # Derive trip_duration_seconds from start_time and end_time
    (col("end_time").cast(LongType()) - col("start_time").cast(LongType())).alias("trip_duration_seconds"),
    # For this demo, using a random dummy distance as no real location data is available.
    # In a real scenario, this would be calculated from actual coordinates.
    lit(round(abs(expr("rand()")) * 20, 2)).alias("trip_distance_km")
).filter(
    (col("ride_id").isNotNull()) &         # Ensure essential IDs are not null
    (col("driver_id").isNotNull()) &
    (col("event_timestamp").isNotNull()) & # Ensure valid timestamp for watermark/event-time processing
    (col("fare").isNotNull()) &            # Ensure fare is present
    (col("fare") >= 0) &                   # Fare should be non-negative
    (col("trip_duration_seconds").isNotNull()) & # Ensure duration is calculable
    (col("trip_duration_seconds") >= 0)    # Trip duration should be non-negative
)


# COMMAND ----------

# 4. Deduplication with Watermark
# Watermarking is crucial for handling late-arriving data and managing state size in streaming aggregations.
# We use a 10-minute watermark on 'event_timestamp' to allow for late-arriving events within this window.
# Deduplicate based on 'ride_id' and 'event_timestamp' to handle potential duplicate records.
print("Applying watermarking and deduplication...")
df_silver_dedup = (
    df_silver_cleaned.withWatermark("event_timestamp", "10 minutes")
    .dropDuplicates(["ride_id", "event_timestamp"]) # Deduplicate based on ride_id and its event time
)

# COMMAND ----------

# 5. Add is_suspicious column (Enrichment)
# This logic flags rides that might be suspicious based on duration and fare.
print("Adding 'is_suspicious' flag...")
df_silver_enriched = df_silver_dedup.withColumn(
    "is_suspicious",
    # Criteria for suspicion: very short duration (less than 2 minutes) AND a high fare (over 300)
    (col("trip_duration_seconds") < 120) & (col("fare") > 300)
)


# COMMAND ----------

# 6. Write the enriched stream to Silver Delta table
# 'mergeSchema' option allows for schema evolution (e.g., if new columns are added later).
# 'append' mode adds new records to the Silver table.
print(f"Writing enriched data to Silver Delta table at: {SILVER_OUTPUT_PATH}")
query_silver = (
    df_silver_enriched.writeStream
    .option("mergeSchema", "true") # Allows for schema changes (e.g. adding new columns later)
    .format("delta")
    .outputMode("append") # Use append mode for Silver table as it's a growing dataset
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .start(SILVER_OUTPUT_PATH)
)

print("Silver stream query started. Monitoring display below...")

# COMMAND ----------
