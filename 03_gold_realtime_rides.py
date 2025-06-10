# Databricks notebook source
# MAGIC %sql
# MAGIC -- SQL command to drop the Gold table if it exists.
# MAGIC -- Useful for a fresh start during development.
# MAGIC DROP TABLE IF EXISTS rides_gold;

# COMMAND ----------
from pyspark.sql.functions import window, count, avg, sum, col, current_timestamp, when, round
from delta.tables import DeltaTable # Ensure 'delta' library is attached to your cluster


# Define paths for your Delta tables and checkpoints.
# Ensure consistency with Bronze and Silver notebooks.
SILVER_OUTPUT_PATH = "dbfs:/delta/rides_silver" # Should match the path from your Silver notebook
GOLD_CHECKPOINT_PATH = "dbfs:/checkpoints/rides_gold_aggregations" # Changed for consistency
GOLD_OUTPUT_PATH = "dbfs:/delta/rides_gold_aggregations"

# --- Optional: Clean up previous run's checkpoint and data for a fresh start ---
# Only run this cell if you want to clear previous state or are encountering errors
# about existing checkpoints/tables.
# Be careful, this deletes data!
print(f"Attempting to remove previous checkpoint: {GOLD_CHECKPOINT_PATH}")
dbutils.fs.rm(GOLD_CHECKPOINT_PATH, True)
print(f"Attempting to remove previous Delta table data: {GOLD_OUTPUT_PATH}")
dbutils.fs.rm(GOLD_OUTPUT_PATH, True) # This deletes the data files in the Delta table path
print("Previous Gold paths cleared (if they existed).")

# COMMAND ----------

# 1. Read from Silver Delta table as a stream
# This stream provides the cleaned and enriched data for aggregation.
print(f"Starting to read from Silver Delta table at: {SILVER_OUTPUT_PATH}")
df_gold_input = (
    spark.readStream
    .format("delta")
    .load(SILVER_OUTPUT_PATH)
)

# COMMAND ----------

# 2. Window Aggregation: Hourly metrics per driver
# This aggregation uses a tumbling window of 1 hour based on 'event_timestamp'.
# It calculates total rides, average fare, and total suspicious rides per driver per hour.
# 'outputMode("complete")' ensures that each batch contains the complete, updated result for each window.
print("Performing hourly window aggregation per driver...")
df_hourly_driver_agg = (
    df_gold_input.groupBy(
        window(col("event_timestamp"), "1 hour"), # Tumbling window of 1 hour
        col("driver_id")
    )
    .agg(
        count("ride_id").alias("total_rides_hourly"),
        round(avg("fare"),2).alias("avg_fare_hourly"),
        # Sums 1 if 'is_suspicious' is true, 0 otherwise, to count suspicious rides
        sum(when(col("is_suspicious") == True, 1).otherwise(0)).alias("total_suspicious_rides_hourly")
    )
    .withColumn("processing_time", current_timestamp()) # Add processing timestamp for when the aggregation was calculated
    .select(
        col("window.start").alias("window_start"), # Extract window start timestamp
        col("window.end").alias("window_end"),     # Extract window end timestamp
        col("driver_id"),
        col("total_rides_hourly"),
        col("avg_fare_hourly"),
        col("total_suspicious_rides_hourly"),
        col("processing_time")
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL DDL to create the Gold Delta table.
# MAGIC -- This table stores the aggregated hourly metrics for drivers.
# MAGIC -- Setting spark.databricks.delta.optimize.maxFileSize helps optimize file sizes for better query performance.
# MAGIC SET spark.databricks.delta.optimize.maxFileSize = 134217728; -- Set target file size to 128 MB
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS rides_gold ( -- Use IF NOT EXISTS to avoid errors if table already exists
# MAGIC      window_start TIMESTAMP,
# MAGIC      window_end TIMESTAMP,
# MAGIC      driver_id STRING,
# MAGIC      total_rides_hourly BIGINT,  
# MAGIC      avg_fare_hourly DOUBLE,
# MAGIC      total_suspicious_rides_hourly BIGINT,  
# MAGIC      processing_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/delta/rides_gold_aggregations'; -- Ensure this matches GOLD_OUTPUT_PATH in Python

# COMMAND ----------

# UDF to perform MERGE (upsert) into the Gold Delta table.
# This function is called for each micro-batch.
def upsertToGoldDelta(microBatchDF, batchId):
    print(f"Processing batch {batchId} for Gold layer...")
    # Ensure the Gold table exists. Create it if it doesn't.
    # This is a fallback; the SQL DDL above should generally create it first.
    if not DeltaTable.isDeltaTable(spark, GOLD_OUTPUT_PATH):
        print(f"Gold Delta Table not found at {GOLD_OUTPUT_PATH}. Creating it now...")
        microBatchDF.write.format("delta").mode("append").option("overwriteSchema", "true").save(GOLD_OUTPUT_PATH)
    else:
        # Perform a MERGE operation to upsert (update or insert) records.
        # This is efficient for complete output mode, as it updates existing windows and inserts new ones.
        deltaTable = DeltaTable.forPath(spark, GOLD_OUTPUT_PATH)
        (deltaTable.alias("target")
            .merge(
                microBatchDF.alias("updates"),
                # Match condition: A record is a match if window_start, window_end, AND driver_id are the same.
                "target.window_start = updates.window_start AND target.window_end = updates.window_end AND target.driver_id = updates.driver_id"
            )
            .whenMatchedUpdateAll()   # If a match is found, update all columns of the target record.
            .whenNotMatchedInsertAll() # If no match, insert the new record from the updates DataFrame.
            .execute())
    print(f"Batch {batchId} processed and merged into Gold Delta Table at {GOLD_OUTPUT_PATH}.")

# COMMAND ----------

# 3. Write the aggregated stream to Gold Delta Lake using foreachBatch
# 'outputMode("complete")' is critical for this type of aggregation, meaning each batch contains the full, updated state of all windows.
# 'foreachBatch' allows us to use the powerful Delta Lake MERGE operation for efficient upserts.
print(f"Starting Gold stream query, writing to: {GOLD_OUTPUT_PATH}")
query_gold = (
    df_hourly_driver_agg.writeStream
    .foreachBatch(upsertToGoldDelta) # Custom logic for each micro-batch
    .outputMode("complete")          # Required for aggregations that update prior results
    .option("checkpointLocation", GOLD_CHECKPOINT_PATH) # Stores state for fault tolerance
    .trigger(processingTime="45 seconds") # Process micro-batches every 45 seconds for demo
    .start()
)

print("Gold stream query started. Monitoring display below...")

# COMMAND ----------