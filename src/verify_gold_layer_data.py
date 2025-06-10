# Databricks notebook source
# Verify the schema of the Gold Delta table
spark.read.format("delta").table("rides_gold").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select the most recent 100 aggregated records from the Gold table.
# MAGIC -- This helps in quickly checking the latest state of the aggregations.
# MAGIC SELECT * FROM rides_gold
# MAGIC ORDER BY window_end DESC, driver_id
# MAGIC LIMIT 100; -- Limit to prevent displaying too much data, especially for large datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example of querying a specific window and driver.
# MAGIC -- You would adjust the 'window_start', 'window_end', and 'driver_id' based on your actual data.
# MAGIC -- Note: The timestamps here are in UTC.
# MAGIC SELECT * FROM rides_gold
# MAGIC WHERE window_start = '2025-06-09T22:00:00.000+00:00' AND window_end = '2025-06-09T23:00:00.000+00:00'
# MAGIC                        AND driver_id = 'DRV001';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This is a duplicate query from the previous cell, often done during interactive exploration.
# MAGIC -- You can remove one of them for a cleaner final notebook for GitHub, or keep both if they serve slightly different testing purposes.
# MAGIC SELECT * FROM rides_gold WHERE window_start = '2025-06-09T22:00:00.000+00:00' AND
# MAGIC                                 window_end = '2025-06-09T23:00:00.000+00:00' AND driver_id = 'DRV001';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregate Gold table data by window to see overall trends.
# MAGIC -- Shows the count of distinct drivers, total rides, and total suspicious rides within each hourly window.
# MAGIC SELECT window_start, COUNT(DISTINCT driver_id) AS distinct_drivers,
# MAGIC        SUM(total_rides_hourly) AS total_rides_in_window,
# MAGIC        SUM(total_suspicious_rides_hourly) AS total_suspicious_rides_in_window
# MAGIC FROM rides_gold
# MAGIC GROUP BY window_start ORDER BY window_start DESC;

# COMMAND ----------

# Get the total count of records in the Gold Delta table.
spark.read.format("delta").table("rides_gold").count()

# COMMAND ----------
