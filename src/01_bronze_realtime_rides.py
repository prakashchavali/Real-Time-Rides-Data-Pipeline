# Databricks notebook source
# COMMAND ----------
# 1. Define Kafka (Confluent Cloud) Connection Parameters
# IMPORTANT: FOR GITHUB UPLOAD, SENSITIVE CREDENTIALS ARE REPLACED WITH PLACEHOLDERS.
# IN A REAL DEPLOYMENT, THESE SHOULD BE MANAGED SECURELY USING DATABRICKS SECRETS.

# PLACEHOLDERS - REPLACE THESE IN YOUR LIVE ENVIRONMENT WITH ACTUAL VALUES
# For example, in Databricks, use dbutils.secrets.get()
#
# To set up Databricks Secrets:
# 1. Create a secret scope: %sh databricks secrets create-scope --scope confluent-kafka-scope
# 2. Put your secrets:
#    %sh databricks secrets put --scope confluent-kafka-scope --key kafka-bootstrap-servers
#    %sh databricks secrets put --scope confluent-kafka-scope --key kafka-api-key
#    %sh databricks secrets put --scope confluent-kafka-scope --key kafka-api-secret

# Placeholder variables for clarity in the code.
# In a real environment, you would retrieve these from a secrets manager.
BOOTSTRAP_SERVERS_PLACEHOLDER = "<YOUR_CONFLUENT_CLOUD_BOOTSTRAP_SERVERS>"
API_KEY_PLACEHOLDER = "<YOUR_CONFLUENT_CLOUD_API_KEY>"
API_SECRET_PLACEHOLDER = "<YOUR_CONFLUENT_CLOUD_API_SECRET>"

KAFKA_TOPIC_NAME = "rides_stream" # This must EXACTLY match the topic name in Confluent Cloud

# Kafka Options - configured to use placeholders or Databricks Secrets
KAFKA_OPTIONS = {
    # If using Databricks Secrets:
    # "kafka.bootstrap.servers": dbutils.secrets.get(scope="confluent-kafka-scope", key="kafka-bootstrap-servers"),
    # If using placeholders for GitHub (replace with your actual values when running):
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS_PLACEHOLDER,

    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",

    # If using Databricks Secrets:
    # "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{dbutils.secrets.get(scope='confluent-kafka-scope', key='kafka-api-key')}\" password=\"{dbutils.secrets.get(scope='confluent-kafka-scope', key='kafka-api-secret')}\";",
    # If using placeholders for GitHub (replace with your actual values when running):
    "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{API_KEY_PLACEHOLDER}\" password=\"{API_SECRET_PLACEHOLDER}\";",

    "subscribe": KAFKA_TOPIC_NAME,
    "startingOffsets": "earliest" # Read all available messages from the topic from the beginning
}

# Define paths for your Delta tables and checkpoints.
# Use standard DBFS paths or mounted ADLS Gen2 paths.
# No need to hardcode 'hardcoded' in the path names for GitHub.
BRONZE_CHECKPOINT_PATH = "dbfs:/checkpoints/rides_bronze"
BRONZE_OUTPUT_PATH = "dbfs:/delta/rides_bronze"

# COMMAND ----------
# OPTIONAL: Clean up previous run's checkpoint and data for a fresh start
# Only run this cell if you want to clear previous state or are encountering errors
# about existing checkpoints/tables.
# Be careful, this deletes data!

print(f"Attempting to remove previous checkpoint: {BRONZE_CHECKPOINT_PATH}")
dbutils.fs.rm(BRONZE_CHECKPOINT_PATH, True)
print(f"Attempting to remove previous Delta table: {BRONZE_OUTPUT_PATH}")
dbutils.fs.rm(BRONZE_OUTPUT_PATH, True)
print("Previous paths cleared (if they existed).")

# COMMAND ----------

# 2. Read from Kafka stream
print("Starting to read from Kafka stream...")
df_bronze = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)

# Convert Kafka binary values to string for initial inspection
# Kafka messages come with 'key', 'value', 'topic', 'partition', 'offset', 'timestamp', 'timestampType'
# We are primarily interested in the 'value' which contains our JSON ride events.
df_bronze_raw = df_bronze.selectExpr("CAST(value AS STRING) as raw_json_data", "timestamp")

# COMMAND ----------

# 3. Write the raw stream to Delta Lake (append mode)
print(f"Writing raw Kafka messages to Delta table at: {BRONZE_OUTPUT_PATH}")
query_bronze = (
    df_bronze_raw.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
    .option("path", BRONZE_OUTPUT_PATH)
    .trigger(processingTime="10 seconds") # Process micro-batches every 10 seconds
    .start()
)

print("Bronze stream query started. Monitoring display below...")

# COMMAND ----------

display(query_bronze)
