import time
import json
import random
import os
from datetime import datetime, timedelta

from confluent_kafka import Producer, KafkaException

# --- Confluent Cloud Kafka Configuration ---
# IMPORTANT: FOR GITHUB UPLOAD, SENSITIVE CREDENTIALS ARE REPLACED WITH PLACEHOLDERS.
# IN A REAL DEPLOYMENT, THESE SHOULD BE MANAGED SECURELY (e.g., environment variables, a vault).

# PLACEHOLDERS - REPLACE THESE IN YOUR LIVE ENVIRONMENT WITH ACTUAL VALUES
# For example, when running locally, you can set these as environment variables:
# export CONFLUENT_CLOUD_BOOTSTRAP_SERVERS='<your_servers>'
# export CONFLUENT_CLOUD_API_KEY='<your_key>'
# export CONFLUENT_CLOUD_API_SECRET='<your_secret>'

# You can uncomment the os.getenv lines below and set actual environment variables
# when you want to run this script.
# For GitHub, the placeholders remain.

CONFLUENT_CLOUD_BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_CLOUD_BOOTSTRAP_SERVERS", "<YOUR_CONFLUENT_CLOUD_BOOTSTRAP_SERVERS>")
CONFLUENT_CLOUD_API_KEY = os.getenv("CONFLUENT_CLOUD_API_KEY", "<YOUR_CONFLUENT_CLOUD_API_KEY>")
CONFLUENT_CLOUD_API_SECRET = os.getenv("CONFLUENT_CLOUD_API_SECRET", "<YOUR_CONFLUENT_CLOUD_API_SECRET>")

# Basic check to ensure all necessary variables are set (either from env or placeholders are replaced)
if "<YOUR_" in CONFLUENT_CLOUD_BOOTSTRAP_SERVERS or \
   "<YOUR_" in CONFLUENT_CLOUD_API_KEY or \
   "<YOUR_" in CONFLUENT_CLOUD_API_SECRET:
    print("WARNING: Confluent Cloud credentials are still placeholders. Set environment variables or replace directly for live execution.")


TOPIC_NAME = "rides_stream" # This must EXACTLY match the topic name in Confluent Cloud

producer_config = {
    'bootstrap.servers': CONFLUENT_CLOUD_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': CONFLUENT_CLOUD_API_KEY,
    'sasl.password': CONFLUENT_CLOUD_API_SECRET,
    'client.id': 'rides-producer-py'
}

# --- Delivery Report Callback ---
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    # else:
    #     print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# --- Data Generation Logic ---
def generate_ride_event(ride_id_counter):
    ride_id = f"RID{ride_id_counter:05d}"
    driver_id = f"DRV{random.randint(1, 100):03d}"
    passenger_id = f"P{random.randint(1000, 9999):04d}"

    start_time_dt = datetime.now() - timedelta(minutes=random.randint(0, 5), seconds=random.randint(0, 59))
    end_time_dt = start_time_dt + timedelta(minutes=random.randint(1, 15), seconds=random.randint(0, 59))
    
    fare = round(random.uniform(50.0, 1000.0), 2)
    status = random.choice(["completed", "cancelled", "started"])
    
    is_suspicious_candidate = random.random() < 0.1
    if is_suspicious_candidate:
        # Make it suspicious: short duration, high fare
        end_time_dt = start_time_dt + timedelta(seconds=random.randint(30, 119)) # Less than 2 minutes
        fare = round(random.uniform(301.0, 1500.0), 2) # High fare

    # event_timestamp is when the event was 'recorded' or 'sent', could be slightly different from end_time
    event_timestamp_dt = end_time_dt + timedelta(seconds=random.randint(-120, 10))
    # Occasionally simulate a very late event (e.g., due to network delay or batching)
    if random.random() < 0.05:
        event_timestamp_dt = end_time_dt - timedelta(minutes=random.randint(6, 10))

    # Occasionally simulate an update to an existing ride (for MERGE testing)
    if random.random() < 0.02 and ride_id_counter > 1:
        ride_id = f"RID{random.randint(max(1, ride_id_counter - 10), ride_id_counter - 1):05d}"

    event = {
        "ride_id": ride_id,
        "driver_id": driver_id,
        "passenger_id": passenger_id,
        "start_time": start_time_dt.isoformat(timespec='seconds') + 'Z',
        "end_time": end_time_dt.isoformat(timespec='seconds') + 'Z',
        "fare": fare,
        "status": status,
        "event_timestamp": event_timestamp_dt.isoformat(timespec='seconds') + 'Z'
    }
    return event

# --- Main Producer Loop ---
if __name__ == '__main__':
    producer = Producer(producer_config)
    ride_id_counter = 0

    try:
        while True:
            ride_id_counter += 1
            event = generate_ride_event(ride_id_counter)
            
            key = event["ride_id"].encode('utf-8') # Use ride_id as key for ordering on partition
            value = json.dumps(event).encode('utf-8')

            producer.produce(TOPIC_NAME, key=key, value=value, callback=delivery_report)
            producer.poll(0) # Non-blocking poll for delivery reports

            time.sleep(random.uniform(0.1, 0.5)) # Adjust sleep time to control message rate

    except KeyboardInterrupt:
        print("Producer interrupted by user.")
    except KafkaException as e:
        print(f"Kafka error: {e}")
    finally:
        print("Flushing producer messages...")
        producer.flush(30) # Ensure all outstanding messages are delivered before exiting
        print("Producer shut down.")