from kafka import KafkaConsumer
from deltalake import DeltaTable
import json
import os
import logging
import polars as pl

# Configuration
logging.basicConfig(level = logging.INFO)
bootstrap_servers = ["localhost:9092"]
delta_path = "./delta_tables"
topics = ["client", "department", "employee", "sale"]

# Function to create and configur Kafka consumer
def create_consumer(topics, bootstrap_servers):
    return KafkaConsumer(
        *topics,
        bootstrap_servers = bootstrap_servers,
        value_deserializer = lambda v: json.loads(v.decode("utf-8"))
    )

# Function to process Kafka message
def process_message(topic, data):
    df = pl.from_records([data])
    delta_table_path = f"{delta_path}/{topic}"
    if not os.path.exists(delta_table_path):
        write_delta_table(df, delta_table_path, "insert")
    else:
        merge_into_delta_table(df, delta_table_path)

# Function to write Delta table
def write_delta_table(df, path, mode):
    try:
        df.write_delta(target = path, mode = mode)
        logging.info(f"{mode.capitalize()}ed message to Delta table: {df}")
    except Exception as e:
        logging.error(f"Failed to {mode} message to Delta table: {df}")
        logging.error(f"Error: {e}")

# Function to merge message into Delta table
def merge_into_delta_table(df, path):
    try:
        df.write_delta(
            target = path, mode = "merge",
            delta_merge_options = {
                "predicate": "s.id = t.id",
                "source_alias": "s",
                "target_alias": "t",
            }
        ).when_matched_update_all().when_not_matched_insert_all().execute()
        logging.info(f"Merged message to Delta table: {df}")
    except Exception as e:
        logging.error(f"Failed to merge message to Delta table: {df}")
        logging.error(f"Error: {e}")

def main():
    consumer = create_consumer(topics, bootstrap_servers)
    for message in consumer:
        process_message(message.topic, message.value)

if __name__ == "__main__":
    main()

