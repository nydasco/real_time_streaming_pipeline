#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
import os
import logging
import polars as pl
import tomli

# Configuration
with open("parameters.toml", mode = "rb") as params:
    config = tomli.load(params)

logging.basicConfig(level = config["logging"]["level"])
bootstrap_servers = config["kafka"]["bootstrap_servers"]
topics = config["kafka"]["topics"]
delta_path = config["kafka"]["delta_path"]

def create_consumer(topics, bootstrap_servers):
    """
    Creates a Kafka consumer with the specified topics and bootstrap servers.

    Parameters:
    - topics (list): A list of topics to subscribe to.
    - bootstrap_servers (str): A string of bootstrap servers in the format "host:port".

    Returns:
    - KafkaConsumer: The created Kafka consumer.

    """
    return KafkaConsumer(
        *topics,
        bootstrap_servers = bootstrap_servers,
        value_deserializer = lambda v: json.loads(v.decode("utf-8"))
    )

def process_message(topic, data):
    """
    Process a message received from a topic.

    Parameters:
    - topic (str): The topic from which the message was received.
    - data (dict): The data of the message.

    Returns:
    None
    """
    df = pl.from_records([data])
    delta_table_path = f"{delta_path}/{topic}"
    if not os.path.exists(delta_table_path):
        write_delta_table(df, delta_table_path, "insert")
    else:
        merge_into_delta_table(df, delta_table_path)

def write_delta_table(df, path, mode):
    """
    Writes the given DataFrame to a Delta table at the specified path using the specified mode.

    Args:
        df (polars.DataFrame): The DataFrame to be written to the Delta table.
        path (str): The path where the Delta table will be created or updated.
        mode (str): The write mode to be used. Valid values are 'append', 'overwrite', 'ignore', and 'error'.

    Returns:
        None

    Raises:
        Exception: If an error occurs while writing the DataFrame to the Delta table.

    """
    try:
        df.write_delta(target=path, mode=mode)
        logging.info(f"{mode.capitalize()}ed message to Delta table: {df}")
    except Exception as e:
        logging.error(f"Failed to {mode} message to Delta table: {df}")
        logging.error(f"Error: {e}")

def merge_into_delta_table(df, path):
    """
    Merge the given DataFrame into a Delta table located at the specified path.

    Parameters:
    - df (polars.DataFrame): The DataFrame to be merged into the Delta table.
    - path (str): The path to the Delta table.

    Returns:
    None

    Raises:
    - Exception: If an error occurs while merging the DataFrame into the Delta table.
    """
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

if __name__ == "__main__":
    consumer = create_consumer(topics, bootstrap_servers)
    for message in consumer:
        process_message(message.topic, message.value)

