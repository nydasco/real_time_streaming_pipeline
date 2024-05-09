from kafka import KafkaProducer
import csv
import json
import time
import logging

# Configuration
logging.basicConfig(level = logging.INFO)
bootstrap_servers = ["localhost:9092"]
raw_path = "./raw_data"
topics = ["client", "department", "employee", "sale"]

# Function to create and configure Kafka producer
def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        value_serializer = lambda v: json.dumps(v).encode("utf-8")
    )

# Function to send batch messages to Kafka
def send_messages(producer, topic, messages):
    try:
        for message in messages:
            producer.send(topic, value = message)
        producer.flush()
        logging.info(f"Batch of messages sent to topic '{topic}'.")
    except Exception as e:
        logging.error(f"Failed to send messages to topic '{topic}'. Error: {e}")

# Main function to process CSV files and send to Kafka
def process_files_and_send(producer, topics):
    for topic in topics:
        try:
            with open(f"{raw_path}/{topic}.csv", encoding = "utf-8-sig") as csvfile:
                csvreader = csv.DictReader(csvfile)
                batch = []
                batch_size = 10  # Adjust batch size based on your scenario

                for rows in csvreader:
                    batch.append(rows)
                    if len(batch) >= batch_size:
                        send_messages(producer, topic, batch)
                        batch = []

                # Send any remaining messages in the last batch
                if batch:
                    send_messages(producer, topic, batch)

        except FileNotFoundError:
            logging.error(f"File not found: {raw_path}/{topic}.csv")
        except Exception as e:
            logging.error(f"Error processing file for topic '{topic}': {e}")

if __name__ == "__main__":
    producer = create_producer(bootstrap_servers)
    
    try:
        process_files_and_send(producer, topics)
    finally:
        producer.close()

