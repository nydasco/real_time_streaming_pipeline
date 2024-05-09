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

def create_producer(bootstrap_servers):
    """
    Creates a KafkaProducer instance with the given bootstrap servers.

    Parameters:
    - bootstrap_servers (str): The list of Kafka bootstrap servers.

    Returns:
    - KafkaProducer: The KafkaProducer instance.

    """
    return KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        value_serializer = lambda v: json.dumps(v).encode("utf-8")
    )

def send_messages(producer, topic, messages):
    """
    Sends a batch of messages to the specified topic using the given producer.

    Args:
        producer (object): The producer object used to send messages.
        topic (str): The name of the topic to send messages to.
        messages (list): A list of messages to send.

    Returns:
        None

    Raises:
        Exception: If there is an error while sending the messages.

    """
    try:
        for message in messages:
            producer.send(topic, value=message)
        producer.flush()
        logging.info(f"Batch of messages sent to topic '{topic}'.")
    except Exception as e:
        logging.error(f"Failed to send messages to topic '{topic}'. Error: {e}")

def process_files_and_send(producer, topics):
    """
    Process files and send their contents to a Kafka producer.

    Args:
        producer (KafkaProducer): The Kafka producer object used to send messages.
        topics (list): A list of topics to process and send messages for.

    Returns:
        None

    Raises:
        FileNotFoundError: If a file for a topic is not found.
        Exception: If there is an error processing a file for a topic.

    """
    for topic in topics:
        try:
            with open(f"{raw_path}/{topic}.csv", encoding="utf-8-sig") as csvfile:
                csvreader = csv.DictReader(csvfile)
                batch = []
                batch_size = 10  # Adjust batch size based on your scenario. 1 is realtime, while 100 will be faster but consume more memory.

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

