from kafka import KafkaConsumer
import json
import polars as pl
from deltalake import DeltaTable

# Kafka broker configuration
bootstrap_servers = ['localhost:9092']

# Delta Lake configuration
delta_path = './delta_tables'

# Create a Kafka consumer
consumer = KafkaConsumer('clients', 'departments', 'employees', 'sales',
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Consume messages from Kafka topics
for message in consumer:
    topic = message.topic
    data = message.value

    # Create a Polars DataFrame from the message data
    df = pl.from_records([data])

    # Write the DataFrame to a Delta table
    delta_table_path = f"{delta_path}/{topic}"
    try:
        df.write_delta(target = delta_table_path, mode = "append")
        print(f"Inserted message from topic '{topic}' to Delta table: {data}")
    except Exception as e:
        print(f"Failed to insert message from topic '{topic}' to Delta table: {data}")
        print(f"Error: {e}")
