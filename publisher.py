from kafka import KafkaProducer
import json
import time

# Kafka broker configuration
bootstrap_servers = ['localhost:9092']

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Define the topics
topics = ['clients', 'departments', 'employees', 'sales']

# Sample data for each topic
data = {
    'clients': [
        {'id': 1, 'name': 'Client 1'},
        {'id': 2, 'name': 'Client 2'}
    ],
    'departments': [
        {'id': 1, 'name': 'Sales'},
        {'id': 2, 'name': 'Marketing'}
    ],
    'employees': [
        {'id': 1, 'name': 'John Doe', 'department_id': 1},
        {'id': 2, 'name': 'Jane Smith', 'department_id': 2}
    ],
    'sales': [
        {'id': 1, 'client_id': 1, 'amount': 1000},
        {'id': 2, 'client_id': 2, 'amount': 2000}
    ]
}

# Publish data to each topic
for topic in topics:
    for message in data[topic]:
        producer.send(topic, value=message)
        print(f"Sent message to topic '{topic}': {message}")
        time.sleep(1)  # Add a delay between messages for demonstration purposes

# Flush and close the producer
producer.flush()
producer.close()
