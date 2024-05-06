from kafka import KafkaProducer
import csv
import json
import time

bootstrap_servers = ["localhost:9092"]

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

with open("raw_data/sale.csv", encoding="utf-8-sig") as csvfile:
    csvreader = csv.DictReader(csvfile)
    for rows in csvreader:
        print(json.dumps(rows))
        producer.send("sales", value = json.dumps(rows))
        print(f"Sent message to topic 'sales': json.dumps(rows)")

producer.flush()
producer.close()
