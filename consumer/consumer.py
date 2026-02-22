from confluent_kafka import Consumer, KafkaError
import boto3
import json 
import time
from datetime import datetime


s3 = boto3.client('s3',
                  endpoint_url = "http://localhost:9002",
                  aws_access_key_id = "admin",
                  aws_secret_access_key = "password123")

bucket_name = "bronze-transactions"
bootstrap_servers = "localhost:29092"
topic = "finance_quotes"
group_id = "bronze-consumer"
consumer = Consumer({
    "bootstrap.servers" : bootstrap_servers,
    "group.id" : group_id,
    "auto.offset.reset": "earliest"
})
consumer.subscribe([topic])
print("Consumer is listening to topic:", topic)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Erreur: {msg.error()}")
            break
    record = json.loads(msg.value().decode('utf-8'))
    symbol = record.get("symbol")
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    key = f"{symbol}/{symbol}_{ts}.json"
    s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(record), ContentType="application/json")
    print(f"message Bien envoyé: {record}")