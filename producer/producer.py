import time
import json
import requests
# from kafka import kafkaproducer
from confluent_kafka import Producer

API_KEY = "d5qje41r01qhn30g37o0d5qje41r01qhn30g37og"
BASE_URL = "https://finnhub.io/api/v1/quote"

bootstrap_servers = 'localhost:29092'
topic = "michel"
producer = Producer({"bootstrap.servers": bootstrap_servers})
message = "messzge oui"
producer.produce(topic, value=message)
producer.flush()
print("message Bien envoyé")


 