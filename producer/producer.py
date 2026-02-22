import time
import json
import requests
# from kafka import kafkaproducer
from confluent_kafka import Producer

API_KEY = "d5qje41r01qhn30g37o0d5qje41r01qhn30g37og"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["MSFT", "TSLA", "GOOGL", "AMZN", "TTWO", "SPY", "BTC", "QQQ", "GLD"]
bootstrap_servers = 'localhost:29092'
topic = "finance_quotes"
producer = Producer({"bootstrap.servers": bootstrap_servers})

def fetch_and_send():
    for symbol in SYMBOLS:
        response = requests.get(f"{BASE_URL}?symbol={symbol}&token={API_KEY}")
        data = response.json()
        data['symbol'] = symbol
        data['timestamp'] = time.time()
        message = json.dumps(data).encode('utf-8')
        try: 
            producer.produce(topic, value=message)
            producer.flush()
            print(f"message Bien envoyé: {message}")
        except Exception as e:
            print(f"Error: {e}")

while True:
    fetch_and_send()
    time.sleep(5)

 