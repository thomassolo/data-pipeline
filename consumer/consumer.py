from confluent_kafka import Consumer, KafkaError

bootstrap_servers = "localhost:29092"
topic = "michel"
group_id = "test_group"
consumer = Consumer({
    "bootstrap.servers" : bootstrap_servers,
    "group.id" : group_id,
    "auto.offset.reset": "earliest"
})
consumer.subscribe([topic])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print("Reached end of partition")
        else:
            print("Error: {}".format(msg.error()))
    else:
        print("Received message: {}".format(msg.value().decode("utf-8")))
