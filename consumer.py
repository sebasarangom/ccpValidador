from kafka import KafkaConsumer, KafkaProducer
import os
import json
import uuid
from concurrent.futures import ThreadPoolExecutor

TOPIC_NAME = "validator-events"

KAFKA_SERVER = "localhost:9092"

VALIDATOR_TOPIC = "validator"
SERVICE_TOPIC = "service"

consumer = KafkaConsumer(
    TOPIC_NAME,
    # to deserialize kafka.producer.object into dict
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

producer = KafkaProducer(
	value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

def processFunction(data):
	validation_data = {
    "hol":"saludo",
    "sabor": "maracuya"
}
	service_data = {
    "hol":"saludo",
    "sabor": "maracuya"
}
	producer.send(VALIDATOR_TOPIC, validation_data)
	producer.flush()
	producer.send(SERVICE_TOPIC, service_data)
	producer.flush()

for inf in consumer:
	inf_data = inf.value
	processFunction(inf_data)