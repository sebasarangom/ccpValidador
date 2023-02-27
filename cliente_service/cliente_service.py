import json
from kafka import KafkaConsumer

TOPIC_NAME = "availability"
availability_consumer = KafkaConsumer(
    TOPIC_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

def availability():
    for ava in availability_consumer:
        availability_data = ava.value
        return availability_data

def produceValidation():
    values = availability()
    print(values)
    if(values[0]== values[1] and values[0]== values[2]):
        return True
    else:
        False