from flask import Flask, request, jsonify
import json
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer
from faker import Faker
from faker.generator import random
import time

app = Flask(__name__)

numeros = [
    {'amount': 0},
    {'amount': 1}
]

TOPIC_NAME = "validatorservice"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

@app.route('/bodegaalmancenamiento', methods=['GET'])
def kafkaProducer():
        while True:
            producer.send(TOPIC_NAME, get_numero())
            producer.flush()
            producer.send(TOPIC_NAME, get_numero())
            producer.flush()
            producer.send(TOPIC_NAME, get_numero())
            producer.flush()
            print("Sent to consumer")
            return jsonify({
            "message": "You will receive an email in a short while with the plot", 
            "status": "Pass"})


def get_numero():
    if round(random.random(), 1) < 0.7:
       return numeros[1]

    return numeros[0]

if __name__ == "__main__":
    app.run(debug=True, port = 5000)