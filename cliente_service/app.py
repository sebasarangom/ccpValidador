from flask import Flask, Response
from threading import Thread
import json
import cliente_service
from kafka import KafkaConsumer

app = Flask(__name__)


@app.route('/validation', methods=['GET'])
def produceValidation():

    TOPIC_NAME = "availability"
    availability_consumer = KafkaConsumer(
    TOPIC_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)
    availability_consumer.subscribe(topics=[TOPIC_NAME])
    def message():
         for message in availability_consumer:
            print(message.value)     
            availability= message.value["Status"]
            return availability
    
    return Response(message())

if __name__ == '__main__':
     app.run()

# @app.before_first_request
# def launch_consumer():
#     t = Thread(target=cliente_service.availability)
#     t.start()

