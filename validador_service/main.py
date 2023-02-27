from configparser import ConfigParser
from kafka import KafkaProducer, KafkaConsumer
import json
import random
import validador_service

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "bodegaalmacenamiento"
CLIENTE_TOPIC = "availability"

validator_producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers = KAFKA_SERVER
)

bodega_almacenamiento_consumer = KafkaConsumer(
    TOPIC_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

bodega_almacenamiento_consumer.subscribe(['validatorservice'])


def start_service(data):

    validation_success = {
    "Status":"Success",
}
    validation_fail = {
    "Status":"Fail",
}

    if(data['amount']==0):
        validator_producer.send(CLIENTE_TOPIC, validation_success)
        validator_producer.flush()
    else: 
        validator_producer.send(CLIENTE_TOPIC, validation_fail)
        validator_producer.flush()
    """ data=dataSelect()
    if(produceValidation(data)==True):
        validator_producer.send(CLIENTE_TOPIC, validation_success)
        validator_producer.flush()
    else:
        validacion_fallida = validationLogic()
        validator_producer.send(CLIENTE_TOPIC, validacion_fallida)
        validator_producer.flush()
 """

""" def produceValidation(data):
    #values = dataRequest()
    print(data)
    if(data[0]== data[1] and data[0]== data[2]):
        return True
    else:
        return False

def validationLogic():
    values = dataRequest()
    if(values[0]!= values[1] and values[0]== values[2]):
        validation_data = {
        "Value":"${values[1]} Response 3 is incorrect",
        }
        return validation_data

    elif(values[0]== values[1] and values[0]!= values[2]):
        return "${values[2]} Response 3 is incorrect"
    else:
        return "${values[0]} Response 1 is incorrect"

def dataRequest(data):
    amounts = []
    amounts.append(data)
    return amounts

def dataSelect():
    for inf in bodega_almacenamiento_consumer:
        inf_data = inf.value
        inf_amount= dataRequest(inf_data)
    return inf_amount
    #start_service(inf_data) """

for inf in bodega_almacenamiento_consumer:
        inf_data = inf.value
        start_service(inf_data)

if __name__ == '__main__':
    start_service()