import json
from kafka import KafkaConsumer

TOPIC_NAME = "bodegaalmacenamiento"
bodega_almacenamiento_consumer = KafkaConsumer(
    TOPIC_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

def produceValidation():
    values = dataRequest()
    print(values)
    if(values[0]== values[1] and values[0]== values[2]):
        return True
    else:
        False

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

def dataRequest():
    amounts = []
    for inf in bodega_almacenamiento_consumer:
        amounts.append(inf.value)
        print(amounts)
    return amounts