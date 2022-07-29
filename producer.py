from kafka import KafkaProducer
import json
from data import get_registered_user
import time

'''We would need bootstrap server and value serializer details to use the KafkaProduce
KafkaProducer need to connect to a kafka server or broker, so we need to provide the details in the configuration
'''

# FOR SINGLE BROKER - 1 PARTITION


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['172.29.179.144:9092'], value_serializer=json_serializer)

# To send some data we need to get the data (data.py)
if __name__ == "__main__":
    while 1 == 1:
        user_registration = get_registered_user()
        print(user_registration)
        producer.send("user_registration", user_registration)
        time.sleep(4)