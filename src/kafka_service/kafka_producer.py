# producer.py
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_registration_message(data):
    topic = "welcome_journey"
    producer.send(topic, value="You've successfully registerd! Welcome!!")
    producer.flush()
