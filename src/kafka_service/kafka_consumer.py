# consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'welcome_journey',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for new user registrations...\n")
for message in consumer:
    print(f"Received message: {message.value}")
