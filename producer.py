# producer.py
from kafka import KafkaProducer
import json
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_message_to_kafka(sender, message, chatroom=None, receiver=None):
    payload = {
        'sender': sender,
        'message': message,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'chatroom': chatroom,
        'receiver': receiver
    }
    producer.send('teamtalk_chat', payload)
    producer.flush()
