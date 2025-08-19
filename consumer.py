import MySQLdb
from kafka import KafkaConsumer
import json
from datetime import datetime

print("✅ Starting Kafka consumer…")
db = MySQLdb.connect(
    host="127.0.0.1",
    user="root",
    passwd="Jyo7483##",
    db="teamtalk"
)
cursor = db.cursor()
print("✅ MySQL connected!")

consumer = KafkaConsumer(
    'teamtalk_chat',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='teamtalk-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("📥 Kafka consumer listening…")

for message in consumer:
    data = message.value
    sender = data['sender']
    msg = data['message']
    chatroom = data.get('chatroom')
    receiver = data.get('receiver')
    ts = data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    cursor.execute(
        """
        INSERT INTO messages (sender, message, timestamp, chatroom, receiver)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (sender, msg, ts, chatroom, receiver)
    )
    db.commit()
    print(f"💾 Saved: [{chatroom or receiver}] [{ts}] {sender}: {msg}")
