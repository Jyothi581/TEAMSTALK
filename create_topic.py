from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

admin = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='teamtalk-admin')
topic = NewTopic(name='teamtalk_chat', num_partitions=1, replication_factor=1)

try:
    admin.create_topics(new_topics=[topic], validate_only=False)
    print("✅ Kafka topic 'teamtalk_chat' created.")
except TopicAlreadyExistsError:
    print("⚠️ Kafka topic already exists.")
