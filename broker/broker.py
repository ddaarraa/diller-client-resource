import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from pymongo import MongoClient

# Load environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
TOPIC = "messages"

# Connect to MongoDB (Retry until successful)
while True:
    try:
        client = MongoClient(MONGO_URI)
        db = client["broker_db"]  # Database name
        collection = db["messages"]  # Collection name
        print("✅ Connected to MongoDB")
        break
    except Exception as e:
        print(f"❌ Retrying MongoDB connection: {e}")
        time.sleep(5)

while True:
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        existing_topics = admin_client.list_topics()
        
        if TOPIC not in existing_topics:
            print(f"⚠️ Topic '{TOPIC}' does not exist. Creating it...")
            topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics([topic])
            print(f"✅ Created topic '{TOPIC}'")
        else:
            print(f"✅ Topic '{TOPIC}' already exists")

        break
    except Exception as e:
        print(f"❌ Retrying Kafka connection: {e}")
        time.sleep(5)

# Connect to Kafka Consumer
while True:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        print("✅ Connected to Kafka")
        break
    except Exception as e:
        print(f"❌ Retrying Kafka consumer connection: {e}")
        time.sleep(5)

# Process messages from Kafka and insert into MongoDB
for message in consumer:
    print(f"📩 Received: {message.value}")
    collection.insert_one(message.value)
    print("✅ Inserted into MongoDB")