import multiprocessing
import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from pymongo import MongoClient

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9093")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpassword@localhost:27017/?directConnection=true&serverSelectionTimeoutMS=2000&authSource=admin&appName=mongosh+2.4.0")
TOPIC = "diller-logs-queue"

def listen_for_new_messages(collection):
    try:
        client = MongoClient(MONGO_URI)
        db = client["logs_db"]  # Use your actual DB name
        collection_watcher = db[collection]

        print("Listening for new messages in the ", collection, " collection...")


        # Create a Change Stream to listen for new inserts
        producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),  # Serialize messages
                )

        with collection_watcher.watch() as stream:
            for change in stream:
                if change["operationType"] == "insert":
                    print("🔥 New message detected:", change["fullDocument"])

                    producer.send(TOPIC, value=change["fullDocument"])
                    producer.flush()  # Ensure message is sent
                    print(f"✅ Message sent to Kafka Topic: {TOPIC}")

    except Exception as e:
        print(f"Error: {e}")



# Kafka Topic Check and Creation
def check_and_create_topic():
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            print(f"🔄 Connecting to Kafka at {KAFKA_BROKER}...")
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


if __name__ == "__main__":
    processes = []
    collections = ["sys_logs_collection", "application_logs_collection", "vpc_logs_collection"]

    check_and_create_topic() # initiation to kafka

    for coll in collections:
        p = multiprocessing.Process(target=listen_for_new_messages, args=(coll,))
        p.start()
        print("process : ", coll.index , " started")
        processes.append(p)

    for p in processes:
        p.join()




















# # Kafka Consumer Setup
# def consume_and_insert_messages():
#     while True:
#         try:
#             # Connect to Kafka Consumer
#             print(f"🔄 Connecting to Kafka Consumer on topic '{TOPIC}'...")
#             consumer = KafkaConsumer(
#                 TOPIC,
#                 bootstrap_servers=KAFKA_BROKER,
#                 auto_offset_reset="earliest",
#                 enable_auto_commit=True,
#                 value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#             )
#             print("✅ Connected to Kafka Consumer")
#             break
#         except Exception as e:
#             print(f"❌ Retrying Kafka consumer connection: {e}")
#             time.sleep(5)

#     # MongoDB Client Setup
#     print("🔄 Connecting to MongoDB...")
#     client = MongoClient(MONGO_URI)
#     db = client['kafka_db']  
#     collection = db['messages'] 

#     # Start consuming messages
#     print("🔄 Starting to consume messages...")
#     for message in consumer:
#         print(f"📩 Received message: {message.value}")
#         try:
#             collection.insert_one(message.value)
#             print("✅ Inserted message into MongoDB")
#         except Exception as e:
#             print(f"❌ Error inserting message into MongoDB: {e}")
