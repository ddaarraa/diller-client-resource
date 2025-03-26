import os
from kafka import KafkaConsumer
import schedule
import time
from datetime import datetime

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9093")
TOPIC = "diller-logs-queue"


# Kafka Consumer
def consume_kafka_messages():
    print("Connecting to Kafka...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',  
        group_id='my-group1',
        enable_auto_commit=True,
        
    )

    # Consume new messages for a short time, then exit
    # print("Consuming messages from Kafka...")
    print(f"[{datetime.now()}] Consuming messages from Kafka...")
    
    messages = consumer.poll(timeout_ms=10000)
    total_messages = sum(len(msgs) for msgs in messages.values())
    
    if messages:
        
        print(total_messages, "messages found.")
        for tp, msgs in messages.items():
            for message in msgs:
                print(f"Consumed message: {message.value.decode('utf-8')}")
    else:
        print("No new messages found.")

    # Close the consumer after processing messages
    consumer.close()  
    print( f"[{datetime.now()}]Consumer connection closed.")


schedule.every(30).seconds.do(consume_kafka_messages)

if __name__ == '__main__':
    print("Starting the scheduled Kafka service...")
    while True:
        schedule.run_pending() 
        time.sleep(1)  