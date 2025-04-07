import json
import os
import logging
import schedule
import time
from kafka import KafkaConsumer
from datetime import datetime
from clustering_service import calculate_log_correlation

logging.basicConfig(
    level=logging.INFO,  # Log only INFO and above
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]  # Logs to stdout
)
logger = logging.getLogger(__name__)

# Suppress Kafka internal logs by adjusting Kafka's log level
logging.getLogger("kafka").setLevel(logging.WARNING)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "diller-logs-queue"


# Kafka Consumer function
def consume_kafka_messages():
    logger.info("Connecting to Kafka...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        group_id='diller-group',
        enable_auto_commit=True,
        max_poll_records=20
    )

    logger.info("Consuming messages from Kafka...")

    # Pass the function reference using a lambda
    schedule.every(60).seconds.do(lambda: polling_message(consumer=consumer))
    while True :
        schedule.run_pending()
        # schedule.run_pending()
        time.sleep(1)
    
def polling_message(consumer) :
    messages = consumer.poll(timeout_ms=10000)

    total_messages = sum(len(msgs) for msgs in messages.values())
    export_messages = []
    if messages:
        logger.info(f"{total_messages} messages found.")
        for tp, msgs in messages.items():
            for message in msgs:
                # logger.info(f"Consumed message: {message.value.decode('utf-8')}")
                export_messages.append(json.loads(message.value.decode('utf-8')))
        # logger.info(f"export message :{export_messages}")
        try :
            calculate_log_correlation(export_messages)
        except ValueError:
            logger.info(f"{ValueError}")
        finally :
            logger.info(f"stop polling")
    else:
        logger.info("No new messages found.")




if __name__ == '__main__':
    logger.info("Starting the scheduled Kafka service...")
    consume_kafka_messages()