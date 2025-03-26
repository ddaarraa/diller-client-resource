from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: v.encode('utf-8')
)

producer.send('diller-logs-queue', 'Hello from producer!')
producer.flush()
