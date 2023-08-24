from kafka import KafkaProducer
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: str(v).encode('utf-8')
)

topic = 'random-numbers-topic'

for _ in range(10):
    random_number = random.randint(1, 100)
    producer.send(topic, value=random_number)
    print(f"Produced: {random_number}")
    time.sleep(5)

producer.close()
