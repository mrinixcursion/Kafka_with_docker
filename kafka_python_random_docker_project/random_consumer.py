from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'random-numbers-topic',
    bootstrap_servers=['localhost:29092'],
     group_id='my-random-consumer-group',  
    value_deserializer=lambda v: int(v.decode('utf-8'))
)

while True:
    for message in consumer:
        random_number = message.value
        #print(message)
        if random_number % 2 == 0:
            print(f"Even number consumed: {random_number}")
        else:
            print(f"Odd number consumed: {random_number}")

consumer.close()
