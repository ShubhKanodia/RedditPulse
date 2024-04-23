# producer_consumer.py
from kafka import KafkaProducer, KafkaConsumer
import json

# Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))



# Produce a message
message = {"message": "Hello, Kafka!"}
producer.send('test-topic', message)
print("Message produced: ", message)

# Kafka Consumer
consumer = KafkaConsumer('test-topic',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Consume messages
print("Consuming messages...")
for message in consumer:
    print("Message consumed: ", message.value)