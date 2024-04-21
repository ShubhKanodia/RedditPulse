# # # import logging
# # # from kafka import KafkaProducer
# # # import tweepy
# # # import json
# # # import re
# # # from tweepy import RateLimitHandler

# # # # Create a rate limit handler
# # # rate_limit_handler = RateLimitHandler()

# # # # Configure logging
# # # logging.basicConfig(level=logging.INFO)
# # # logger = logging.getLogger(__name__)

# # # # Load configuration
# # # with open('config.json') as f:
# # #     config = json.load(f)

# # # # Twitter API authentication
# # # client = tweepy.Client(
# # #     consumer_key=config['twitter']['consumer_key'],
# # #     consumer_secret=config['twitter']['consumer_secret'],
# # #     access_token=config['twitter']['access_token'],
# # #     access_token_secret=config['twitter']['access_token_secret'],
# # #     bearer_token=config['twitter']['bearer_token'],
# # #     wait_on_rate_limit=True,
# # #     rate_limit_handler=rate_limit_handler
# # # )

# # # # Create Kafka producer
# # # producer = KafkaProducer(bootstrap_servers=config['kafka']['bootstrap_servers'],
# # #                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# # # # Stream tweets and send to respective topics
# # # class MyStreamingClient(tweepy.StreamingClient):
# # #     def on_tweet(self, tweet):
# # #         try:
# # #             tweet_data = {'text': tweet.text, 'user': tweet.author.username}
# # #             if re.search(r'\bbatting\b', tweet.text, re.IGNORECASE):
# # #                 producer.send(config['kafka']['topics']['batting'], value=tweet_data)
# # #             elif re.search(r'\bbowling\b', tweet.text, re.IGNORECASE):
# # #                 producer.send(config['kafka']['topics']['bowling'], value=tweet_data)
# # #             elif re.search(r'\bfielding\b', tweet.text, re.IGNORECASE):
# # #                 producer.send(config['kafka']['topics']['fielding'], value=tweet_data)
# # #         except Exception as e:
# # #             logger.error(f"Error sending tweet to Kafka: {str(e)}")

# # #     def on_error(self, status_code):
# # #         if status_code == 420:
# # #             return False
# # #         else:
# # #             logger.error(f"Twitter streaming error: {status_code}")

# # # # Start streaming
# # # try:
# # #     streaming_client = MyStreamingClient(bearer_token=config['twitter']['bearer_token'])
# # #     rules = tweepy.StreamRule("IPL")
# # #     streaming_client.add_rules(rules)
# # #     streaming_client.filter(tweet_fields=["author_id", "text"])
# # # except Exception as e:
# # #     logger.error(f"Error occurred during Twitter stream: {str(e)}")


# # import logging
# # from kafka import KafkaProducer
# # import tweepy
# # import json
# # import re
# # from tweepy import OAuth2BearerHandler

# # # Configure logging
# # logging.basicConfig(level=logging.INFO)
# # logger = logging.getLogger(__name__)

# # # Load configuration
# # with open('config.json') as f:
# #     config = json.load(f)

# # # Twitter API authentication
# # client = tweepy.Client(
# #     bearer_token=config['twitter']['bearer_token'],
# #     consumer_key=config['twitter']['client_id'],
# #     consumer_secret=config['twitter']['client_secret'],
# #     wait_on_rate_limit=True,
# # )

# # # Create Kafka producer
# # producer = KafkaProducer(bootstrap_servers=config['kafka']['bootstrap_servers'],
# #                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# # # Stream tweets and send to respective topics
# # class MyStreamingClient(tweepy.StreamingClient):
# #     def on_tweet(self, tweet):
# #         try:
# #             tweet_data = {'text': tweet.text, 'user': tweet.author.username}
# #             if re.search(r'\bbatting\b', tweet.text, re.IGNORECASE):
# #                 producer.send(config['kafka']['topics']['batting'], value=tweet_data)
# #             elif re.search(r'\bbowling\b', tweet.text, re.IGNORECASE):
# #                 producer.send(config['kafka']['topics']['bowling'], value=tweet_data)
# #             elif re.search(r'\bfielding\b', tweet.text, re.IGNORECASE):
# #                 producer.send(config['kafka']['topics']['fielding'], value=tweet_data)
# #         except Exception as e:
# #             logger.error(f"Error sending tweet to Kafka: {str(e)}")

# #     def on_error(self, status_code):
# #         if status_code == 420:
# #             return False
# #         else:
# #             logger.error(f"Twitter streaming error: {status_code}")

# # # Start streaming
# # try:
# #     streaming_client = MyStreamingClient(bearer_token=config['twitter']['bearer_token'])
# #     rules = tweepy.StreamRule("IPL")
# #     streaming_client.add_rules(rules)
# #     streaming_client.filter(tweet_fields=["author_id", "text"])
# # except Exception as e:
# #     logger.error(f"Error occurred during Twitter stream: {str(e)}")

# from confluent_kafka import Producer
# import csv
# import time

# # Kafka configuration
# conf = {'bootstrap.servers': "localhost:9092"}
# producer = Producer(conf)

# def delivery_report(err, msg):
#     if err is not None:
#         print("Message delivery failed:", err)
#     else:
#         print(f"Message delivered to {msg.topic()} partition [{msg.partition()}]")

# def kafka_producer(filename, bootstrap_servers='localhost:9092', delay=0.5):
#     # Configure the producer
#     conf = {'bootstrap.servers': bootstrap_servers}
#     producer = Producer(conf)

#     # Read the CSV file
#     with open(filename, 'r', encoding='utf-8') as file:
#         reader = csv.DictReader(file)
#         for row in reader:
#             tweet_text = row['text']
#             topic = row['topic']

#             # Produce the tweet to the corresponding Kafka topic
#             producer.produce(f"{topic}_topic", value=tweet_text.encode('utf-8'), callback=delivery_report)
            

#             # Simulate a delay between producing messages
#             time.sleep(delay)

#     # Flush the producer to ensure all messages are delivered
#     producer.flush()

# # Example usage
# kafka_producer('tweets.csv', delay=0.5)
from confluent_kafka import Producer
import csv
import time

def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed:", err)
    else:
        print(f"Message delivered to {msg.topic()} partition [{msg.partition()}]")

def kafka_producer(filename, bootstrap_servers='localhost:9092', delay=5):
    conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(conf)

    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Create a CSV line from the data
            data_str = f"{row['text']},{row['topic']},{row['sentiment']},{row['likes']},{row['hashtags']},{row['timestamp']}"
            # Produce the tweet to the corresponding Kafka topic
            producer.produce(f"{row['topic']}_topic", value=data_str.encode('utf-8'), callback=delivery_report)
            print(f"Produced tweet to {row['topic'].capitalize()} topic: {row['text']}")
            time.sleep(delay)  # Simulate a delay between producing messages

    producer.flush()

# Example usage
kafka_producer('tweets.csv', delay=0.1)