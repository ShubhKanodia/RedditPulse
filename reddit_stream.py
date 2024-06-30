import praw
from kafka import KafkaProducer
import json
from datetime import datetime
import time
import logging
import mysql.connector
from config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, KAFKA_BROKER, KAFKA_TOPIC, MYSQL_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Authenticate to Reddit
reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID,
                     client_secret=REDDIT_CLIENT_SECRET,
                     user_agent=REDDIT_USER_AGENT)

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def mysql_connect():
    return mysql.connector.connect(**MYSQL_CONFIG)

def store_in_mysql(post):
    conn = mysql_connect()
    cursor = conn.cursor()
    sql = "INSERT INTO reddit_posts (text, likes, timestamp) VALUES (%s, %s, %s)"
    cursor.execute(sql, (post['text'], post['likes'], post['timestamp']))
    conn.commit()
    cursor.close()
    conn.close()

def fetch_and_send(subreddit_name, limit=10):
    subreddit = reddit.subreddit(subreddit_name)
    submissions = subreddit.new(limit=limit)
    for submission in submissions:
        post = {
            'text': submission.title,
            'likes': submission.score,
            'timestamp': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        }
        logging.info(f"Fetching post from {subreddit_name}: {post}")
        producer.send(KAFKA_TOPIC, post)
        store_in_mysql(post)
        logging.info(f"Sent post to {KAFKA_TOPIC} and stored in MySQL: {post}")

def main():
    try:
        while True:
            fetch_and_send('cricket')
            time.sleep(60)  # Wait for 1 minute before next poll
    except KeyboardInterrupt:
        logging.info("Stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()