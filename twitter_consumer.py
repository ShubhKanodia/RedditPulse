# import csv
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType

# # Create SparkSession
# spark = SparkSession.builder \
#     .appName("IPL Tweet Analyzer") \
#     .getOrCreate()

# # Define schema for tweets
# tweet_schema = StructType([
#     StructField("text", StringType(), True),
#     StructField("topic", StringType(), True),
#     StructField("sentiment", StringType(), True)
# ])

# # Read tweets from CSV file
# tweets_df = spark.read \
#     .option("header", True) \
#     .schema(tweet_schema) \
#     .csv("tweets.csv")

# # Define processing logic for each DataFrame
# batting_tweets = tweets_df.filter(tweets_df.topic == "batting")
# bowling_tweets = tweets_df.filter(tweets_df.topic == "bowling")
# fielding_tweets = tweets_df.filter(tweets_df.topic == "fielding")

# # Process batting tweets
# batting_processed = batting_tweets.select("text", "topic", "sentiment")
# batting_processed.show(truncate=False)

# # Process bowling tweets
# bowling_processed = bowling_tweets.select("text", "topic", "sentiment")
# bowling_processed.show(truncate=False)

# # Process fielding tweets
# fielding_processed = fielding_tweets.select("text", "topic", "sentiment")
# fielding_processed.show(truncate=False)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import window, count, col, desc, avg, max, min, expr
from pyspark.sql.window import Window
import mysql.connector

# Create SparkSession
spark = SparkSession.builder.appName("IPL Tweet Analyzer").getOrCreate()

# Define schema for tweets including new fields
tweet_schema = StructType([
    StructField("text", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("hashtags", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# MySQL connection setup
def mysql_connect():
    return mysql.connector.connect(host="localhost", user="root", passwd="password", database="tweet_db")

def write_to_mysql(df, topic):
    processed_df = df.filter(df.topic == topic).select("text", "topic", "sentiment", "likes", "hashtags", "timestamp")
    data = processed_df.collect()
    conn = mysql_connect()
    cursor = conn.cursor()
    for row in data:
        print(f"Consuming tweet: {row['text']} | Topic: {row['topic']} | Sentiment: {row['sentiment']} | Likes: {row['likes']} | Hashtags: {row['hashtags']} | Timestamp: {row['timestamp']}")
        sql = "INSERT INTO tweets (text, topic, sentiment, likes, hashtags, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (row['text'], row['topic'], row['sentiment'], row['likes'], row['hashtags'], row['timestamp']))
    conn.commit()
    cursor.close()
    conn.close()
def stream_processing(df):
    # Count tweets within a 5-second sliding window
    tweet_counts = df \
        .withWatermark("timestamp", "5 seconds") \
        .groupBy(
            window("timestamp", "5 seconds", "1 second"),
            "topic"
        ) \
        .count() \
        .orderBy("window")
    tweet_counts.show(truncate=False)

    # Group tweets by hashtags
    hashtag_groups = df.groupBy("hashtags").count().orderBy(desc("count"))
    hashtag_groups.show(truncate=False)

    # Apply aggregate functions on numerical data within 5-second sliding windows
    window_spec = Window.partitionBy("topic").orderBy("timestamp").rowsBetween(-5, 0)
    agg_df = df.select("*",
                       max("likes").over(window_spec).alias("max_likes"),
                       min("likes").over(window_spec).alias("min_likes"),
                       avg("likes").over(window_spec).alias("avg_likes"))
    agg_df.show(truncate=False)
# Read CSV data simulated as being read from Kafka
tweets_df = spark.read.schema(tweet_schema).csv("tweets.csv")

# Process and write each topic to MySQL
topics = ["batting", "bowling", "fielding"]
for topic in topics:
    write_to_mysql(tweets_df, topic)

# Perform stream processing operations
stream_processing(tweets_df)