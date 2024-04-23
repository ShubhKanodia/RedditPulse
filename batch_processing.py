# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# from pyspark.sql.functions import col, desc, avg, max, min, expr

# # Create SparkSession
# spark = SparkSession.builder.appName("IPL Tweet Analyzer").getOrCreate()

# # Define schema for tweets including new fields
# tweet_schema = StructType([
#     StructField("text", StringType(), True),
#     StructField("topic", StringType(), True),
#     StructField("sentiment", StringType(), True),
#     StructField("likes", IntegerType(), True),
#     StructField("hashtags", StringType(), True),
#     StructField("timestamp", TimestampType(), True)
# ])

# # Read CSV data
# tweets_df = spark.read.schema(tweet_schema).csv("tweets.csv")

# # Batch Processing

# # 1. Count tweets by topic
# tweet_counts = tweets_df.groupBy("topic").count().orderBy("count", descending=False)
# print("Tweet counts by topic:")
# tweet_counts.show(truncate=False)

# # 2. Group tweets by hashtags
# hashtag_groups = tweets_df.groupBy("hashtags").count().orderBy(desc("count"))
# print("Tweets grouped by hashtags:")
# hashtag_groups.show(truncate=False)

# # 3. Apply aggregate functions on numerical data
# agg_df = tweets_df.select("*",
#                           max("likes").alias("max_likes"),
#                           min("likes").alias("min_likes"),
#                           avg("likes").alias("avg_likes")).groupBy("topic")
# print("Aggregate stats for likes:")
# agg_df.show(truncate=False)

import mysql.connector
from tabulate import tabulate  # Install tabulate: pip install tabulate

# MySQL connection setup
mysql_config = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "tweet_db"
}

# Establish a connection to the MySQL database
conn = mysql.connector.connect(**mysql_config)
cursor = conn.cursor()

# Batch Processing

# 1. Count tweets by topic
query = "SELECT topic, COUNT(*) AS count FROM tweets GROUP BY topic ORDER BY count"
cursor.execute(query)
print("\nTweet counts by topic (Batch Processing):")
print(tabulate(cursor.fetchall(), headers=["Topic", "Count"], tablefmt="grid"))

# 2. Group tweets by hashtags
query = "SELECT hashtags, COUNT(*) AS count FROM tweets GROUP BY hashtags ORDER BY count DESC"
cursor.execute(query)
print("\nTweets grouped by hashtags (Batch Processing):")
print(tabulate(cursor.fetchall(), headers=["Hashtags", "Count"], tablefmt="grid"))

# 3. Apply aggregate functions on numerical data
query = """
SELECT topic, MAX(likes) AS max_likes, MIN(likes) AS min_likes, AVG(likes) AS avg_likes
FROM tweets
GROUP BY topic
ORDER BY topic
"""
cursor.execute(query)
print("\nAggregate stats for likes (Batch Processing):")
print(tabulate(cursor.fetchall(), headers=["Topic", "Max Likes", "Min Likes", "Avg Likes"], tablefmt="grid"))

# Close the database connection
cursor.close()
conn.close()