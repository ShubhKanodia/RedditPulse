from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, max, min, expr, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from textblob import TextBlob
from config import KAFKA_BROKER, KAFKA_TOPIC

def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

def create_spark_session():
    return SparkSession.builder \
        .appName("RedditStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

def main():
    spark = create_spark_session()

    # Register UDF for sentiment analysis
    sentiment_udf = udf(get_sentiment, FloatType())
    spark.udf.register("sentiment", sentiment_udf)

    # Define schema for the incoming data
    schema = StructType([
        StructField("text", StringType()),
        StructField("likes", IntegerType()),
        StructField("timestamp", TimestampType())
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse the JSON data
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Add sentiment column
    sentiment_df = parsed_df.withColumn("sentiment", sentiment_udf(col("text")))

    # Sliding window analysis
    window_duration = "5 minutes"
    slide_duration = "1 minute"

    windowed_df = sentiment_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(window("timestamp", window_duration, slide_duration)) \
        .agg(
            count("*").alias("post_count"),
            avg("sentiment").alias("avg_sentiment"),
            avg("likes").alias("avg_likes"),
            max("likes").alias("max_likes"),
            min("likes").alias("min_likes")
        )

    # Start the streaming query
    query = windowed_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()