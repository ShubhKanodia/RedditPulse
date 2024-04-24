# IPL Tweet Analyzer

This project is a real-time tweet analysis system for the Indian Premier League (IPL) cricket tournament. It simulates generating tweets related to batting, bowling, and fielding, streams the tweets using Apache Kafka, performs real-time analysis using Apache Spark Streaming, and stores the tweet data in a MySQL database. Additionally, it performs batch processing on the stored data for comparison with the real-time streaming results.

## Prerequisites

Before running the project, ensure that you have the following installed:

- Python 3.9 or above
- Apache Kafka
- Apache Zookeeper
- MySQL

## Installation

1. Clone the repository:

```bash
https://github.com/ShubhKanodia/IPL_live_tweet_analaysis.git
```

2. Install the required Python packages:

```bash
pip install faker confluent-kafka mysql-connector-python
```

## Running the Project

Follow these steps to run the project:

1. Start the Zookeeper service:

```bash
zookeeper-server-start.sh /path/to/kafka/config/zookeeper.properties
```

2. Start the Kafka server in a separate terminal:

```bash
kafka-server-start.sh /path/to/kafka/config/server.properties
```

3. Create Kafka topics for batting, bowling, and fielding:

```bash
kafka-topics.sh --create --topic batting_topic --bootstrap-server localhost:9092
kafka-topics.sh --create --topic bowling_topic --bootstrap-server localhost:9092
kafka-topics.sh --create --topic fielding_topic --bootstrap-server localhost:9092
```

4. Generate and populate the CSV file with simulated tweets:

```bash
python generate_new_tweets.py
```

5. Run the Kafka producer to send tweets to the respective topics:

```bash
python twitter_producer.py
```

6. In a new terminal, run the Kafka spark consumer to consume tweets from the topics, perform real-time streaming analysis using Apache Spark, and populate the MySQL database:

```bash
 spark-3.5.1-bin-hadoop3 % ./bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /path/to/twitter_consumer.py
```

7. After running the consumer, run the batch processing script to perform the same analysis on the entire dataset stored in the MySQL database:

```bash
python batch_processing.py
```

## Analysis

The project performs the following analysis:

1. **Count tweets by topic**: Counts the number of tweets for each topic (batting, bowling, fielding) within a 5-second sliding window (for streaming) and on the entire dataset (for batch processing).

2. **Group tweets by hashtags**: Groups the tweets by hashtags and counts the number of tweets for each hashtag group within a 5-second sliding window (for streaming) and on the entire dataset (for batch processing).

3. **Aggregate stats for likes**: Calculates the maximum, minimum, and average likes for tweets within a 5-second sliding window, partitioned by topic (for streaming) and on the entire dataset, grouped by topic (for batch processing).

The results of the streaming analysis are displayed in real-time, while the batch processing results are displayed after running the `batch_processing.py` script.

## Evaluation and Comparison

After running both the streaming and batch processing scripts, you can compare the results, accuracy, and performance between the two modes of execution. The accuracy can be evaluated by comparing the results for each operation (count tweets by topic, group tweets by hashtags, and aggregate stats for likes). The performance can be measured by recording the execution time of each script.

## Notes

1. The 5-second sliding window used in the streaming analysis is a configurable parameter. It can be adjusted based on the desired granularity and latency requirements.

2. The producer code (`producer.py`) reads the simulated tweets from the CSV file and produces them to the respective Kafka topics (`batting_topic`, `bowling_topic`, and `fielding_topic`). The code uses the `confluent-kafka` library to interact with Kafka.

3. The consumer code (`consumer.py`) consumes tweets from the Kafka topics, performs real-time streaming analysis using Apache Spark Streaming, and stores the tweet data in a MySQL database. The code uses the `pyspark` library for streaming analysis and the `mysql-connector-python` library to interact with the MySQL database.

4. The batch processing script (`batch_processing.py`) reads the tweet data from the MySQL database and performs the same analysis as the streaming mode, but on the entire dataset. The script uses the `mysql-connector-python` library to interact with the MySQL database.
