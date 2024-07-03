# RedditPulse: Real-Time Subreddit Analyzer

## Project Overview

RedditPulse is a sophisticated data pipeline that fetches, processes, and analyzes Reddit posts in real-time. It demonstrates both streaming and batch processing capabilities, providing insights into subreddit trends, sentiment, and engagement metrics.

### Key Features

- Real-time Reddit data ingestion using PRAW
- Streaming data pipeline with Apache Kafka and Spark Structured Streaming
- Sentiment analysis using TextBlob
- Batch processing for historical data analysis
- MySQL database for data persistence

## Technology Stack

- Python 3.9+
- Apache Kafka
- Apache Spark
- MySQL
- Libraries: PRAW, pyspark, mysql-connector-python, pandas, TextBlob

## Installation and Setup

### Clone the repository:
```bash
git clone https://github.com/ShubhKanodia/RedditPulse.git
```

### Install dependencies:
```bash
pip install praw kafka-python pyspark mysql-connector-python pandas textblob
```

### Set up Kafka and Zookeeper (instructions for your specific OS)

### Create Kafka topic:
```bash
kafka-topics.sh --create --topic reddit_posts --bootstrap-server localhost:9092
```

### Configure your Reddit API credentials and MySQL connection details in `config.py`

## Usage

### Start the Reddit data stream:
```bash
python reddit_stream.py
```

### Run Spark streaming consumer:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 reddit_consumer.py
```

### Run batch processing (in a separate terminal):
```bash
python batch_processing.py
```

## Analysis Components

### Streaming Analysis

- Real-time ingestion of Reddit posts every minute
- Sentiment analysis using TextBlob
- 5-minute sliding window analytics for post count, average sentiment, and engagement metrics with watermarking feature

### Batch Processing

- Hourly analysis of posts from the last 24 hours
- Aggregates post count and engagement metrics (average, max, min likes)

## Project Structure

- `reddit_stream.py`: Fetches Reddit data and publishes to Kafka
- `reddit_consumer.py`: Spark Structured Streaming job for real-time analysis
- `batch_processing.py`: Hourly batch analysis of historical data
- `config.py`: Configuration settings for API credentials and database connection

## Results Visualization

### Streaming Analysis Results
### Producer
<img width="838" alt="Screenshot 2024-06-30 at 6 13 08 PM" src="https://github.com/ShubhKanodia/IPL_live_tweet_analaysis/assets/110471762/5fa503b8-9056-4591-90a5-25f37f6433c1">

### Spark Consumer
<img width="833" alt="Screenshot 2024-06-30 at 6 17 48 PM" src="https://github.com/ShubhKanodia/IPL_live_tweet_analaysis/assets/110471762/b35067cc-13dc-4d97-b346-1b07954013f0">


### Batch Processing Results
<img width="595" alt="Screenshot 2024-06-30 at 6 11 58 PM" src="https://github.com/ShubhKanodia/IPL_live_tweet_analaysis/assets/110471762/a8ce6c2e-28cb-4e51-8f19-b0f8c337d40c">




## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
