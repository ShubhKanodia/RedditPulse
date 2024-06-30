import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import time
from config import MYSQL_CONFIG

def mysql_connect():
    return mysql.connector.connect(**MYSQL_CONFIG)

def batch_process():
    try:
        conn = mysql_connect()
        
        # Fetch data from MySQL for the last day
        query = "SELECT * FROM reddit_posts WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 1 DAY)"
        df = pd.read_sql(query, conn, parse_dates=['timestamp'])
        
        if df.empty:
            print("No data found for the last 24 hours.")
            return

        # Ensure timestamp is in datetime format
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Remove any rows where timestamp couldn't be parsed
        df = df.dropna(subset=['timestamp'])
        
        if df.empty:
            print("No valid data found after parsing timestamps.")
            return

        # Perform batch analysis
        df['hour'] = df['timestamp'].dt.floor('H')
        hourly_analysis = df.groupby('hour').agg({
            'text': 'count',
            'likes': ['mean', 'max', 'min']
        })
        
        hourly_analysis.columns = ['post_count', 'avg_likes', 'max_likes', 'min_likes']
        hourly_analysis = hourly_analysis.reset_index()
        
        print("Hourly Batch Analysis:")
        print(hourly_analysis)
        
    except mysql.connector.Error as e:
        print(f"Error while connecting to MySQL: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def main():
    while True:
        batch_process()
        # Sleep for 1 hour
        time.sleep(3600)

if __name__ == "__main__":
    main()