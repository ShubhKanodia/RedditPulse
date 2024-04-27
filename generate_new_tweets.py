import csv
from faker import Faker
import random

fake = Faker()

# List of topics
topics = ['batting', 'bowling', 'fielding']

# Potential hashtags for tweets
hashtags_list = ['#cricket', '#IPL', '#sports', '#gameDay', '#play', '#team', '#win', '#match']

# Word lists for each topic and sentiment
batting_words = {
    'positive': ['brilliant', 'match-winning', 'sensational', 'impressive', 'remarkable', 'majestic', 'stroke-filled', 'blistering'],
    'negative': ['disappointing', 'dismal', 'collapse', 'struggle', 'failure', 'poor', 'lackluster', 'underwhelming'],
    'neutral': ['innings', 'runs', 'scored', 'partnership', 'strike rate', 'opener', 'top-order', 'middle-order']
}

bowling_words = {
    'positive': ['lethal', 'unplayable', 'fiery', 'hostile', 'devastating', 'match-winning', 'sensational', 'masterclass'],
    'negative': ['wayward', 'erratic', 'ineffective', 'loose', 'struggling', 'costly', 'ordinary', 'expensive'],
    'neutral': ['overs', 'wickets', 'economy', 'spell', 'line and length', 'new ball', 'death overs', 'spearhead']
}

fielding_words = {
    'positive': ['sensational', 'acrobatic', 'athletic', 'stunning', 'breathtaking', 'game-changing', 'electric', 'brilliant'],
    'negative': ['sloppy', 'shoddy', 'error-prone', 'costly', 'dropped catches', 'misfields', 'sloppy', 'sloppy'],
    'neutral': ['catch', 'run-out', 'throw', 'diving', 'boundary', 'outfield', 'slip cordon', 'ground fielding']
}

# Generate tweets
num_tweets = 1000
tweets = []

for _ in range(num_tweets):
    topic = random.choice(topics)
    sentiment = random.choice(['positive', 'negative', 'neutral'])
    
    if topic == 'batting':
        word_list = batting_words[sentiment]
    elif topic == 'bowling':
        word_list = bowling_words[sentiment]
    else:
        word_list = fielding_words[sentiment]
    
    tweet_text = fake.sentence(nb_words=6, variable_nb_words=True, ext_word_list=word_list)
    if sentiment == 'positive':
        tweet_text = f"{topic.capitalize()} was {tweet_text}. "
    elif sentiment == 'negative':
        tweet_text = f"{topic.capitalize()} was {tweet_text}. "
    else:
        tweet_text = f"{tweet_text} in {topic}. "
    
    # Generate additional columns
    likes = random.randint(0, 1000)  # Random number of likes
    hashtags = random.sample(hashtags_list, k=random.randint(1, 3))  # Random selection of 1-3 hashtags
    timestamp = fake.date_time_between(start_date="-30d", end_date="now").isoformat()  # Random datetime within the last 30 days
    
    tweets.append([tweet_text, topic, sentiment, likes, ','.join(hashtags), timestamp])

# Write tweets to a CSV file
with open('tweets.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['text', 'topic', 'sentiment', 'likes', 'hashtags', 'timestamp'])  # Updated header row
    writer.writerows(tweets)
