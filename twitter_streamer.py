import tweepy
import json
import sys
from tweepy import OAuthHandler, API, Stream
from tweepy.streaming import StreamListener


# Twitter API credentials
CONSUMER_KEY = 'RXD9AK6TjK4eUt4wrR753l7Gm'
CONSUMER_SECRET = 'zFfjOkeNfTBomleMczzw54krUNu3SXtwOEeaiObUP8hQfpQEjP'
ACCESS_TOKEN = '1686094158534995969-6yhr5Q1vk003nhj7tTffq5zLKJhlog'
ACCESS_TOKEN_SECRET = 'f8l29NkvOwti5sikmEQJsRqhvm9gXA40fjirdaLdOrrRM'


#bearer token : AAAAAAAAAAAAAAAAAAAAAHOmtQEAAAAAei9LwCkf%2FAC50dm1TCqTi7ahk2k%3Doogh7ZukWmOex4DQNAH7eidlaTZmKJSmP5YemuzdRD3HiL9wLP
#client secret : u0U-_wR6w4vEepP2tuDjkjaeWTz8dGgE2bOn2-jBaFvTRTV4qb
# Authenticate to Twitter
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

class Listener(StreamListener):
    def __init__(self, output_file=sys.stdout):
        super(Listener, self).__init__()
        self.output_file = output_file

    def on_status(self, status):
        try:
            tweet_text = status.text
            likes = status.favorite_count
            hashtags = [hashtag['text'] for hashtag in status.entities['hashtags']]
            timestamp = status.created_at

            # Log tweet details to the console
            print(f"Text: {tweet_text}", file=self.output_file)
            print(f"Likes: {likes}", file=self.output_file)
            print(f"Hashtags: {', '.join(hashtags)}", file=self.output_file)
            print(f"Timestamp: {timestamp}", file=self.output_file)
            print('-' * 80, file=self.output_file)
        except Exception as e:
            print(f"Error: {e}")

    def on_error(self, status_code):
        print(status_code)
        return False

output = open('stream_output.txt', 'w')
listener = Listener(output_file=output)

stream = Stream(auth=api.auth, listener=listener)
try:
    print('Start streaming.')
    stream.filter(track=['batting', 'bowling', 'fielding'], languages=['en'])
except KeyboardInterrupt:
    print("Stopped.")
finally:
    print('Done.')
    stream.disconnect()
    output.close()