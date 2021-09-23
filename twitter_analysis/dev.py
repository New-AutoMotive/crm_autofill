from tweet_monitoring import bqTweetTools, key_words
import pandas as pd

tt = bqTweetTools()

tt.get_handles(limit = 20)

tt.get_all_tweets(key_words = key_words)
tt.save_csv('tmp.csv')

df = pd.read_csv('tmp.csv')

print(df.shape)

print(df)