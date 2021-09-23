from tweet_monitoring import bqTweetTools, key_words

tt = bqTweetTools()

tt.get_handles()

tt.get_all_tweets(key_words = key_words)


print(tt.tweets.head())