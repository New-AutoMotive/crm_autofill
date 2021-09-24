from tweet_monitoring import bqTweetTools, key_words
# import pandas as pd

# Instantiate a bqTweetTools
tt = bqTweetTools()
#
# # Pull all handles from the database
# tt.get_handles()
#
# # Search on the standard keywords
# tt.get_all_tweets(key_words = key_words)
#
# tt.save_csv('tmp.csv')

tt.from_csv('tmp.csv')
print(tt.tweets.shape)

tt.tweets.drop_duplicates(inplace=True)
print(tt.tweets.shape)
print(tt.tweets.head())

tt.bq_upload_overwrite(table_name = 'ac_wide_keywords_tweets')
