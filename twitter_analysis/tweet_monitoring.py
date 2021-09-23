import snscrape.modules.twitter as sntwitter
import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

key_words = ['electric vehicles', 'electric vehicle', 'electric car', 'range anxiety', 'charge point' 'EV charging']

class bqTweetTools:
    def __init__(self):
        # This attribute will contain twitter handles obtained from the bq database.
        self.handles = None
        # This attribute will be a DataFrame containing tweets.
        self.tweets = None
        pass

    def get_handles(self, limit=10000):
        bqclient = bigquery.Client()

        query_string = """
        SELECT *
        FROM crmserver-id.twitter.twitter_handles
        LIMIT {l}
        """.format(l = limit)

        df = (
            bqclient.query(query_string)
                .result()
                .to_dataframe()
        )

        print('DataFrame of handles downloaded...')
        self.handles = df.copy()
        return df

    def get_all_tweets(self, key_words):
        key_words = key_words

        def get_tweets(mp_handle, key_words):
            tweets_list1 = []
            # Using TwitterSearchScraper to scrape data and append tweets to list
            for i, tweet in enumerate(
                    sntwitter.TwitterSearchScraper('{s} from:{h}'.format(s=key_words, h=mp_handle)).get_items()):
                if i > 300:
                    break
                tweets_list1.append([tweet.date, tweet.id, tweet.content, tweet.user.username])

            # Creating a dataframe from the tweets list above
            tweets_df1 = pd.DataFrame(tweets_list1, columns=['datetime', 'tweetID', 'text', 'userName'])
            tweets_df1['text'] = tweets_df1.apply(lambda x: x.replace('&amp;', '&'))
            return tweets_df1


        try:
            list_of_handles = self.handles.twitter_handle_9.tolist()
        except:
            print("No handles, call .get_handles() to obtain twitter handles.")
        dfs = []

        print('Getting tweets!')

        try:
            for h in tqdm(list_of_handles):
                dfs.append(get_tweets(h, key_words))
        except UnboundLocalError:
            print('Argh - UnboundLocalError! Did you call .get_handles first?')

        df = pd.concat(dfs)

        self.tweets = df.copy()
        return df

    def save_csv(self, path=''):
        try:
            self.tweets.to_csv(path)
        except:
            print('No tweets! Call .get_handles, then .get_all_tweets to get some tweets!')

    def bq_upload_overwrite(self, table_name):
        table_index = 'crmserver-id.twitter.{t}'.format(table_name)
        bqclient = bigquery.Client()
        try:
            job_config = bigquery.job.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job = bqclient.load_table_from_dataframe(self.tweets, table_index, job_config=job_config)
            job.result()
            print('Table {} successfully created'.format(table_name))
        except:
            print('Did you run .get_handles and .get_all_tweets first?')

    def get_tweets_from_bq(self, limit=10000):
        bqclient = bigquery.Client()

        query_string = """
         SELECT *
         FROM crmserver-id.twitter.all_contacts_ev_tweets
         LIMIT {l}
         """.format(l=limit)

        df = (
            bqclient.query(query_string)
                .result()
                .to_dataframe()
        )

        self.tweets = df.copy()
        return df

    def wordcloud(self):
        stopwords = set(STOPWORDS)
        stopwords.add('https')
        stopwords.add('t')
        stopwords.add('co')
        stopwords.add('amp')
        wordcloud = WordCloud(background_color='white', stopwords = stopwords, random_state=2016, width = 32, height=24).generate(" ".join([i for i in self.tweets.text.str.lower()]))
        plt.imshow(wordcloud)
        plt.axis('off')
        plt.title('Electric Vehicles Tweets Wordcloud')
        plt.show()