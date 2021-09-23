import snscrape.modules.twitter as sntwitter
import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

key_words = ['electric vehicles', 'electric vehicle', 'electric car', 'range anxiety', 'charge point' 'EV charging']

class bqTweetTools:
    """
    A toolkit for using our database of twitter handles to obtain tweets, and perform some simple analyses.
    """

    def __init__(self):
        # This attribute will contain twitter handles obtained from the bq database.
        self.handles = None
        # This attribute will be a DataFrame containing tweets.
        self.tweets = None
        pass

    def get_handles(self, limit=10000):
        """
        This method obtains a list of twitter handles stored in our CiviCRM database. It uses table crmserver-id.twitter.twitter_handles. Before you can call this method, you need to set Google Application
        Credentials as an environment variable and set project_id to be crmserver-id.

        The best way to authenticate is to run `export GOOGLE_APPLICATION_CREDENTIALS='path/to/json/file' after having generated and downloaded a keyfile.

        Find out more about authenticating with Google here: https://cloud.google.com/docs/authentication

        To change your project-id to crmserver-id, run `gcloud config set project crmserver-id`.

        :param limit: Integer, optional, default is 10,000. Limits the number of twitter handles to pull from the database. Useful for testing code on small samples of twitter handles, which runs quicker than the whole dataset.

        :return: Returns a DataFrame of twitter handles, usernames, and CiviCRM unique contact ids, a copy of which is stored as a DataFrame class attribute, self.handles.
        """
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
        """
        Obtains all tweets sent by your selection of twitter handles that contain the key word(s) you are interested in.

        Only call this method once you have successfully called .get_handles().

        Raises UnboundLocalError if you haven't called .get_handles().

        :param key_words: Str, list of strings. The key words that you want to search on, e.g. 'electric vehicle', or ['electric vehicle', 'electric cars'], etc.

        :return:  Returns a DataFrame with tweets sent by the handles you pulled earlier containing the specified keywords. A copy of this DataFrame is saved as self.tweets.
        """
        key_words = key_words

        def get_tweets(handle, key_words):
            tweets_list1 = []
            # Using TwitterSearchScraper to scrape data and append tweets to list
            for i, tweet in enumerate(
                    sntwitter.TwitterSearchScraper('{s} from:{h}'.format(s=key_words, h=handle)).get_items()):
                if i > 300:
                    break
                tweets_list1.append([tweet.date, tweet.id, tweet.content, tweet.user.username])

            # Creating a dataframe from the tweets list above
            tweets_df1 = pd.DataFrame(tweets_list1, columns=['datetime', 'tweetID', 'text', 'userName'])
            tweets_df1['text'] = tweets_df1.text.apply(lambda x: x.replace('&amp;', '&'))
            return tweets_df1


        try:
            list_of_handles = self.handles.twitter_handle_9.tolist()
        except:
            print("No handles, call .get_handles() to obtain twitter handles.")
        dfs = []

        print('Getting tweets!')

        try:
            for h in tqdm(list_of_handles):
                if isinstance(key_words, list):
                    for word in key_words:
                        dfs.append(get_tweets(h, word))
                else:
                    dfs.append(get_tweets(h, key_words))
        except UnboundLocalError:
            print('Argh - UnboundLocalError! Did you call .get_handles first?')

        df = pd.concat(dfs)

        self.tweets = df.copy()
        return df

    def save_csv(self, path=''):
        """
        Save your latest DataFrame to a csv, using Pandas' .to_csv() method.

        :param path: Str, specifying the name + path of the file you'd like to save.

        :return: None.
        """
        try:
            self.tweets.to_csv(path)
        except:
            print('No tweets! Call .get_handles, then .get_all_tweets to get some tweets!')

    def bq_upload_overwrite(self, table_name):
        """
        A method for uploading the tables you've obtained by calling .get_handles() and .get_all_tweets() to bigquery.

        :param table_name: String, the name of the table within the 'twitter' dataset.

        :return: None.
        """
        table_index = 'crmserver-id.twitter{t}'.format(t=table_name)
        bqclient = bigquery.Client()
        try:
            job_config = bigquery.job.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job = bqclient.load_table_from_dataframe(self.tweets, table_index, job_config=job_config)
            job.result()
            print('Table {} successfully created'.format(table_name))
        except:
            print('Did you run .get_handles and .get_all_tweets first?')

    def get_tweets_from_bq(self, limit=10000, table_name='all_contacts_ev_tweets'):
        """
        If you already have a bigquery table with tweets in it, don't bother downloading them again - it's lengthy as anything! Pull the tweets from the table instead by calling this method :)
        NB you must already be authenticated with Google Cloud, and have selected crmserver-id as your project-id.

        The best way to authenticate is to run `export GOOGLE_APPLICATION_CREDENTIALS='path/to/json/file' after having generated and downloaded a keyfile.

        Find out more about authenticating with Google here: https://cloud.google.com/docs/authentication

        To change your project-id to crmserver-id, run `gcloud config set project crmserver-id`.

        :param limit: Int, a cap on the number of tweets to pull. Default = 10,000.

        :param table_name: Specify the bigquery table name within the twitter dataset. The default is the all_contacts_ev_tweets table that contains tweets from all contacts in our db.

        :return: Returns a DataFrame, a copy of which is also saved to the self.tweets attribute.
        """
        bqclient = bigquery.Client()

        query_string = """
         SELECT *
         FROM crmserver-id.twitter.{tn}
         LIMIT {l}
         """.format(l=limit, tn=table_name)

        df = (
            bqclient.query(query_string)
                .result()
                .to_dataframe()
        )

        self.tweets = df.copy()
        return df

    def wordcloud(self):
        """
        Who doesn't enjoy a wordcloud? Builds it from the self.tweets DataFrame you created using other methods.

        :return: Nothing, but it'll show you a pretty cloud of words.
        """
        stopwords = set(STOPWORDS)
        stopwords.add('https')
        stopwords.add('t')
        stopwords.add('co')
        stopwords.add('amp')
        wordcloud = WordCloud(background_color='white', stopwords = stopwords, random_state=2016).generate(" ".join([i for i in self.tweets.text.str.lower()]))
        plt.imshow(wordcloud)
        plt.axis('off')
        plt.title('Electric Vehicles Tweets Wordcloud')
        plt.show()