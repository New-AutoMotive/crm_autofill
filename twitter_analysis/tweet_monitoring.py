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

    def get_all_tweets(self, key_words, since = ''):
        """
        Obtains all tweets sent by your selection of twitter handles that contain the key word(s) you are interested in.

        Only call this method once you have successfully called .get_handles().

        Raises UnboundLocalError if you haven't called .get_handles().

        :param key_words: Str, list of strings. The key words that you want to search on, e.g. 'electric vehicle', or ['electric vehicle', 'electric cars'], etc.

        :param since: Str. A starting date after which to search from, in the format YYYY-MM-DD, e.g. 2021-03-01. 

        :return:  Returns a DataFrame with tweets sent by the handles you pulled earlier containing the specified keywords. A copy of this DataFrame is saved as self.tweets.
        """

        # Firstly, we handle cases where a list of strings has been passed as the keywords parameter. This replaces ['search term 1', 'search term 1'] with ("search term 1" OR "search term 2")
        # This script handles each element in the list as a boolean search term, only returning tweets that contain that whole phrase. 

        if isinstance(key_words, list):
            lst_m = ['"'+x+'"' for x in key_words]
            new_str = ' OR '.join(lst_m)
            new_str = "("+new_str+")"
            key_words = new_str
        else:
            pass
        
        since = since

        def get_tweets(handle, key_words, since=''):
            tweets_list1 = []
            # Using TwitterSearchScraper to scrape data and append tweets to list
            for i, tweet in enumerate(
                    sntwitter.TwitterSearchScraper('{s} from:{h} since:{t}'.format(s=key_words, h=handle, t=since)).get_items()):
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
                dfs.append(get_tweets(h, key_words, since))
        except UnboundLocalError:
            print('Argh - UnboundLocalError! Did you call .get_handles first?')

        df = pd.concat(dfs)

        df.drop_duplicates(inplace=True)

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

    def from_csv(self, path='', index_col = 0):
        """
        Retreive a CSV saved with the save_csv() method, essentially just making use of pandas' read_csv() method. The DataFrame is returned, and saved as self.tweets.

        :param path: Str, default ''. The name/path to the file you wish to open.
        :param index_col: Int, default 0. The first column of a csv saved with the .save_csv() method saves the index in column 0.

        :return: A DataFrame of tweets.
        """


        self.tweets = pd.read_csv(path, index_col = index_col)
        df = pd.read_csv(path)
        return df

    def bq_upload_overwrite(self, table_name):
        """
        A method for uploading the tables you've obtained by calling .get_handles() and .get_all_tweets() to bigquery.

        :param table_name: String, the name of the table within the 'twitter' dataset.

        :return: None.
        """
        table_index = 'crmserver-id.twitter.{t}'.format(t=table_name)
        bqclient = bigquery.Client()
        # try:
        job_config = bigquery.job.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = bqclient.load_table_from_dataframe(self.tweets, table_index, job_config=job_config)
        job.result()
        print('Table {} successfully created'.format(table_name))
        # except:
        #     print('Did you run .get_handles and .get_all_tweets first?')

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