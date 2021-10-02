from bs4 import BeautifulSoup
import requests

class ParallelParliament():
    def __init__(self):
        self.url = 'https://www.parallelparliament.co.uk/mp/'

    def get_mp(self, mp):
        """
        This function obtains the HTML 'soup' for any particular MP.
        """
        self.mp = mp
        self.url = 'https://www.parallelparliament.co.uk/mp/{mp}'.format(mp=self.mp)
        self.r = requests.get(self.url)
        self.soup = BeautifulSoup(self.r.content, 'html.parser')


    def get_mp_twitter(self, mp):
        """
        This function refreshes the HTML soup for a given MP and then returns the twitter pofile. 
        """


        self.get_mp(mp)

        # Get twitter profile
        try:
            self.twitter = self.soup.find(attrs={'class': 'col-sm-12 text-center mt-0'}).contents[7]['href']
        except:
            self.twitter = 'Not Found'
        return self.twitter


def update_mp_twitter():
    pp = ParallelParliament()

    msg = 'Updating MP twitter account details...'
    print_to_slack(message = msg, slack_token = slack_token)

    # Connect to the MySQL database
    db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)

    # Obtain dataframe with all first and last-name combinations for MPs. 
    query = "SELECT id, first_name, last_name FROM `civicrm_contact` WHERE contact_sub_type = 'Member_of_UK_Parliament' AND suffix_id = 9 AND is_deleted = 0;"
    mps = pd.read_sql(query,db)
    
    # Wrangle data to get twitter name
    mps['url_element'] = mps.progress_apply(lambda row: row.first_name+'-'+row.last_name, axis=1)
    mps['twitter'] = mps.progress_apply(lambda row: pp.get_mp_twitter(row['url_element']), axis=1)
    mps['twitter'] = mps.twitter.progress_apply(lambda x: x.split('.')[-1] if (x.split('.')[0] == 'https://twitter') else 'Not found')
    mps['twitter'] = mps.twitter.progress_apply(lambda x: x.split('/')[-1])
    mps = mps[mps.twitter != 'Not found']
    mps = mps[['id', 'twitter']]
    mps = mps.rename(columns={'id': 'entity_id', 'twitter': 'twitter_handle_9'})

    # List of current contacts' twitter accounts
    db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)
    query = "SELECT * FROM `civicrm_value_social_media_8`;"
    civi_thandles = pd.read_sql(query,db)

    # Outer join, indicating whether values are in a particular DataFrame
    mg = pd.merge(mps, civi_thandles, on='entity_id', how='outer', indicator=True)

    # Existing twitter handles
    mg_ex = mg[mg._merge == 'right_only'][['entity_id', 'twitter_handle_9_y']].rename(columns={'twitter_handle_9_y': 'twitter_handle_9'})
    mg_new = mg[mg._merge.isin(['both', 'left_only'])][['entity_id', 'twitter_handle_9_x']].rename(columns={'twitter_handle_9_x': 'twitter_handle_9'})

    num_new_rows = len(mg_new)

    # final is the new DataFrame, which will be uploaded to the SQL database
    final = pd.concat([mg_ex, mg_new], ignore_index=True)

    # Delete existing twitter handles
    db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)
    query = "DELETE FROM `civicrm_value_social_media_8`;"
    cursor = db.cursor()
    cursor.execute(query)
    db.commit()
    db.close()

    # Connect to the MySQL database
    engine = create_engine('mysql://ben:{pw}@localhost:3306/wordpress'.format(pw=my_sql_password))
    cnx = engine.connect()

    # Upload the new table
    try:
        frame = final.to_sql('civicrm_value_social_media_8', cnx, if_exists='append', index=False, )
    except ValueError as vx:
        print(vx)
        done_msg = 'Uh oh, there was a ValueError while trying to upload updated MP twitter details. The details are here: \n {vx}'.format(vx=vx)
        print_to_slack(message = done_msg, slack_token = slack_token)
    except Exception as ex:   
        print(ex)
        done_msg = 'Uh oh, there was an Exception while trying to upload updated MP twitter details. The details are here: \n {vx}'.format(vx=ex)
        print_to_slack(message = done_msg, slack_token = slack_token)
    else:
        print("Table {} updated successfully.".format('civicrm_value_social_media_8'))
        done_msg = 'FInished updating MP twitter details. I added {} new twitter handles!'.format(num_new_rows)
        print_to_slack(message = done_msg, slack_token = slack_token)
    finally:
        cnx.close()