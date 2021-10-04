# Some useful functions
# from credentials import slack_token
import slack
import mysql.connector as connection
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

# Print a message to slack's civi-crm channel

def print_to_slack(message, slack_token):
    client = slack.WebClient(token = slack_token)
    client.chat_postMessage(channel = "civi-crm", text = message)

# Upload a dataframe to a civicrm database
def upload_to_civi(sql_password, table_name, slack_token, df, write_disposition):
    Base = declarative_base()
    engine = create_engine('mysql+pymysql://ben:{pw}@localhost:3306/wordpress'.format(pw=sql_password))
    cnx = engine.connect()
    try:
        frame = df.to_sql(table_name, cnx, if_exists=write_disposition, index=False)
    except ValueError as vx:
        print(vx)
        # done_msg = 'Uh oh, there was a ValueError while trying to upload updated constituency relationships. The details are here: \n {vx}'.format(vx=vx)
        # print_to_slack(message = done_msg, slack_token = slack_token)
    except Exception as ex:   
        print(ex)
        # done_msg = 'Uh oh, there was an Exception while trying to upload updated constituency relationships. The details are here: \n {vx}'.format(vx=ex)
        # print_to_slack(message = done_msg, slack_token = slack_token)
    else:
        print("Table {} updated successfully.".format(table_name))
        # done_msg = 'Just updated constituency relationships between individuals/organisations and constituencies. I updated/created {} new relationships!'.format(num_new_rows)
        # print_to_slack(message = done_msg, slack_token = slack_token)
    finally:
        cnx.close()
