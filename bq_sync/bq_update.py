import pandas as pd
# import mysql.connector as connection
from sqlalchemy import create_engine, text
from google.cloud import bigquery
import sys

def connect_to_civi(sql_password):
    engine = create_engine('mysql+pymysql://ben:{pw}@localhost/wordpress'.format(pw=sql_password))
    return engine


# Open connections
def gbq_mysql_connect(sql_password):
    db = connection.connect(user='ben', password=sql_password, host='localhost', database='wordpress', use_pure=True)
    bqclient = bigquery.Client()
    print('connection opened!')
    return db, bqclient

# db, bqclient = gbq_mysql_connect()


#######

# Create BQ Table Copy of civicrm_value_social_media_8
# This is to create an up to date copy of the table that contains all our twitter handles

def update_bq_twitter_handles(db, bqclient):
    query = "SELECT * FROM `civicrm_value_social_media_8` WHERE twitter_handle_9 != '';"
    civi_thandles = pd.read_sql(query,db)
    civi_thandles['twitter_handle_9'] = civi_thandles.twitter_handle_9.apply(lambda x: x.replace(' ', '') if x is not None else x)

    table_index='crmserver-id.twitter.twitter_handles'
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = bqclient.load_table_from_dataframe(civi_thandles, table_index, job_config = job_config)
    job.result()

# update_bq_twitter_handles()

########

# Obtain a join of some civicrm_contact details and social media handles
# This allows us to tell what type of contact people are

def create_contact_info_twitter(db, bqclient):
    query = "SELECT A.id, A.contact_type, A.contact_sub_type, A.display_name, B.twitter_handle_9 FROM `civicrm_contact` A INNER JOIN `civicrm_value_social_media_8` B ON A.id = B.entity_id INNER JOIN `civicrm_entity_tag` C WHERE twitter_handle_9 != ''"
    civi_contacts_social = pd.read_sql(query,db)
    civi_contacts_social.drop_duplicates(inplace=True)
    civi_contacts_social['contact_sub_type'] = civi_contacts_social.contact_sub_type.apply(lambda x: x.replace(' ', '') if x is not None else x)
    civi_contacts_social['twitter_handle_9'] = civi_contacts_social.twitter_handle_9.apply(lambda x: x.replace(' ', '') if x is not None else x)

# Upload to BigQuery
    table_index='crmserver-id.twitter.contact_info_twitter'
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = bqclient.load_table_from_dataframe(civi_contacts_social, table_index, job_config = job_config)
    job.result()

# create_contact_info_twitter()

#########

# Obtain a join of tag info and tag_id numbers
def create_bq_tag_info_tag_id(db, bqclient):
    query = "SELECT A.tag_id, B.name, B.description FROM `civicrm_entity_tag` A INNER JOIN `civicrm_tag` B ON A.tag_id = B.id"
    civi_tag_info = pd.read_sql(query,db)

# Upload to BigQuery
    table_index='crmserver-id.twitter.tag_info'
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = bqclient.load_table_from_dataframe(civi_tag_info, table_index, job_config = job_config)
    job.result()

# create_bq_tag_info_tag_id()