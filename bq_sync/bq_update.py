import pandas as pd
import mysql.connector as connection
from google.cloud import bigquery
import sys
sys.path.append("/home/ben/crm_autofill/crm_autofill/")
from credentials import my_sql_password

# Create BQ Table Copy of civicrm_value_social_media_8
db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)
query = "SELECT * FROM `civicrm_value_social_media_8`;"
civi_thandles = pd.read_sql(query,db)

bqclient = bigquery.Client()
table_index='crmserver-id.twitter.twitter_handles'
job_config = bigquery.job.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job = bqclient.load_table_from_dataframe(civi_thandles, table_index, job_config = job_config)
job.result()

# Obtain a join of some civicrm_contact details and social media handles
query = "SELECT A.id, A.contact_type, A.contact_sub_type, A.display_name, B.twitter_handle_9 FROM `civicrm_contact` A INNER JOIN `civicrm_value_social_media_8` B ON A.id = B.entity_id INNER JOIN `civicrm_entity_tag` C"
civi_contacts_social = pd.read_sql(query,db)

# Upload to BigQuery
table_index='crmserver-id.twitter.contact_info_twitter'
job_config = bigquery.job.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job = bqclient.load_table_from_dataframe(civi_contacts_social, table_index, job_config = job_config)
job.result()

# Obtain a join of tag info and tag_id numbers
query = "SELECT A.tag_id, B.name, B.description FROM `civicrm_entity_tag` A INNER JOIN `civicrm_tag` B ON A.tag_id = B.id"
civi_tag_info = pd.read_sql(query,db)

# Upload to BigQuery
table_index='crmserver-id.twitter.tag_info'
job_config = bigquery.job.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job = bqclient.load_table_from_dataframe(civi_tag_info, table_index, job_config = job_config)
job.result()