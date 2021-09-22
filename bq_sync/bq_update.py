import pandas as pd
import mysql.connector as connection
from google.cloud import bigquery
import sys
sys.path.append("/home/ben/crm_autofill/crm_autofill/")
from credentials import my_sql_password

# Create BQ Table Copy of 
db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)
query = "SELECT * FROM `civicrm_value_social_media_8`;"
civi_thandles = pd.read_sql(query,db)

bqclient = bigquery.Client()
table_index='crmserver-id.twitter.twitter_handles'
job_config = bigquery.job.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job = bqclient.load_table_from_dataframe(civi_thandles, table_index, job_config = job_config)
job.result()