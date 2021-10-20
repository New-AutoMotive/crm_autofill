# Some useful functions
# from credentials import slack_token
import slack
# import mysql.connector as connection
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

# Print a message to slack's civi-crm channel
def print_to_slack(message, slack_token, channel = 'civi-crm'):
    client = slack.WebClient(token = slack_token)
    client.chat_postMessage(channel = channel, text = message)

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


def sql_to_pandas(sql_pw, query):
    Base = declarative_base()
    engine = create_engine('mysql+pymysql://ben:{pw}@localhost:3306/wordpress'.format(pw=sql_pw))
    cnx = engine.connect()
    df = pd.read_sql(sql = query, con = cnx)
    return df

# this is a standard, out of the box function from google
from google.cloud import storage

def upload_blob(bucket_name, source_file_name, destination_blob_name, project_id):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project = project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

from google.cloud import bigquery

def csv_to_bq(table_id, uri):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = table_id

    # Set the encryption key to use for the destination.
    # TODO: Replace this key with a key you have created in KMS.
    # kms_key_name = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
    #     "cloud-samples-tests", "us", "test", "test"
    # )
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    uri = uri
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))