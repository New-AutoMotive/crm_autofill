import pandas as pd
import mysql.connector

cnx = mysql.connector.connect(user='ben', password='new_auto_2020', host='localhost', database='wordpress')
query = "SELECT entity_id, ons_code_7 FROM `civicrm_value_ons_code_7`"
df_a = pd.read_sql(query,cnx)
cnx.close()

df_b = pd.read_csv('address_files/lookup.csv')

df = pd.merge(df_a, df_b, how='inner', left_on='ons_code_7', right_on='pcon')

df = df[['pcd', 'entity_id']]

df.to_csv('address_files/lookup_pcd_constituency_id.csv')

print('Successfully created an updated lookup file which has postcodes and constituency entity ids. It is saved in address_files/lookup_pcd_constituency_id.csv')