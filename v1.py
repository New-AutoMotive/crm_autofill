import pandas as pd
import mysql.connector

# Keep us up to date...
print('Starting sync...')

# First, we generate a lookup dictionary pcd -> contact_id
cnx = mysql.connector.connect(user='ben', password='new_auto_2020', host='localhost', database='wordpress')

# Calculate the len of relationships database at beginning:
query = "SELECT COUNT(*) FROM civicrm_relationship"
cursor = cnx.cursor()
cursor.execute(query)

start_num = 0

for x in cursor:
    start_num += x[0]


cursor = cnx.cursor()
# This query selects just postal codes and contact ids from the address database. 
query = "SELECT postal_code, contact_id FROM `civicrm_address` WHERE postal_code NOT LIKE 'SW1%'"
cursor.execute(query)

print('SQL query executed successfully')
# We create an empty dictionary to receive the values
post_code_cid = {}

for x in cursor:
    # We must strip back postcodes so they have no whitespace, and are lower case. This facilitates better matching. 
    pcd = x[0].replace(' ', '').replace(' ', '').strip().lower()
    # Populate the dictionary
    post_code_cid[pcd] = x[1]

# We load the dictionary into a df
pcd_cid = pd.DataFrame.from_dict(post_code_cid, orient='index').reset_index()
pcd_cid.columns = ['pcd', 'cid']

print('Reading lookup...')

# Now we get a lookup pcd->constituency code
lookup = pd.read_csv('lookup.csv')

# Now we do an inner join of the dataframe
jdf = pd.merge(pcd_cid, lookup, how='inner', on='pcd')

# print(jdf.head())

# Get the ONS_code - to contact_id details
print('Getting ONS codes')
query = "SELECT entity_id, ons_code_7 FROM `civicrm_value_ons_code_7`"
cursor = cnx.cursor()
cursor.execute(query)

pcon_id = {}

for x in cursor:
    pcon_id[x[1]] = x[0]

pcon_id_df = pd.DataFrame.from_dict(data = pcon_id, orient='index').reset_index()
pcon_id_df.columns = ['pcon', 'entity_id']

jdf = jdf.merge(pcon_id_df, how='left', left_on='pcon', right_on='pcon')
print('Finished merging databases...') 

# Create a list of values to use to populate the relationships
list_contact_as = jdf.cid.tolist()
list_contact_bs = jdf.entity_id.tolist()

# Create first bit of SQL query
query = "INSERT INTO civicrm_relationship (contact_id_a, contact_id_b, relationship_type_id, start_date, end_date, is_active, description, is_permission_a_b, is_permission_b_a, case_id) VALUES "

print('Constructing SQL query to write relationships...')
for i, e in enumerate(list_contact_as):
    print(i, e)
    query = query + "({con_id_a}, {con_id_b}, 16, NULL, NULL, 1, NULL, 0, 0, NULL),".format(con_id_a = list_contact_as[i], con_id_b = list_contact_bs[i])

query = query[:-1]
print('Executing query...')
cursor = cnx.cursor()
cursor.execute(query)
print('Finished, closing connection')
cursor.close()

# Calculate the len of relationships database at end:
query = "SELECT COUNT(*) FROM civicrm_relationship"
cursor = cnx.cursor()
cursor.execute(query)

end_num = 0

for x in cursor:
    end_num += x[0]

new_rows = end_num - start_num

print('Successfully created {} new relationship records'.format(new_rows))

# print(jdf.head())

