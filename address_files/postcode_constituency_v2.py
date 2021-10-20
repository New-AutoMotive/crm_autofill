import pandas as pd
import numpy as np
import mysql.connector as connection
from sqlalchemy import create_engine
import slack
import os
# from credentials import slack_token, my_sql_password
# from helpers import print_to_slack
from datetime import datetime

# #################
# # Housekeeping! #
# #################

# ## Change the directory so that the script can be executed by CRON from the home directory.
# os.chdir('/home/ben/crm_autofill/crm_autofill/address_files')

# # # Connect to the slack bot to keep people informed about what's going on. 
# message = "DB Update Notification: About to update contacts' constituency information using information in the postcode field. I'll report back when it's done."
# client = slack.WebClient(token=slack_token)
# client.chat_postMessage(channel = "civi-crm", text = message)

# # Create today's date for when we need to end/start some relationships
# today = datetime.today().strftime('%Y-%m-%d')

# #################################
# # Obtain our required databases #
# #################################

# # Connect to the MySQL database
# db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)

# # Obtain civicrm_relationship to get existing and active 'constituent of' relationships
# query = "SELECT * FROM `civicrm_relationship` WHERE relationship_type_id=16 AND is_active=1 AND contact_id_a IN (SELECT contact_id FROM `civicrm_address` WHERE postal_code NOT LIKE 'SW1%' AND postal_code IS NOT NULL)"
# civicrm_relationship = pd.read_sql(query,db)

# # Obtain latest civicrm_addresses
# query = "SELECT postal_code, contact_id FROM `civicrm_address` WHERE postal_code NOT LIKE 'SW1%' AND postal_code IS NOT NULL;"
# civicrm_address_sel = pd.read_sql(query, db)

# # Obtain list of contact ids
# query = "SELECT id FROM `civicrm_contact`"
# contact_ids = pd.read_sql(query, db)

# # Obtain civicrm_address_sel_old, the version from last time
# civicrm_address_sel_old = pd.read_csv('/home/ben/crm_autofill/crm_autofill/address_files/last_civicrm_addresses.csv', index_col=0)

# # UNCOMMENT AFTER FIRST RUN!
# civicrm_address_sel = civicrm_address_sel[~ civicrm_address_sel.contact_id.isin(civicrm_address_sel_old.contact_id.tolist())]

# # Import postcode - constituency contact id lookup
# lookup = pd.read_csv('/home/ben/crm_autofill/crm_autofill/address_files/lookup_pcd_constituency_id.csv', index_col=0)
# lookup = lookup.astype({'entity_id': 'int64'})

# # Close the connection to the server
# db.close() 

# ##########################################################
# # Going from civicrm_address_sel to civicrm_relationship #
# ##########################################################

# # We start with a table that looks like this (civicrm_address_sel):
# #   postal_code  contact_id
# # 0    TN14 6LX           1
# # 1    TN14 6LX           5
# # 2    TN14 6LX           6
# # 3      SW22BP           4
# # 4     NE1 6PA          23
# # 
# # Our target table is something like this (civicrm_relationship):
# #      id  contact_id_a  contact_id_b  relationship_type_id start_date end_date  is_active description  is_permission_a_b  is_permission_b_a case_id
# # 0  4005            10           622                    16       None     None          1                              0                  0    None
# # 1  4282             4           983                    16       None     None          1                              0                  0    None
# # 
# # Steps are:
# # 1. Go from postal_code to contact_id_b
# # 2. Reform into the target DataFrame
# # 3. Check for existing entries in the target DataFrame that have the same contact_id_a field as our new data
# # 4. In cases where there are existing entries, if the contact_id_b field is different, end that relationship, and commence a new relationship

# # Step 1
# # First, we need to match stripped and lower case postcodes in our civicrm_address_sel table
# civicrm_address_sel['postal_code'] = civicrm_address_sel.postal_code.apply(lambda x: x.strip().replace(' ', '').replace(' ', '').lower())
# # Now we do a merge on the lookup and civicrm_address_sel
# civicrm_relationship_new = pd.merge(civicrm_address_sel, lookup, how='left', left_on='postal_code', right_on='pcd')
# civicrm_relationship_new.dropna(inplace=True)

# # Step 2
# # Now we reform the DataFrame into something that starts to resemble our target. 
# civicrm_relationship_new = civicrm_relationship_new.astype({'entity_id': 'int64'})
# civicrm_relationship_new.rename(columns={'contact_id': 'contact_id_a', 'entity_id': 'contact_id_b'}, inplace=True)
# civicrm_relationship_new.drop(columns=['postal_code', 'pcd'], inplace=True)

# # Step 3
# # Now we have a DataFrame with contact_id_a, and contact_id_b. We need to do some comparisons. 
# # First, let's check to see which relationships need updating. We'll check for records where contact_id_a occurs in the existing DataFrame. 
# civicrm_relationship_update = civicrm_relationship_new.merge(civicrm_relationship, how='left', on='contact_id_a', indicator=True)
# # Find records where the contact_id_a appears in both the new and old tables.
# civicrm_relationship_update = civicrm_relationship_update[civicrm_relationship_update._merge == 'both']
# # Now filter so that we get rid of records where the constituencies are the same in both new and old tables (i.e. no change of constituency)
# civicrm_relationship_update = civicrm_relationship_update[civicrm_relationship_update.contact_id_b_x != civicrm_relationship_update.contact_id_b_y]
# # That leaves civicrm_relationship_update as a DataFrame that only contains records that need to be updated. 
# # contact_id_b_y is the old constituency
# # contact_id_b_x is the new constituency

# # We close off those relationships by creating a DataFrame
# civicrm_relationship_deactivate = civicrm_relationship_update[['id', 'contact_id_a', 'contact_id_b_y', 'relationship_type_id', 'start_date', 'end_date', 'is_active', 'description', 'is_permission_a_b', 'is_permission_b_a', 'case_id']]
# civicrm_relationship_deactivate.loc[:, 'is_active'] = 0
# civicrm_relationship_deactivate.loc[:, 'end_date'] = today
# civicrm_relationship_deactivate.rename(columns = {'contact_id_b_y': 'contact_id_b'}, inplace=True)

# # We open up relationships that are new
# civicrm_relationship_activate = civicrm_relationship_update[['id', 'contact_id_a', 'contact_id_b_x', 'relationship_type_id', 'start_date', 'end_date', 'is_active', 'description', 'is_permission_a_b', 'is_permission_b_a', 'case_id']]
# civicrm_relationship_activate.loc[:, 'is_active'] = 1
# civicrm_relationship_activate.loc[:, 'start_date'] = today
# civicrm_relationship_activate.rename(columns = {'contact_id_b_x': 'contact_id_b'}, inplace=True)

# # Now we get the brand new relationships (i.e. where there wasn't a constituent of relationship beforehand)
# civicrm_relationship_new = civicrm_relationship_new.merge(civicrm_relationship, how='left', on='contact_id_a', indicator=True)
# civicrm_relationship_new = civicrm_relationship_new[civicrm_relationship_new._merge == 'left_only']
# civicrm_relationship_new.drop(columns=['_merge', 'contact_id_b_y'], inplace=True)
# civicrm_relationship_new.rename(columns={'contact_id_b_x': 'contact_id_b'}, inplace=True)

# civicrm_relationship_new.loc[:, 'start_date'] = today
# civicrm_relationship_new.loc[:, 'is_active'] = 1

# civicrm_relationship_update = pd.concat([civicrm_relationship_activate, civicrm_relationship_deactivate, civicrm_relationship_new])

# # Assign all updated constituent relationships the correct id. 
# civicrm_relationship_update.loc[:, 'relationship_type_id'] = 16

# # Conform a few numerical types - this is likely unnecessary. 
# civicrm_relationship_update = civicrm_relationship_update.astype({'contact_id_b': 'int64', 'relationship_type_id': 'int64'})

# # Create the final new database of constituency relations to update
# civicrm_relationship_to_upload = pd.concat([civicrm_relationship_update])
# civicrm_relationship_to_upload.loc[:, 'case_id'] = np.nan
# civicrm_relationship_to_upload.drop(columns='id', inplace=True)
# civicrm_relationship_to_upload.loc[:, 'is_permission_a_b'] = 0
# civicrm_relationship_to_upload.loc[:, 'is_permission_b_a'] = 0

# num_new_rows = len(civicrm_relationship_to_upload)

# ###########################
# # Update the new database #
# ###########################

# # Delete all constituency relationships
# db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)
# query = "DELETE FROM `civicrm_relationship` WHERE relationship_type_id=16 AND is_active=1 AND contact_id_a IN (SELECT contact_id FROM `civicrm_address` WHERE postal_code NOT LIKE 'SW1%' AND postal_code IS NOT NULL);"
# cursor = db.cursor()
# cursor.execute(query)
# db.commit()
# db.close()

# # Connect to the MySQL database
# engine = create_engine('mysql://ben:{pw}@localhost:3306/wordpress'.format(pw=my_sql_password))
# cnx = engine.connect()

# # Upload the constituency table
# try:
#     frame = civicrm_relationship_to_upload.to_sql('civicrm_relationship', cnx, if_exists='append', index=False)
# except ValueError as vx:
#     print(vx)
#     done_msg = 'Uh oh, there was a ValueError while trying to upload updated constituency relationships. The details are here: \n {vx}'.format(vx=vx)
#     print_to_slack(message = done_msg, slack_token = slack_token)
# except Exception as ex:   
#     print(ex)
#     done_msg = 'Uh oh, there was an Exception while trying to upload updated constituency relationships. The details are here: \n {vx}'.format(vx=ex)
#     print_to_slack(message = done_msg, slack_token = slack_token)
# else:
#     print("Table {} updated successfully.".format('civicrm_relationship'))
#     done_msg = 'Just updated constituency relationships between individuals/organisations and constituencies. I updated/created {} new relationships!'.format(num_new_rows)
#     print_to_slack(message = done_msg, slack_token = slack_token)
# finally:
#     cnx.close()


# # Finally, save this latest version of civicrm_addresses_sel to a csv so that we can check against it in the future
# civicrm_address_sel.to_csv('address_files/last_civicrm_addresses.csv')




def update_constituency(slack_t, sql_pw, slack_func):
    #################
    # Housekeeping! #
    #################

    ## Change the directory so that the script can be executed by CRON from the home directory.
    os.chdir('/home/ben/crm_autofill/crm_autofill/address_files')

    # # Connect to the slack bot to keep people informed about what's going on. 
    message = "DB Update Notification: About to update contacts' constituency information using information in the postcode field. I'll report back when it's done."
    client = slack.WebClient(token=slack_t)
    client.chat_postMessage(channel = "civi-crm", text = message)

    # Create today's date for when we need to end/start some relationships
    today = datetime.today().strftime('%Y-%m-%d')

    #################################
    # Obtain our required databases #
    #################################

    # Connect to the MySQL database
    db = connection.connect(user='ben', password=sql_pw, host='localhost', database='wordpress', use_pure=True)

    # Obtain civicrm_relationship to get existing and active 'constituent of' relationships
    query = "SELECT * FROM `civicrm_relationship` WHERE relationship_type_id=16 AND is_active=1 AND contact_id_a IN (SELECT contact_id FROM `civicrm_address` WHERE postal_code NOT LIKE 'SW1%' AND postal_code IS NOT NULL)"
    civicrm_relationship = pd.read_sql(query,db)

    # Obtain latest civicrm_addresses
    query = "SELECT postal_code, contact_id FROM `civicrm_address` WHERE postal_code NOT LIKE 'SW1%' AND postal_code IS NOT NULL;"
    civicrm_address_sel = pd.read_sql(query, db)

    # Obtain list of contact ids
    query = "SELECT id FROM `civicrm_contact`"
    contact_ids = pd.read_sql(query, db)

    # Obtain civicrm_address_sel_old, the version from last time
    try:
        civicrm_address_sel_old = pd.read_csv('/home/ben/crm_autofill/crm_autofill/address_files/last_civicrm_addresses.csv', index_col=0)
        # UNCOMMENT AFTER FIRST RUN!
        civicrm_address_sel = civicrm_address_sel[~ civicrm_address_sel.contact_id.isin(civicrm_address_sel_old.contact_id.tolist())]
    except FileNotFoundError:
        pass

    # Import postcode - constituency contact id lookup
    lookup = pd.read_csv('/home/ben/crm_autofill/crm_autofill/address_files/lookup_pcd_constituency_id.csv', index_col=0)
    lookup = lookup.astype({'entity_id': 'int64'})

    # Close the connection to the server
    db.close() 

    ##########################################################
    # Going from civicrm_address_sel to civicrm_relationship #
    ##########################################################

    # We start with a table that looks like this (civicrm_address_sel):
    #   postal_code  contact_id
    # 0    TN14 6LX           1
    # 1    TN14 6LX           5
    # 2    TN14 6LX           6
    # 3      SW22BP           4
    # 4     NE1 6PA          23
    # 
    # Our target table is something like this (civicrm_relationship):
    #      id  contact_id_a  contact_id_b  relationship_type_id start_date end_date  is_active description  is_permission_a_b  is_permission_b_a case_id
    # 0  4005            10           622                    16       None     None          1                              0                  0    None
    # 1  4282             4           983                    16       None     None          1                              0                  0    None
    # 
    # Steps are:
    # 1. Go from postal_code to contact_id_b
    # 2. Reform into the target DataFrame
    # 3. Check for existing entries in the target DataFrame that have the same contact_id_a field as our new data
    # 4. In cases where there are existing entries, if the contact_id_b field is different, end that relationship, and commence a new relationship

    # Step 1
    # First, we need to match stripped and lower case postcodes in our civicrm_address_sel table
    civicrm_address_sel['postal_code'] = civicrm_address_sel.postal_code.apply(lambda x: x.strip().replace(' ', '').replace(' ', '').lower())
    # Now we do a merge on the lookup and civicrm_address_sel
    civicrm_relationship_new = pd.merge(civicrm_address_sel, lookup, how='left', left_on='postal_code', right_on='pcd')
    civicrm_relationship_new.dropna(inplace=True)

    # Step 2
    # Now we reform the DataFrame into something that starts to resemble our target. 
    civicrm_relationship_new = civicrm_relationship_new.astype({'entity_id': 'int64'})
    civicrm_relationship_new.rename(columns={'contact_id': 'contact_id_a', 'entity_id': 'contact_id_b'}, inplace=True)
    civicrm_relationship_new.drop(columns=['postal_code', 'pcd'], inplace=True)

    # Step 3
    # Now we have a DataFrame with contact_id_a, and contact_id_b. We need to do some comparisons. 
    # First, let's check to see which relationships need updating. We'll check for records where contact_id_a occurs in the existing DataFrame. 
    civicrm_relationship_update = civicrm_relationship_new.merge(civicrm_relationship, how='left', on='contact_id_a', indicator=True)
    # Find records where the contact_id_a appears in both the new and old tables.
    civicrm_relationship_update = civicrm_relationship_update[civicrm_relationship_update._merge == 'both']
    # Now filter so that we get rid of records where the constituencies are the same in both new and old tables (i.e. no change of constituency)
    civicrm_relationship_update = civicrm_relationship_update[civicrm_relationship_update.contact_id_b_x != civicrm_relationship_update.contact_id_b_y]
    # That leaves civicrm_relationship_update as a DataFrame that only contains records that need to be updated. 
    # contact_id_b_y is the old constituency
    # contact_id_b_x is the new constituency

    # We close off those relationships by creating a DataFrame
    civicrm_relationship_deactivate = civicrm_relationship_update[['id', 'contact_id_a', 'contact_id_b_y', 'relationship_type_id', 'start_date', 'end_date', 'is_active', 'description', 'is_permission_a_b', 'is_permission_b_a', 'case_id']]
    civicrm_relationship_deactivate.loc[:, 'is_active'] = 0
    civicrm_relationship_deactivate.loc[:, 'end_date'] = today
    civicrm_relationship_deactivate.rename(columns = {'contact_id_b_y': 'contact_id_b'}, inplace=True)

    # We open up relationships that are new
    civicrm_relationship_activate = civicrm_relationship_update[['id', 'contact_id_a', 'contact_id_b_x', 'relationship_type_id', 'start_date', 'end_date', 'is_active', 'description', 'is_permission_a_b', 'is_permission_b_a', 'case_id']]
    civicrm_relationship_activate.loc[:, 'is_active'] = 1
    civicrm_relationship_activate.loc[:, 'start_date'] = today
    civicrm_relationship_activate.rename(columns = {'contact_id_b_x': 'contact_id_b'}, inplace=True)

    # Now we get the brand new relationships (i.e. where there wasn't a constituent of relationship beforehand)
    civicrm_relationship_new = civicrm_relationship_new.merge(civicrm_relationship, how='left', on='contact_id_a', indicator=True)
    civicrm_relationship_new = civicrm_relationship_new[civicrm_relationship_new._merge == 'left_only']
    civicrm_relationship_new.drop(columns=['_merge', 'contact_id_b_y'], inplace=True)
    civicrm_relationship_new.rename(columns={'contact_id_b_x': 'contact_id_b'}, inplace=True)

    civicrm_relationship_new.loc[:, 'start_date'] = today
    civicrm_relationship_new.loc[:, 'is_active'] = 1

    civicrm_relationship_update = pd.concat([civicrm_relationship_activate, civicrm_relationship_deactivate, civicrm_relationship_new])

    # Assign all updated constituent relationships the correct id. 
    civicrm_relationship_update.loc[:, 'relationship_type_id'] = 16

    # Conform a few numerical types - this is likely unnecessary. 
    civicrm_relationship_update = civicrm_relationship_update.astype({'contact_id_b': 'int64', 'relationship_type_id': 'int64'})

    # Create the final new database of constituency relations to update
    civicrm_relationship_to_upload = pd.concat([civicrm_relationship_update])
    civicrm_relationship_to_upload.loc[:, 'case_id'] = np.nan
    civicrm_relationship_to_upload.drop(columns='id', inplace=True)
    civicrm_relationship_to_upload.loc[:, 'is_permission_a_b'] = 0
    civicrm_relationship_to_upload.loc[:, 'is_permission_b_a'] = 0

    num_new_rows = len(civicrm_relationship_to_upload)

    ###########################
    # Update the new database #
    ###########################

    # Delete all constituency relationships
    db = connection.connect(user='ben', password=sql_pw, host='localhost', database='wordpress', use_pure=True)
    query = "DELETE FROM `civicrm_relationship` WHERE relationship_type_id=16 AND is_active=1 AND contact_id_a IN (SELECT contact_id FROM `civicrm_address` WHERE postal_code NOT LIKE 'SW1%' AND postal_code IS NOT NULL);"
    cursor = db.cursor()
    cursor.execute(query)
    db.commit()
    db.close()

    # Connect to the MySQL database
    engine = create_engine('mysql+pymysql://ben:{pw}@localhost:3306/wordpress'.format(pw=sql_pw))
    cnx = engine.connect()


    # Upload the constituency table
    try:
        frame = civicrm_relationship_to_upload.to_sql('civicrm_relationship', cnx, if_exists='append', index=False)
    except ValueError as vx:
        print(vx)
        done_msg = 'Uh oh, there was a ValueError while trying to upload updated constituency relationships. The details are here: \n {vx}'.format(vx=vx)
        slack_func(message = done_msg, slack_token = slack_t)
    except Exception as ex:   
        print(ex)
        done_msg = 'Uh oh, there was an Exception while trying to upload updated constituency relationships. The details are here: \n {vx}'.format(vx=ex)
        slack_func(message = done_msg, slack_token = slack_t)
    else:
        print("Table {} updated successfully.".format('civicrm_relationship'))
        done_msg = 'Just updated constituency relationships between individuals/organisations and constituencies. I updated/created {} new relationships!'.format(num_new_rows)
        slack_func(message = done_msg, slack_token = slack_t)
    finally:
        cnx.close()


    # Finally, save this latest version of civicrm_addresses_sel to a csv so that we can check against it in the future
    civicrm_address_sel.to_csv('/home/ben/crm_autofill/crm_autofill/address_files/last_civicrm_addresses.csv', mode='a', header=False)