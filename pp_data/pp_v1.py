import pandas as pd
import numpy as np
import mysql.connector as connection

import sys
sys.path.append("/home/ben/crm_autofill/crm_autofill/")

import slack

from credentials import slack_token, my_sql_password
from helpers import print_to_slack


# Connect to the MySQL database
db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)

# Obtain 
query = "SELECT * FROM `civicrm_contact` WHERE contact_sub_type = 'Member_of_UK_Parliament' AND suffix_id = 9 AND last_name = 'Dunne';"
mps = pd.read_sql(query,db)

print(mps)

# first_names = mps.first_name.tolist()
# surnames = mps.last_name.tolist()

# url_element = []

# for i in range(len(first_names)):
#     new_element = first_names[i]+'-'+surnames[i]
#     new_element = new_element.replace(' ', '-').replace(' ', '-').replace(' ', '-').replace(' ', '-')
#     url_element.append(new_element)

# print(url_element)

