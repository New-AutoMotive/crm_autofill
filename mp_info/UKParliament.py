import requests
import json
import ..credentials.credentials.my_sql_password
import mysql.connector as connection
import pandas as pd

print(my_sql_password)

class UKParliament:
    def __init__(self):
        self.db = connection.connect(user='ben', password=my_sql_password, host='localhost', database='wordpress', use_pure=True)
        query = "SELECT * FROM `civicrm_value_uk_parliament_9` WHERE entity_id IN (SELECT id FROM `civicrm_contact` WHERE contact_sub_type = 'Member_of_UK_Parliament' AND is_deleted = 0);"
        self.mps = pd.read_sql(query,self.db)
        pass
