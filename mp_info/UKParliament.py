import requests
import json
import mysql.connector as connection
from sqlalchemy import create_engine
import pandas as pd
import glob
import tqdm

class UKParliament:
    """
    A Class to retrieve information about MPs from the UK Parliament's API. 
    """
    def __init__(self, sql_password):
        self.sql_password = sql_password
        self.db = connection.connect(user='ben', password=self.sql_password, host='localhost', database='wordpress', use_pure=True)
        query = "SELECT * FROM `civicrm_value_uk_parliament_9` WHERE entity_id IN (SELECT id FROM `civicrm_contact` WHERE contact_sub_type = 'Member_of_UK_Parliament' AND is_deleted = 0);"
        self.mps = pd.read_sql(query,self.db)


    def get_job_history(self, api_number, create_id_col = False):
        """
        Returns a DataFrame in the format of civicrm_value_mp_informatio_10.
        If the request is not successful, then nothing is returned. 

        :param api_number: Int, the unique identifier in Parliament's database for the MP or Lord in question. 

        :param create_id_col: bool, default False. If you want to create a separate id column with autoincremented integers starting at id=1, then set to True. 

        :return: a DataFrame if the request to Parliament's API is successful, else no return is made. 
        """
        headers = {
            'accept': 'text/plain',
        }
        response = requests.get('https://members-api.parliament.uk/api/Members/{n}/Biography'.format(n=api_number), headers=headers)
        if response.status_code == 200:
            data = response.json()

            jobs_df = pd.concat([pd.DataFrame(data['value']['governmentPosts']),
            pd.DataFrame(data['value']['oppositionPosts']),
            pd.DataFrame(data['value']['committeeMemberships'])])
            jobs_df['mp_id'] = api_number
            jobs_df = jobs_df.merge(self.mps, how='inner', left_on='mp_id', right_on='parliament_api_number_10')
            jobs_df.drop(columns = ['id_x', 'id_y', 'parliament_api_number_10', 'house', 'mp_id', 'additionalInfoLink'], inplace=True, errors='ignore')
            try:
                jobs_df = jobs_df[['entity_id', 'name', 'startDate', 'endDate', 'additionalInfo']]
                jobs_df.columns = ['entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']
                jobs_df.drop_duplicates(inplace=True)
                # Need to remove the below lines when doing a mass number of MPs
                if create_id_col:
                    jobs_df['id'] = jobs_df.index + 1
                    jobs_df = jobs_df[['id', 'entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']]
                else:
                    pass
                return jobs_df
            except KeyError:
                pass
        else:
            pass

def update_mp_job_info(sql_pw, path_to_tmp_folder):
    """
    This function returns a DataFrame containing all MP job information."""

    # Instantiate UK Parliament class above. 
    pp = UKParliament(sql_password = sql_pw)

    mps_list = pp.mps.parliament_api_number_10.tolist()
    for mp in tqdm(mps_list):
        try:
            pp.get_job_history(mp).to_csv(path_to_tmp_folder+'{}.csv'.format(mp))
        except:
            pass
        
    df = pd.concat([pd.read_csv(file, index_col=0) for file in glob.glob(path_to_tmp_folder+'/*.csv')], ignore_index=True)
    df['id'] = df.index + 1
    df = df[['id', 'entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']]

    return df