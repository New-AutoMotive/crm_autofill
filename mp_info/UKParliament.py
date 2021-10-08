import requests
import json
import mysql.connector as connection
from sqlalchemy import create_engine
import pandas as pd
import glob
from tqdm import tqdm

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

            # From the response, we obtain the nested data, which is sorted into three kinds of jobs
            govt_jobs = pd.DataFrame(data['value']['governmentPosts'])
            oppo_jobs = pd.DataFrame(data['value']['oppositionPosts'])
            cttee_jobs = pd.DataFrame(data['value']['committeeMemberships'])

            # The committee jobs dataframe needs some tidying to distinguish between membership and chairmanship of the committee. 
            # Add 'Member of' in front of committee jobs
            cttee_jobs['name'] = cttee_jobs.name.apply(lambda x: 'Member of ' + x if 'Committee' in x else x)

            # Replace 'Member of' with 'Chair of' if additionalInfo column indicates that they were a chair of the committee
            cttee_jobs['name'] = cttee_jobs.apply(lambda row: row['name'].replace('Member of ', 'Chair of ') if row['additionalInfo'] == 'Chaired' else row['name'], axis=1)

            # Now we concatenate the three dataframes into one big one. They all have the same columns, so this is easy. 
            jobs_df = pd.concat([govt_jobs, oppo_jobs, cttee_jobs])
            # Make a column that specifies the MPs' Parliament API number so we have some reference back to the MP
            jobs_df['mp_id'] = api_number
            # Now we merge them with self.mps, which has API number + contact_id number from CiviCRM
            jobs_df = jobs_df.merge(self.mps, how='inner', left_on='mp_id', right_on='parliament_api_number_10')
            # Drop a few unecessary columns - we need to start making this DataFrame look like our target SQL table.
            jobs_df.drop(columns = ['id_x', 'id_y', 'parliament_api_number_10', 'house', 'mp_id', 'additionalInfoLink'], inplace=True, errors='ignore')
            # In this try and except clause, we try to obtain the necessary columns for our target table. Of course, it's possible that MPs  have no job info, if they've never had a parliamentary job. 
            # In these cases, there will be an error, and we simple pass and return no data, since there's no job info to be found. That's why the except passes on a 'keyerror'. 
            # 
            try:
                jobs_df = jobs_df[['entity_id', 'name', 'startDate', 'endDate', 'additionalInfo']]
                jobs_df.columns = ['entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']
                jobs_df.drop_duplicates(inplace=True)
                # This provisions some functionality around an id column. Not useful when doing mass scrapes. 
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

    def get_party(self, api_number):
        """
        A function to get the party of a particular mp. 

        :return: a DataFrame with columns...
        """
        url = 'https://members-api.parliament.uk/api/Members/{mp}'.format(mp=api_number)
        headers = {
        'accept': 'text/plain',
        }
        response = requests.get(url, headers=headers)
        if response.stat_code == 200:
            data = response.json()
            data['value']['latestParty']['mpId'] = api_number
            df = pd.DataFrame(data['value']['latestParty'], index=[0])[['id', 'name', 'abbreviation', 'mpId']]
            return df
        else:
            pass


def update_mp_job_info(sql_pw, path_to_tmp_folder):
    """
    This function returns a DataFrame containing all MP job information."""

    # Instantiate UK Parliament class above. 
    pp = UKParliament(sql_password = sql_pw)

    mps_list = pp.mps.parliament_api_number_10.tolist()
    print('Got preliminary info')
    for mp in tqdm(mps_list):
        try:
            df = pp.get_job_history(mp)
            df.to_csv(path_to_tmp_folder+'mp_{}.csv'.format(mp))
        except:
            print('excepted')
            pass
        
    df = pd.concat([pd.read_csv(file, index_col=0) for file in glob.glob(path_to_tmp_folder+'mp_*.csv')], ignore_index=True)
    df.drop_duplicates(inplace=True)
    df['id'] = df.index + 1
    df = df[['id', 'entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']]
    return df