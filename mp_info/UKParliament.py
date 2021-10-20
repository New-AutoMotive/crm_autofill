import requests
import json
import mysql.connector as connection
# from sqlalchemy import create_engine
import pandas as pd
import glob
from tqdm import tqdm
import json
import os
import glob

# Out of the box function from Google to upload stuff to storage.
# from google.cloud import storage

# def upload_blob(bucket_name, source_file_name, destination_blob_name, project_id):
#     """Uploads a file to the bucket."""
#     # The ID of your GCS bucket
#     # bucket_name = "your-bucket-name"
#     # The path to your file to upload
#     # source_file_name = "local/path/to/file"
#     # The ID of your GCS object
#     # destination_blob_name = "storage-object-name"

#     storage_client = storage.Client(project = project_id)
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)

#     blob.upload_from_filename(source_file_name)

#     print(
#         "File {} uploaded to {}.".format(
#             source_file_name, destination_blob_name
#         )
#     )


class UKParliament:
    """
    A Class to retrieve information about MPs from the UK Parliament's API. 
    """
    def __init__(self, sql_password):
        self.sql_password = sql_password
        self.db = connection.connect(user='ben', password=self.sql_password, host='localhost', database='wordpress', use_pure=True)
        query = "SELECT * FROM `civicrm_value_uk_parliament_9` WHERE entity_id IN (SELECT id FROM `civicrm_contact` WHERE contact_sub_type = 'Member_of_UK_Parliament' AND is_deleted = 0);"
        self.mps = pd.read_sql(query, self.db)
        self.mps_ids = self.mps.parliament_api_number_10.tolist()
        mps_ids = self.mps.parliament_api_number_10.tolist()

    def get_all_parliamentarians(self, path_to_tmp):
        # First we determine which id numbers are real and which do not refer
        possible_numbers = [x for x in range(1, 5001)]
        actual_numbers = []
        # We iterate through the possible numbers and append those with response code 200 (success) to the actual numbers
        print('Getting new list of MP id numbers + info. This can take about 15 minutes...')

        active_members = []
        former_members = []

        for mp in tqdm(possible_numbers):
            url = 'https://members-api.parliament.uk/api/Members/{mp}'.format(mp=mp)
            headers = {'accept': 'text/plain'}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data['value']['latestHouseMembership']['membershipEndDate'] is None:
                    with open(path_to_tmp+'{}_active_info.json'.format(mp), 'w') as outfile:
                        json.dump(data['value'], outfile)
                    active_members.append(mp)
                else:
                    with open(path_to_tmp+'{}_former_info.json'.format(mp), 'w') as outfile:
                        json.dump(data['value'], outfile)
                    former_members.append(mp)
            else:
                pass




        # We'll combine the current members into one big bit of json
        combined_json = []
        combined_former_json = []

        for f in tqdm(glob.glob(path_to_tmp+'*_active_info.json')):
            with open(f, 'rb') as infile:
                combined_json.append(json.load(infile))
                
        for f in tqdm(glob.glob(path_to_tmp+'*_former_info.json')):
            with open(f, 'rb') as infile:
                combined_former_json.append(json.load(infile))

        with open(path_to_tmp+"active_members.json", "w") as outfile:
            json.dump(combined_json, outfile, indent=0)
            # for line in combined_json:
            #     outfile.write(line + "\n")

        # Pandas + CSV solution:
        active_members_df = pd.json_normalize(combined_json)
        # active_members.drop(columns='')
        active_members_df.to_csv(path_to_tmp+'active_members.csv', index=False)

        with open(path_to_tmp+'former_members.json', 'w') as outfile:
            json.dump(combined_former_json, outfile, indent=0)
            # for line in combined_former_json:
            #     outfile.write(line + "\n")

        # Pandas + CSV solution:
        former_members_df = pd.json_normalize(combined_former_json)
        former_members_df.to_csv(path_to_tmp+'former_members.csv', index=False)

        print('Uploading active members json to Google Cloud Storage...')
        upload_blob(bucket_name = 'civicrm_assets', source_file_name = path_to_tmp+'active_members.csv', destination_blob_name='Parliament/active_members.csv', project_id='crmserver-id')
        print('...done. \nNow uploading former members json to Google Cloud Storage...')
        upload_blob(bucket_name = 'civicrm_assets', source_file_name = path_to_tmp+'former_members.csv', destination_blob_name='Parliament/former_members.csv', project_id='crmserver-id')
        print('...done. Finished json upload.')
        
        # Get a list of all the file paths that ends with .txt from in specified directory
        fileList = glob.glob(path_to_tmp+'*_active_info.json')
        # Iterate over the list of filepaths & remove each file.
        print('Cleaning up... active_info.json files...')
        for filePath in tqdm(fileList):
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)

        # Get a list of all the file paths that ends with .txt from in specified directory
        fileList = glob.glob(path_to_tmp+'*_former_info.json')
        # Iterate over the list of filepaths & remove each file.
        print('Cleaning up... former_info.json files...')
        for filePath in tqdm(fileList):
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)
        print('All done! Be on your merry way.')
        return active_members_df, former_members_df

    def get_contact_info(self, path_to_active_members_json, path_to_tmp):
        with open(path_to_active_members_json, 'r') as f:
            data = json.load(f)
        # Get list of active member ids
        active_member_ids = []
        for e in data:
            active_member_ids.append(e['id'])

        # Combined json
        combined_json = []
        print('Downloading MP contact information...')
        # Now get contact info
        for mp in tqdm(active_member_ids):
            try:
                headers = {'accept': 'text/plain',}
                response = requests.get('https://members-api.parliament.uk/api/Members/{id}/Contact'.format(id=mp), headers=headers)
                data = response.json()['value']
                for d in data:
                    d['mp_id'] = mp
                combined_json.append(data)
            except:
                pass

        contact_info = pd.json_normalize(combined_json)
        contact_info.to_csv(path_to_tmp+'contact_info.csv')
        # with open(path_to_tmp+'contact_info.json', 'w') as f:
        #     json.dump(combined_json, f, indent=0)
            # for line in combined_json:
            #     f.write(line+"\n")

        print('Uploading contact details json to Google Cloud Storage...')
        upload_blob(bucket_name = 'civicrm_assets', source_file_name = path_to_tmp+'contact_info.csv', destination_blob_name='Parliament/contact_details.csv', project_id='crmserver-id')
        print('...done. Finished json upload.')


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

    def get_party(self):
        list_of_api_nos = self.mps_ids
        list_of_dfs = []
        """
        A function to get the party of a particular mp. 

        :return: a DataFrame with columns...
        """
        for mp in tqdm(list_of_api_nos):
            url = 'https://members-api.parliament.uk/api/Members/{mp}'.format(mp=mp)
            headers = {
            'accept': 'text/plain',
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                data['value']['latestParty']['mpId'] = mp
                df = pd.DataFrame(data['value']['latestParty'], index=[0])[['id', 'name', 'abbreviation', 'mpId']]
                list_of_dfs.append(df)
            else:
                pass
        df = pd.concat(list_of_dfs)
        return df


    def update_mp_job_info(self, path_to_tmp_folder):
        """
        This function returns a DataFrame containing all MP job information."""

        # Instantiate UK Parliament class above. 

        # mps_list = self.mps.parliament_api_number_10.tolist()
        print('Got preliminary info')
        for mp in tqdm(self.mps_ids):
            try:
                df = self.get_job_history(mp)
                df.to_csv(path_to_tmp_folder+'mp_{}.csv'.format(mp))
            except:
                print('excepted')
                pass
            
        df = pd.concat([pd.read_csv(file, index_col=0) for file in glob.glob(path_to_tmp_folder+'mp_*.csv')], ignore_index=True)
        df.drop_duplicates(inplace=True)
        df['id'] = df.index + 1
        df = df[['id', 'entity_id', 'job_title_11', 'start_date_12', 'end_date_13', 'employer_govt_dept_committee_etc_14']]
        return df