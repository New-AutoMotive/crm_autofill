# crm_autofill

This repo contains the development of a script to automatically update contact info with various information pertaining to contacts held in our CiviCRM database. 

Overview of files:
* postcode_constituency_v2.py is a python script that looks through a CiviCRM database and matches contacts to parliamentary constituencies, building relationships between contacts and those constituencies. 
* helpers.py contains general helfpful functions
* create_pcd_constituency_id_lookup.py is a python script to create a csv that matches postcodes to parliamentary constituency entity_id fields in the civicrm SQL databases that sit behind our database. 
* pp_data holds scripts for extracting information from Parallel Parliament
* mp_info contains scripts for extracting fields about MPs (mostly twitter handles)
* disk_status contains scripts for automatically reporting on the state of the server's storage capacity
* bq_sync contains scripts for synchronising some MySQL tables with BigQuery. At present this just keeps the BQ tables in step with the CiviCRM database. 
* archive contains old crap
* address_files contains csvs (not in this repo) which are used to match postcodes to other details. 