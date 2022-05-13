"""Import Python Libaries"""
import json
import os
import pandas as pd
import requests
from datetime import datetime
# from bigquery-sync.app.utils import *
"""Import custom Functions"""
import jobs.Upload_Zoho_to_Big_Query as Upload_Zoho_to_Big_Query


#  <<< Description >>>
#  This program calls the Zoho CRM APIs to retrieve the specified Records given by the module.
#
#  < Output> : Exported records in CSV format under folder Zoho_CRM / Data
#  Examples: Leads.csv, Deals.csv


# Generate Bearer Token for Calling Zoho APIs
def get_access_token(refresh_token, client_id, client_secret, authorization_url, grant_type):
    parameters = {
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': grant_type

    }
    response = requests.post(url=authorization_url, data=parameters)

    dict_response = json.loads(response.text)
    bearer_token = dict_response['access_token']

    return bearer_token


# Get the Records counts for the specified module
def get_records_count(bearer_token, module):
    url = 'https://www.zohoapis.com/crm/v2.1/{}/actions/count'.format(module)

    headers = {
        'Authorization': 'Zoho-oauthtoken {}'.format(bearer_token),
    }

    response = requests.get(url=url, headers=headers)

    if response is not None:
        logger.info("HTTP Status Code : " + str(response.status_code))

        return response.json()['count']


# Get the Records for the specified module
def get_records(bearer_token, module, page):
    url = 'https://www.zohoapis.com/crm/v2/{}'.format(module)

    headers = {
        'Authorization': 'Zoho-oauthtoken {}'.format(bearer_token),
    }

    parameters = {
        'approved': 'both',
        'converted': 'both',
        'page': page,
        'include_child': 'false',
    }
    success = False
    response = requests.get(url=url, headers=headers, params=parameters)

    if response is not None:
        logger.info("HTTP Status Code : " + str(response.status_code))

    if response.status_code == 200:

        success = True
        return [response.json(), success]
    else:
        return [1, success]


#  Generate Dataframe from the specified Records Response
def create_dataframe_from_records(records):
    records_list = records['data']
    df_columns = list(records_list[0].keys())

    df_records = pd.DataFrame(columns=df_columns)

    for record in records_list:
        # record = list(records_list[0].values())

        df_row = pd.DataFrame([record.values()], index=[0], columns=list(record.keys()))
        df_records = pd.concat([df_records, df_row])

    df_records.reset_index(drop=True, inplace=True)
    return df_records


#  Generate Records Dataframe from successful Response for the given module and page
#  Dependencies:
# get_records(bearer_token, module=choose_module, page=page)
# create_dataframe_from_records(records)

def create_df_from_successful_response(bearer_token, choose_module, page):
    #  Create DataFrame from Zoho CRM Records

    [records, success] = get_records(bearer_token, module=choose_module, page=page)

    if success:

        df_records = create_dataframe_from_records(records)

        return df_records, success

    else:
        logger.info('There are no records in <Page {c_page}> for <module: {c_module}>!'.format(c_page=page,
                                                                                               c_module=choose_module))
        success = False
    return 1, success


# Create a Master DataFrame from data selected from page_start to page_end - NOT IN USE
#  Dependencies:
# 1. create_df_from_successful_response(...)
# 2. create_dataframe_from_records()
# 3. get_records()

# def create_df_master_pages(bearer_token, module, page_start, page_end):
#     Master_Leads = create_df_from_successful_response(bearer_token, module, page_start)
#
#     for i in range(page_start + 1, page_end + 1):
#         print(i)
#         Master_Leads = Master_Leads.append(create_df_from_successful_response(bearer_token, module, i))
#
#     Master_Leads.reset_index(inplace=True, drop=True)
#     Master_Leads.drop('index', inplace=True, axis=1)
#
#     return Master_Leads

#  Get information on the organization - NOT IN USE
def get_organization_id(bearer_token):
    url = 'https://www.zohoapis.com/crm/v2/org'

    headers = {
        'Authorization': 'Zoho-oauthtoken {}'.format(bearer_token),
        # 'If-Modified-Since': '2020-03-19T17:59:50+05:30'
    }

    success = False
    response = requests.get(url=url, headers=headers)

    if response is not None:
        logger.info("HTTP Status Code : " + str(response.status_code))

    if response.status_code == 200:

        success = True
        return [response.json(), success]
    else:
        return [1, success]


# Retrieve the number of records for the list of specified modules and return them as a DataFrame
# < Output> :
# 1. df_modules_count

def create_dataframe_module_counts(list_modules, bearer_token):
    df_modules_count = pd.DataFrame(data=None
                                    , columns=['Module', 'Record Count'])

    for module in list_modules:
        module_count = get_records_count(bearer_token, module)
        df_modules_count_row = pd.DataFrame([[module, module_count]], columns=['Module', 'Record Count'])

        df_modules_count = pd.concat([df_modules_count, df_modules_count_row])

    df_modules_count.reset_index(drop=True, inplace=True)

    return df_modules_count


# Create the Master DataFrame containing all the available records for the specified module
#  Dependencies:
# 1. create_df_from_successful_response(...)
# 2. create_dataframe_from_records(...)
# 3. get_records(...)
def create_master_df_records(bearer_token, module):
    logger.info('Retrieving Record Information from Zoho CRM module <{}>...'.format(module))

    now = datetime.now()
    [df_Records_p1, success] = create_df_from_successful_response(bearer_token, module, 1)
    df_Records_p1['API_call'] = 1
    df_Master_Records = df_Records_p1.copy()
    API_call = 1
    current_rec_count = len(df_Records_p1)

    while success:
        API_call = API_call + 1
        logger.info('Current Page to retrieve Data is {} '.format(API_call))
        [df_Records_temp, success] = create_df_from_successful_response(bearer_token, module, API_call)
        if success:

            df_Records_temp['API_call'] = API_call
            temp_count = len(df_Records_temp)
            logger.info('Page number {}  has  {} rows'.format(API_call, temp_count))
            df_Master_Records = pd.concat([df_Master_Records, df_Records_temp])
            current_rec_count = temp_count + current_rec_count
        else:
            break
    df_Master_Records['Refresh_Time'] = now

    logger.info('Retrieval for module <{}> is Done!'.format(module))

    return df_Master_Records, current_rec_count


def main():
    """ Import Credentials from Environment"""

    # Enter the credentials for authentication the Requests
    authorization_url = '  https://accounts.zoho.com/oauth/v2/token'
    grant_type = 'refresh_token'
    refresh_token = get_environmental_variable('ZOHO_CRM_REFRESH_TOKEN')
    client_id = get_environmental_variable('ZOHO_CRM_CLIENT_ID')
    client_secret = get_environmental_variable('ZOHO_CRM_CLIENT_SECRET')
    GOOGLE_APPLICATION_CREDENTIALS = get_environmental_variable('GOOGLE_APPLICATION_CREDENTIALS')

    """Define the modules for the needed records"""
    list_modules = ['Leads', 'Deals', 'PSPs', 'Accounts', 'contacts']

    """Generate the bearer token"""
    bearer_token = get_access_token(refresh_token, client_id, client_secret, authorization_url, grant_type)

    """Create a list with All_Dataframes"""
    List_all_Dataframes = []

    for module in list_modules:
        dataframe = create_master_df_records(bearer_token, module)[0]
        List_all_Dataframes.append(dataframe)

    dataset = 'zoho_dataset'
    project_id = get_environmental_variable('GOOGLE_PROJECT_ID')

    for i in range(0, len(list_modules)):
        module = list_modules[i]
        table_name = 'crm_{}'.format(module)
        dataframe = List_all_Dataframes[i]

        job_result = Upload_Zoho_to_Big_Query.main(table_name, dataframe, project_id, dataset)
        logger.info('BigQuery Job status is {}'.format(job_result.state))
        logger.info('BigQuery errors: '.format(job_result.errors))
