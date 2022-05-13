"""Import Python Libraries"""
import json
import os
import pandas as pd
import requests
from datetime import datetime
from utils import *

"""Import custom Functions"""
import jobs.Upload_Zoho_to_Big_Query as Upload_Zoho_to_Big_Query

#  <<< Description >>>
#  This program calls the Zoho CRM APIs to retrieve the specified Records given by the module.
#
#  < Output> : Exported records in CSV format under folder Zoho_CRM / Data
#  Examples: Leads.csv, Deals.csv


# Enter the credentials for authentication the Requests
books_authorization_url = '  https://accounts.zoho.com/oauth/v2/token'
books_redirect_url = 'http://www.zoho.com/books'
books_grant_type = 'refresh_token'


# Generate Bearer Token for Calling Zoho APIs
def get_access_token(refresh_token, client_id, client_secret, redirect_url, authorization_url, grant_type):
    parameters = {
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_url,
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


# Get the Books for the specified module
def get_books(bearer_token, module, page, organization_id):
    url = 'https://books.zoho.com/api/v3/{c_module}?organization_id={o_id}'.format(c_module=module,
                                                                                   o_id=organization_id)

    headers = {
        'Authorization': 'Zoho-oauthtoken {}'.format(bearer_token),
        # 'If-Modified-Since': '2020-03-19T17:59:50+05:30'
    }

    parameters = {
        'page': page
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


# Get the Records counts for the specified Tag and Module
def get_record_count_tag(tag_id, module):
    url = 'https://www.zohoapis.com/crm/v2.1/settings/tags/{tag_id}/actions/records_count?module={module_api_name}'.format(
        tag_id=tag_id, module_api_name=module)

    headers = {
        'Authorization': 'Zoho-oauthtoken {}'.format(bearer_token),
    }

    response = requests.get(url=url, headers=headers)

    if response is not None:
        logger.info("HTTP Status Code : " + str(response.status_code))

        return response.json()['count']


#  Generate Dataframe from the specified Records Response
def create_dataframe_from_books(books, module):
    # Inputs
    # <books>: Dictionary : Zoho Books
    # <module>: Text: Module to select, example: 'invoices'

    #  Outputs
    #  <df_books>: DataFrame: The books in a Pandas DataFrame

    books_list = books[module]
    df_books = pd.DataFrame([books_list[0].values()], index=[0], columns=list(books_list[0].keys()))

    for item in range(0, len(books_list)):
        book = books_list[item]
        df_row = pd.DataFrame([book.values()], index=[0], columns=list(book.keys()))
        df_books = pd.concat([df_books, df_row])

    df_books.reset_index(drop=True, inplace=True)
    return df_books


#  Generate Records Dataframe from successful Response for the given module and page
#  Dependencies:
# get_records(bearer_token, module=choose_module, page=page)
# create_dataframe_from_records(records)

def create_df_from_successful_response(bearer_token, module, page, organization_id):
    #  Create DataFrame from Zoho Books

    [books, success] = get_books(bearer_token, module, page, organization_id)
    books_module_data = books[module]

    if success and len(books_module_data) > 0:

        df_books = create_dataframe_from_books(books, module)
        return df_books, success

    else:
        success = False
        logger.info(
            'There are no Books in <Page {c_page}> for <module: {c_module}>!'.format(c_page=page, c_module=module))
        return 1, success


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


# Create the Master DataFrame containing all the available records for the specified module
#  Dependencies:
# 1. create_df_from_successful_response(...)
# 2. create_dataframe_from_records(...)
# 3. get_records(...)
def create_master_df_books(bearer_token, module, unique_key, organization_id):
    logger.info('Retrieving Record Information from Zoho Books module <{}>...'.format(module))
    [df_Books_p1, success] = create_df_from_successful_response(bearer_token, module, 1, organization_id)
    df_Books_p1['API_call'] = 1
    df_Master_Books = df_Books_p1.copy()
    API_call = 1
    now = datetime.now()

    while success:

        API_call = API_call + 1
        [df_Books_temp, success] = create_df_from_successful_response(bearer_token, module, API_call, organization_id)
        logger.info('Current API call number is {} for module {} is successful: {}'.format(API_call, module, success))

        if success:
            df_Books_temp['API_call'] = API_call
            df_Master_Books = pd.concat([df_Master_Books, df_Books_temp])
        else:
            break
        df_Master_Books.drop_duplicates(subset=[unique_key], keep='first', inplace=True, ignore_index=True)
        df_Master_Books.reset_index(drop=True, inplace=True)

    df_Master_Books['Refresh_Time'] = now
    logger.info('Retrieval for module <{}> is Done!'.format(module))
    return df_Master_Books


""" Remove empty Dictionary columns """


#  Pyarrow cannot automatically upload empty dictionary columns as data type: Record since
#  since they do not have any children fields
def remove_columns_with_no_child_field(dataframe):
    test_df = dataframe.iloc[0]
    s = list(test_df)

    temp = [type(i) == dict for i in s]
    indexes_with_dict = [i for i, j in enumerate(temp) if j is True]

    to_remove_columns = list()

    for index in indexes_with_dict:
        dict_column = dataframe.iloc[:, index]
        remove = True

        for i in range(0, len(dataframe)):

            size_value = len(dict_column[i])

            if size_value >= 2:
                remove = False
                break

        if remove:
            to_remove_columns.append(index)

    dataframe.drop(dataframe.columns[to_remove_columns], axis=1, inplace=True)

    return dataframe


def main():
    """ Import Credentials from Environment"""
    books_refresh_token = get_environmental_variable('ZOHO_BOOKS_REFRESH_TOKEN')
    books_client_id = get_environmental_variable('ZOHO_BOOKS_CLIENT_ID')
    books_client_secret = get_environmental_variable('ZOHO_BOOKS_CLIENT_SECRET')
    organization_id = get_environmental_variable('ZOHO_BOOKS_ORGANIZATION_ID')
    GOOGLE_APPLICATION_CREDENTIALS = get_environmental_variable('GOOGLE_APPLICATION_CREDENTIALS')

    """Define the modules for the needed records"""
    list_modules_books = {'invoices': 'invoice_id', 'customerpayments': 'payment_id', 'creditnotes': 'creditnote_id',
                          'contacts': 'contact_id'}

    # list_modules_books = {'customerpayments': 'payment_id', 'creditnotes': 'creditnote_id', 'contacts': 'contact_id'}

    # list_modules_books = {'contacts': 'contact_id'}

    """Generate the bearer token"""
    bearer_token = get_access_token(books_refresh_token, books_client_id, books_client_secret, books_redirect_url,
                                    books_authorization_url, books_grant_type)

    """Create a list with All_Dataframes"""
    List_all_Dataframes = []

    for item_dict in list_modules_books.items():
        module = item_dict[0]
        unique_key = item_dict[1]

        dataframe = create_master_df_books(bearer_token, module, unique_key, organization_id)
        dataframe = remove_columns_with_no_child_field(dataframe)
        List_all_Dataframes.append(dataframe)

    dataset = 'zoho_dataset'
    project_id = get_environmental_variable('GOOGLE_PROJECT_ID')

    counter = 0

    for item_dict in list_modules_books.items():
        module = item_dict[0]
        table_name = 'books_{}'.format(module)
        dataframe = List_all_Dataframes[counter]
        counter += 1
        job_result = Upload_Zoho_to_Big_Query.main(table_name, dataframe, project_id, dataset)
        logger.info('BigQuery Job status is {}'.format(job_result.state))
        logger.info('BigQuery errors: '.format(job_result.errors))
