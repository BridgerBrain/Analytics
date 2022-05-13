from google.cloud import bigquery
import pandas as pd
from log import logger

""" < Description > 
 This program will upload the Specified Data Frame into Google BigQuery by creating a new table.
 Will replace any existing tables.

 < Input >
 1. Dataframe:  <Pandas Dataframe> : Dataframe containing Zoho Modules
 2. Dataset:  <String>: Name of the Dataset we are storing the Dataframe in BigQuery
 3. Module: <String>: Name of the module obtained from Zoho, will be used as name of the Generated table in BigQuery

 < Output >
 1. a table with id:project_name.dataset.<module_name> in Google BigQuery
 2. job_result: the job result of

"""

""" Replaces field names from DataFrame from <$field_name> to <Calc_field_name> as columns beginning with $ """
"""BigQuery column names cannot begin with $"""


def adjust_columns(df_Record):
    df_Record_columns = list(df_Record.columns)
    list_new_columns = list()

    df_column_map = pd.DataFrame()
    df_column_map['Original'] = df_Record_columns

    for column in df_Record_columns:
        if column[0] == "$":
            # print(column)
            list_new_columns.append('Calc_' + column[1:])
        else:
            list_new_columns.append(column)

    df_Record.columns = list_new_columns
    return df_Record


""""  Create a table in BigQuery from the specified Dataframe"""


#  Returns the result of the BigQuery task.
def create_table(table_id, job_config, dataframe, client):
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    return job.result()  # Wait for the job to complete.


""" Create the BigQuery job configuration"""


def create_job_config():
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    return job_config


""" Upload Data to Bigquery"""


def upload_table_to_BigQuery(table_name, dataframe, dataset, project_id, client):
    table_id = '{}.{}.{}'.format(project_id, dataset, table_name)
    df_record = dataframe
    df_record = adjust_columns(df_record)

    job_config = create_job_config()

    job_result = create_table(table_id, job_config, df_record, client)

    logger.info('Initiating creation of {}...'.format(table_id))

    # Create new table using the BigQuery Client

    try:
        create_table(table_id, job_config, df_record, client)

    except:
        logger.critical('Unable to create table')

    return df_record, job_result


""" Main Function to Call from Outside"""


def main(table_name, dataframe, project_id, dataset):
    client = bigquery.Client()

    [df_record, job_result] = upload_table_to_BigQuery(table_name, dataframe, dataset, project_id, client)

    return job_result
