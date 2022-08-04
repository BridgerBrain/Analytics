from google.cloud import bigquery
import google.auth


def get_bigquery_table(QUERY, project_id):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)

    # Retrieve Leads from BigQuery
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    df_BQ_table = rows.to_dataframe()

    return df_BQ_table


def create_table(table_id, job_config, dataframe, client):
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    return job.result()  # Wait for the job to complete.


def create_job_config(schema_config, append):
    if append:
        job_config = bigquery.LoadJobConfig(schema=schema_config, append="WRITE_APPEND")
    else:
        job_config = bigquery.LoadJobConfig(schema=schema_config, write_disposition="WRITE_TRUNCATE")
    return job_config


def upload_dataframe(df, schema_config, append, project_id, dataset, table_name):

    client = bigquery.Client(project=project_id)
    job_config = create_job_config(schema_config, append)
    table_id = '{}.{}.{}'.format(project_id, dataset, table_name)
    result = create_table(table_id, job_config, df, client)
    return result


def get_defined_schema(table_name):

    if table_name == 'Summarised_Table':

        table_schema = [bigquery.SchemaField('Date',                        'DATE',     mode='NULLABLE'),
                        bigquery.SchemaField('Closed_Won_M0',               'INTEGER',  mode='NULLABLE'),
                        bigquery.SchemaField('Daily_Signups',               'INTEGER',  mode='NULLABLE'),
                        bigquery.SchemaField('Cumulative_Closed_Won_M0',    'INTEGER',  mode='NULLABLE'),
                        bigquery.SchemaField('Cumulative_Signups',          'INTEGER',  mode='NULLABLE'),
                        bigquery.SchemaField('Daily_CR_Closed_Won_M0',      'FLOAT',    mode='NULLABLE'),
                        bigquery.SchemaField('Cumulative_CR_Closed_Won_M0', 'FLOAT',    mode='NULLABLE'),
                        bigquery.SchemaField('ARIMA_predictions',           'FLOAT',    mode='NULLABLE')]

    if table_name == 'Performance_Marketing_Report':

        table_schema = [bigquery.SchemaField('Lead_ID',                 'STRING',       mode='NULLABLE'),
                        bigquery.SchemaField('Created_Time',            'STRING',       mode='NULLABLE'),
                        bigquery.SchemaField('Lead_Status',             'STRING',       mode='NULLABLE'),
                        bigquery.SchemaField('Demo_Date',               'STRING',       mode='NULLABLE'),
                        bigquery.SchemaField('Is_Test',                 'BOOLEAN',      mode='NULLABLE'),
                        bigquery.SchemaField('Demo_Status',             'STRING',       mode='NULLABLE'),
                        bigquery.SchemaField('Converted_Account_ID',    'STRING',       mode='NULLABLE'),
                        bigquery.SchemaField('Converted_Deal_ID',       'STRING',       mode='NULLABLE'),

                        bigquery.SchemaField('Accounts_Adjusted_First_Trx',                 'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Account_ID',                'STRING',  mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Created_Time',              'STRING',  mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Lead_Status',               'STRING',  mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Subscription_Plan',         'STRING',  mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Account_Name',              'STRING',  mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Demo_Date',                 'STRING',  mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Lead_Created_Time',         'STRING',  mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Activation_Key_Activated',  'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_PSP_MID_Connected',         'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Is_Test',                   'BOOLEAN', mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Demo_Status',               'STRING',  mode='NULLABLE'),

                        bigquery.SchemaField('Deals_Adjusted_Deal_ID',              'STRING',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Deal_Name',            'STRING',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Account_ID',           'STRING',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Stage',                'STRING',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Subscription_Plan',    'STRING',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Closing_Date',         'STRING',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Lead_Created_Time',    'STRING',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Is_Test',              'BOOLEAN',  mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Expected_MRR',         'FLOAT',    mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Amount',               'FLOAT',    mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Currency',             'STRING',   mode='NULLABLE'),

                        bigquery.SchemaField('Campaign_ID1',    'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('Campaign_ID',     'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('Campaign_UTM1',   'STRING', mode='NULLABLE'),

                        bigquery.SchemaField('Campaign_Name',   'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('Source_UTM1',     'STRING', mode='NULLABLE'),
                        bigquery.SchemaField('Campaign_Source', 'STRING', mode='NULLABLE'),

                        bigquery.SchemaField('Leads_Refresh_Time',              'DATETIME',   mode='NULLABLE'),
                        bigquery.SchemaField('Accounts_Adjusted_Refresh_Time',  'DATETIME',   mode='NULLABLE'),
                        bigquery.SchemaField('Deals_Adjusted_Refresh_Time',     'DATETIME',   mode='NULLABLE')]

    return table_schema


def upload_table_to_bigquery(data, table_name, bool_upload):

    # Retrieve google credentials
    credentials = google.auth.load_credentials_from_file(
        "C:\\Users\\user\\AppData\\Roaming\\gcloud\\application_default_credentials.json")

    google_dataset_id = "Dev_Evri"
    project_id = "gtm-wtb9mc4-m2m0z"

    google_predictions_table = table_name

    defined_schema = get_defined_schema(table_name)

    if bool_upload:

        # Upload predictions_df to Google Big Query as a new table
        upload_dataframe(data,
                         defined_schema,
                         False,
                         project_id,
                         google_dataset_id,
                         google_predictions_table)

    print("Table '" + table_name + " 'Uploaded on Big Query...")

    # ========================================================================
