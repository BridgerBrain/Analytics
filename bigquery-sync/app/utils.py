import os
from log import logger
from google.cloud import bigquery


def get_environmental_variable(variable):
    env_variable = os.getenv(variable, None)

    if env_variable is None:
        logger.critical("ERROR: MISSING {}".format(variable))
        exit(1)

    return env_variable

def get_bigquery_table(QUERY):
    # Construct a BigQuery client object.
    client = bigquery.Client()

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

def create_job_config(append):
    if append:

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    else:

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    return job_config

def upload_dataframe(df, append, project_id, dataset, table_name):
    client = bigquery.Client()

    job_config = create_job_config(append)
    table_id = '{}.{}.{}'.format(project_id, dataset, table_name)

    result = create_table(table_id, job_config, df, client)

    return result

def insert_rows_bq(client, table_id, dataset_id, project_id, data):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    table = client.get_table(table_ref)

    resp = client.insert_rows_json(
        json_rows=data,
        table=table_ref,
    )

    logger.info("Success uploaded to table {}".format(table.table_id))
