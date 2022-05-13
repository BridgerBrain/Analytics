from google.oauth2.service_account import Credentials
from google.ads.googleads.client import GoogleAdsClient
import pandas as pd
from google.cloud import bigquery
from datetime import date, datetime, timedelta
from log import logger
from schedule import every, repeat, run_pending
from utils import *



def get_gcld_info(date, subject_email):
    SCOPES = ['https://www.googleapis.com/auth/adwords']

    PATH_TO_DELETEGATED_SERVICE_ACCOUNT_JSON = get_environmental_variable('GOOGLE_APPLICATION_CREDENTIALS')

    CUSTOMER_ID = get_environmental_variable('GOOGLE_ADS_CUSTOMER_ID')
    GOOGLE_DEVELOPER_TOKEN = get_environmental_variable('GOOGLE_DEVELOPER_TOKEN')

    # gclid_date = '2021-12-01'
    gclid_date = date

    QUERY = """SELECT
    click_view.gclid,
    click_view.ad_group_ad,
    click_view.keyword,
    click_view.keyword_info.text,
    click_view.location_of_presence.country,
    ad_group.campaign,
    ad_group.name
    FROM click_view
    WHERE segments.date = '{}'
    """.format(gclid_date)

    credentials = Credentials.from_service_account_file(PATH_TO_DELETEGATED_SERVICE_ACCOUNT_JSON, scopes=SCOPES,
                                                        subject=subject_email)

    client = GoogleAdsClient(credentials=credentials, developer_token=GOOGLE_DEVELOPER_TOKEN, version="v9")

    ga_service = client.get_service("GoogleAdsService")

    gclid_columns = ['GCLID', 'Ad', 'Keyword_ID', 'Keyword_Name', 'Campaign',
                     'Adgroup_Name', 'Country_Presence']

    df_gclid_info = pd.DataFrame(data=None, columns=gclid_columns)

    stream = ga_service.search_stream(customer_id=CUSTOMER_ID, query=QUERY)
    logger.info('Extracted Information from Google Adwords API!')
    for batch in stream:
        logger.info('Casting Results into Dataframe...')

        for row in batch.results:
            df_gclid_info_row = pd.DataFrame()
            df_gclid_info_row.loc[0, 'GCLID'] = row.click_view.gclid
            df_gclid_info_row.loc[0, 'Ad'] = row.click_view.ad_group_ad
            df_gclid_info_row.loc[0, 'Keyword_ID'] = row.click_view.keyword
            df_gclid_info_row.loc[0, 'Keyword_Name'] = row.click_view.keyword_info.text
            df_gclid_info_row.loc[0, 'Campaign'] = row.ad_group.campaign
            df_gclid_info_row.loc[0, 'Adgroup_Name'] = row.ad_group.name
            df_gclid_info_row.loc[0, 'Ad_click_date'] = gclid_date
            df_gclid_info_row.loc[0, 'Country_Presence'] = row.click_view.location_of_presence.country

            df_gclid_info = df_gclid_info.append(df_gclid_info_row)

    df_gclid_info.reset_index(drop=True, inplace=True)

    return df_gclid_info


def main(skip_days):
    subject_email = get_environmental_variable('SUBJECT_EMAIL')
    date_today = date.today()
    date_start = date_today - timedelta(days=skip_days)

    google_dataset_id = "google_ads"
    zoho_dataset_id = "zoho_dataset"
    project_id = get_environmental_variable("GOOGLE_PROJECT_ID")
    zoho_table_name = 'crm_Leads'
    google_table_name = 'google_adwords_info'
    selected_columns = 'GCLID'

    table_id = '{}.{}.{}'.format(project_id, zoho_dataset_id, zoho_table_name)
    leads_QUERY = (
        'SELECT {} FROM `{}` WHERE GCLID IS NOT NULL'.format(selected_columns, table_id)
    )

    df_Leads = get_bigquery_table(leads_QUERY)
    df_Leads.drop_duplicates('GCLID', inplace=True)

    df_Leads.dropna(inplace=True)

    df_gclid_info_master = pd.DataFrame()

    date_string = str(date_start)
    # date_string = '2022-03-03'

    df_gclid_info = get_gcld_info(date_string, subject_email)

    if len(df_gclid_info) > 0:
        df_gclid_info_merged = df_gclid_info.merge(df_Leads)
        df_gclid_info_master = pd.concat([df_gclid_info_master,df_gclid_info_merged], ignore_index=True)
        logger.info('Extracted Adword Information for date {}'.format(date_string))

        """ Upload GCLID Campaign Information to the specified BigQuery Table """
        append = True
        table_id = '{}.{}.{}'.format(project_id, google_dataset_id, google_table_name)
        logger.info('Uploading Data to {}...'.format(table_id))
        df_gclid_info_master['Ad_click_date'] = pd.to_datetime(df_gclid_info_master['Ad_click_date'])
        result = upload_dataframe(df_gclid_info_master, append, project_id, google_dataset_id, google_table_name)
        logger.info('BigQuery Job status is {}'.format(result.state))
        logger.info('BigQuery errors: '.format(result.errors))
    else:
        logger.info("There is no Data for Date <{}>".format(date_string))

    return df_gclid_info, df_gclid_info_master


@repeat(every().day.at('05:00'))
def run_adwords_info():
    skip_days = 1
    """ Run Main """
    df_gclid_info_master = main(skip_days)
