from google.oauth2.service_account import Credentials
from google.ads.googleads.client import GoogleAdsClient
from schedule import every, repeat, run_pending
import pandas as pd
from datetime import date, datetime, timedelta
from utils import *

""" Retrieve Google Adwords Metrics using the Adwords API """


def get_Adwords_Metrics(date, subject_email):
    SCOPES = ['https://www.googleapis.com/auth/adwords']

    PATH_TO_DELETEGATED_SERVICE_ACCOUNT_JSON = get_environmental_variable('GOOGLE_APPLICATION_CREDENTIALS')

    CUSTOMER_ID = get_environmental_variable('GOOGLE_ADS_CUSTOMER_ID')
    GOOGLE_DEVELOPER_TOKEN = get_environmental_variable('GOOGLE_DEVELOPER_TOKEN')

    # gclid_date = '2021-12-01'
    gclid_date = date

    QUERY = """SELECT 
    ad_group_ad.ad.id, 
    ad_group_ad.ad.name, 
    ad_group.name,
    segments.date, 
    metrics.clicks, 
    metrics.conversions, 
    metrics.impressions, 
    metrics.interactions,
    ad_group.campaign, 
    ad_group.id, 
    campaign.id, 
    campaign.name, 
    metrics.average_cost, 
    campaign.advertising_channel_type

    FROM ad_group_ad 

    WHERE segments.date = '{}'

    """.format(gclid_date)

    credentials = Credentials.from_service_account_file(PATH_TO_DELETEGATED_SERVICE_ACCOUNT_JSON, scopes=SCOPES,
                                                        subject=subject_email)

    client = GoogleAdsClient(credentials=credentials, developer_token=GOOGLE_DEVELOPER_TOKEN, version="v10")

    ga_service = client.get_service("GoogleAdsService")

    Metrics_columns = ['Ad_ID', 'Ad_Name', 'AdGroup_Name', 'Clicks',
                       'Conversions', 'Impressions', 'Interactions', 'AdGroup_ID', 'Campaign_ID',
                       'Campaign_Name', 'Average_Cost', 'Channel_Type', 'Date'
                       ]

    df_Metrics = pd.DataFrame(data=None, columns=Metrics_columns)

    stream = ga_service.search_stream(customer_id=CUSTOMER_ID, query=QUERY)
    logger.info('Extracted Information from Google Adwords API!')
    for batch in stream:
        logger.info('Casting Results into Dataframe...')

        for row in batch.results:
            df_Metrics_row = pd.DataFrame()
            df_Metrics_row.loc[0, 'Ad_ID'] = row.ad_group_ad.ad.id
            df_Metrics_row.loc[0, 'Ad_Name'] = row.ad_group_ad.ad.name
            df_Metrics_row.loc[0, 'AdGroup_Name'] = row.ad_group.name
            # df_Metrics_row.loc[0, 'Keyword'] = row.segments.keyword.info.text
            df_Metrics_row.loc[0, 'Clicks'] = row.metrics.clicks
            df_Metrics_row.loc[0, 'Conversions'] = row.metrics.conversions
            df_Metrics_row.loc[0, 'Impressions'] = row.metrics.impressions
            df_Metrics_row.loc[0, 'Interactions'] = row.metrics.interactions
            df_Metrics_row.loc[0, 'AdGroup_ID'] = row.ad_group.id
            df_Metrics_row.loc[0, 'Campaign_ID'] = row.campaign.id
            df_Metrics_row.loc[0, 'Campaign_Name'] = row.campaign.name
            df_Metrics_row.loc[0, 'Average_Cost'] = row.metrics.average_cost
            df_Metrics_row.loc[0, 'Channel_Type'] = row.campaign.advertising_channel_type

            df_Metrics_row.loc[0, 'Date'] = gclid_date

            df_Metrics = pd.concat([df_Metrics, df_Metrics_row], ignore_index=True)

    df_Metrics.reset_index(drop=True, inplace=True)

    return df_Metrics


def main(skip_days):
    subject_email = get_environmental_variable('SUBJECT_EMAIL')
    date_today = date.today()
    date_start = date_today - timedelta(days=skip_days)

    google_dataset_id = "google_ads"
    project_id = get_environmental_variable("GOOGLE_PROJECT_ID")
    google_table_name = 'google_Metrics'

    date_string = str(date_start)
    # date_string = '2022-03-03'

    df_Adwords_Metrics = get_Adwords_Metrics(date_string, subject_email)

    if len(df_Adwords_Metrics) > 0:

        logger.info('Extracted Google Metrics Information for date {}'.format(date_string))

        """ Upload  Campaign Information to the specified BigQuery Table """
        append = True
        table_id = '{}.{}.{}'.format(project_id, google_dataset_id, google_table_name)
        logger.info('Uploading Data to {}...'.format(table_id))
        df_Adwords_Metrics['Date'] = pd.to_datetime(df_Adwords_Metrics['Date'])
        result = upload_dataframe(df_Adwords_Metrics, append, project_id, google_dataset_id, google_table_name)
        logger.info('BigQuery Job status is {}'.format(result.state))
        logger.info('BigQuery errors: '.format(result.errors))
    else:
        logger.info("There is no Data for Date <{}>".format(date_string))

    return df_Adwords_Metrics


@repeat(every().day.at('05:00'))
def run_adwords_metrics():
    skip_days = 1
    """ Run Main """
    df_Adwords_Metrics = main(skip_days)

